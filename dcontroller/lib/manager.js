"use strict";

const { createLogger, formatError } = require("log");
const { buildReadyStatus, buildNotReadyStatus } = require("./status");
const {
  startOperatorInstance,
  stopOperatorInstance,
  operatorName,
  generationOf,
} = require("./operator-runtime");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
const OPERATOR_EVENT_TOPIC = "dcontroller/operator/events";
const OPERATOR_STATUS_TOPIC = "dcontroller/operator/status";

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function parseWeight(entry) {
  if (!Array.isArray(entry) || entry.length < 2) {
    return 0;
  }
  const n = Number(entry[1]);
  if (!Number.isFinite(n)) {
    return 0;
  }
  return n;
}

function parseOperatorDoc(entry) {
  if (!Array.isArray(entry) || entry.length === 0) {
    return null;
  }
  const doc = entry[0];
  if (!doc || typeof doc !== "object") {
    return null;
  }
  return doc;
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const keys = Object.keys(value).sort();
    const parts = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`);
    return `{${parts.join(",")}}`;
  }
  return JSON.stringify(value);
}

function equivalentSpecUpdate(oldDoc, newDoc) {
  if (!oldDoc || !newDoc) {
    return false;
  }
  const oldGeneration = generationOf(oldDoc);
  const newGeneration = generationOf(newDoc);
  if (oldGeneration !== newGeneration) {
    return false;
  }
  return stableStringify(oldDoc.spec || {}) === stableStringify(newDoc.spec || {});
}

class DControllerManager {
  constructor(config) {
    const cfg = config && typeof config === "object" ? config : {};
    this.runtimeConfig = cfg.runtimeConfig || {};
    this.log = cfg.logger || createLogger("dcontroller.manager");

    this.operators = new Map();
    this.componentIndex = new Map();

    this.operatorWatchHandle = null;
    this.operatorStatusHandle = null;
  }

  start() {
    kubernetes.runtime.start(this.runtimeConfig);

    this.operatorStatusHandle = kubernetes.update(OPERATOR_STATUS_TOPIC, { gvk: OPERATOR_GVK });
    this.operatorWatchHandle = kubernetes.watch(OPERATOR_EVENT_TOPIC, { gvk: OPERATOR_GVK });

    subscribe(OPERATOR_EVENT_TOPIC, (entries) => {
      this.reconcileOperatorEvents(entries);
    });

    runtime.onError((err) => {
      this.onRuntimeError(err);
    });

    this.log.info({
      event_type: "dbsp runtime event",
      topic: OPERATOR_EVENT_TOPIC,
      operator_gvk: OPERATOR_GVK,
    }, "dcontroller manager started");
  }

  reconcileOperatorEvents(entries) {
    const list = Array.isArray(entries) ? entries : [];

    const deletes = new Map();
    const upserts = new Map();

    for (const entry of list) {
      const doc = parseOperatorDoc(entry);
      if (!doc) {
        continue;
      }
      const w = parseWeight(entry);
      const key = operatorName(doc);
      if (!key) {
        continue;
      }
      if (w < 0) {
        deletes.set(key, doc);
      }
      if (w > 0) {
        upserts.set(key, doc);
      }
    }

    for (const [key, oldDoc] of deletes.entries()) {
      const newDoc = upserts.get(key);
      if (!newDoc) {
        continue;
      }
      if (equivalentSpecUpdate(oldDoc, newDoc)) {
        deletes.delete(key);
        upserts.delete(key);
        this.log.debug({
          event_type: "dbsp runtime event",
          topic: key,
        }, "ignored status-only operator update");
      }
    }

    for (const key of deletes.keys()) {
      this.deleteOperator(key);
    }
    for (const doc of upserts.values()) {
      if (doc) {
        this.upsertOperator(doc);
      }
    }
  }

  upsertOperator(operatorDoc) {
    const name = operatorName(operatorDoc);
    if (!name) {
      this.log.error({
        event_type: "dbsp runtime event",
        topic: OPERATOR_EVENT_TOPIC,
      }, "skipping operator without metadata.name");
      return;
    }

    this.deleteOperator(name);

    const registerComponent = (componentName) => {
      if (!componentName) {
        return;
      }
      this.componentIndex.set(componentName, name);
    };

    let state = null;
    try {
      state = startOperatorInstance(
        operatorDoc,
        this.log.child({ operator: name }),
        registerComponent,
      );
      this.operators.set(name, state);
      this.publishStatus(name, buildReadyStatus(state.generation, state.errors));
    } catch (err) {
      this.log.error({
        event_type: "dbsp runtime event",
        topic: name,
        error: formatError(err),
      }, "operator reconciliation failed");

      for (const [origin, mapped] of this.componentIndex.entries()) {
        if (mapped === name) {
          this.componentIndex.delete(origin);
        }
      }

      this.publishFailedStatus(operatorDoc, err);
    }
  }

  deleteOperator(name) {
    const key = String(name || "").trim();
    if (!key) {
      return;
    }

    const state = this.operators.get(key);
    if (!state) {
      return;
    }

    this.operators.delete(key);
    for (const componentName of state.components.values()) {
      this.componentIndex.delete(componentName);
    }

    stopOperatorInstance(state, this.log.child({ operator: key }));
  }

  onRuntimeError(payload) {
    const origin = payload && payload.origin ? String(payload.origin) : "";
    const message = payload && payload.message ? String(payload.message) : "unknown runtime error";

    if (!origin) {
      this.log.error({
        event_type: "dbsp runtime event",
        message,
      }, "runtime error without origin");
      return;
    }

    const operatorKey = this.componentIndex.get(origin);
    if (!operatorKey) {
      this.log.error({
        event_type: "dbsp runtime event",
        origin,
        message,
      }, "runtime error from unmanaged component");
      return;
    }

    const state = this.operators.get(operatorKey);
    if (!state) {
      return;
    }

    const fullMessage = `[${origin}] ${message}`;
    state.errors.push(fullMessage);

    this.log.error({
      event_type: "dbsp runtime event",
      topic: operatorKey,
      origin,
      message,
    }, "operator runtime error");

    this.publishStatus(operatorKey, buildReadyStatus(state.generation, state.errors));
  }

  publishFailedStatus(operatorDoc, err) {
    const name = operatorName(operatorDoc);
    const generation = generationOf(operatorDoc);
    this.publishRawStatus(operatorDoc, buildNotReadyStatus(generation, err));

    this.log.error({
      event_type: "dbsp runtime event",
      topic: name,
      error: formatError(err),
    }, "published failed operator status");
  }

  publishStatus(operatorNameValue, status) {
    const state = this.operators.get(operatorNameValue);
    if (!state) {
      return;
    }
    this.publishRawStatus(state.doc, status);
  }

  publishRawStatus(operatorDoc, status) {
    const out = deepClone(operatorDoc);
    out.status = status;
    publish(OPERATOR_STATUS_TOPIC, [[out, 1]]);
  }
}

module.exports = {
  DControllerManager,
};
