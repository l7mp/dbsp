"use strict";

const { createLogger, formatError } = require("log");
const { buildReadyStatus, buildNotReadyStatus } = require("./status");
const { startOperatorInstance, stopOperatorInstance } = require("./operator-runtime");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
const OPERATOR_EVENT_TOPIC = "dcontroller/operator/events";
const OPERATOR_STATUS_TOPIC = "dcontroller/operator/status";

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

class DControllerManager {
  constructor(config) {
    this.runtimeConfig = config.runtimeConfig || {};
    this.log = config.logger || createLogger("dcontroller.manager");

    this.operators = new Map();

    this.operatorWatchHandle = null;
    this.operatorStatusHandle = null;
  }

  start() {
    kubernetes.runtime.start(this.runtimeConfig);

    this.operatorStatusHandle = kubernetes.update(OPERATOR_STATUS_TOPIC, { gvk: OPERATOR_GVK });
    // GenerationChanged drops status-only updates at the Watcher: K8s bumps
    // metadata.generation on .spec changes, not on .status churn.
    this.operatorWatchHandle = kubernetes.watch(OPERATOR_EVENT_TOPIC, {
      gvk: OPERATOR_GVK,
      predicate: "GenerationChanged",
    });

    subscribe(OPERATOR_EVENT_TOPIC, (entries) => {
      this.reconcileOperatorEvents(entries);
    });

    runtime.onError((err) => {
      this.onRuntimeError(err);
    });

    this.log.info({
      event_type: "dcontroller_manager_started",
      topic: OPERATOR_EVENT_TOPIC,
      operator_gvk: OPERATOR_GVK,
    }, "dcontroller manager started");
  }

  reconcileOperatorEvents(entries) {
    const deletes = new Map();
    const upserts = new Map();

    for (const [doc, weight] of entries) {
      const key = doc.metadata.name;
      if (weight < 0) {
        deletes.set(key, doc);
      } else if (weight > 0) {
        upserts.set(key, doc);
      }
    }

    for (const key of deletes.keys()) {
      this.deleteOperator(key);
    }
    for (const doc of upserts.values()) {
      this.upsertOperator(doc);
    }
  }

  upsertOperator(operatorDoc) {
    const name = operatorDoc.metadata.name;
    this.deleteOperator(name);

    let state;
    try {
      state = startOperatorInstance(operatorDoc, this.log.child({ operator: name }));
    } catch (err) {
      this.log.error({
        event_type: "operator_reconciliation_failed",
        topic: name,
        error: formatError(err),
      }, "operator reconciliation failed");
      this.publishFailedStatus(operatorDoc, err);
      return;
    }

    this.operators.set(name, state);
    this.publishStatus(name, buildReadyStatus(state.generation, state.errors));
  }

  deleteOperator(name) {
    const state = this.operators.get(name);
    if (!state) {
      return;
    }

    this.operators.delete(name);
    stopOperatorInstance(state, this.log.child({ operator: name }));
  }

  // findOperatorByComponent locates the operator that owns a given runtime
  // component name (the origin reported in runtime errors). Linear in the
  // number of operators; runtime errors are rare.
  findOperatorByComponent(origin) {
    for (const [name, state] of this.operators) {
      for (const ctrl of state.controllers) {
        if (ctrl.components.has(origin)) {
          return { name, state };
        }
      }
    }
    return null;
  }

  onRuntimeError(payload) {
    const { origin, message } = payload;
    const owner = this.findOperatorByComponent(origin);
    if (!owner) {
      this.log.error({
        event_type: "runtime_error_unmanaged_component",
        origin,
        message,
      }, "runtime error from unmanaged component");
      return;
    }

    owner.state.errors.push(`[${origin}] ${message}`);

    this.log.error({
      event_type: "operator_runtime_error",
      topic: owner.name,
      origin,
      message,
    }, "operator runtime error");

    this.publishStatus(owner.name, buildReadyStatus(owner.state.generation, owner.state.errors));
  }

  publishFailedStatus(operatorDoc, err) {
    this.publishRawStatus(operatorDoc, buildNotReadyStatus(operatorDoc.metadata.generation, err));

    this.log.error({
      event_type: "operator_failed_status_published",
      topic: operatorDoc.metadata.name,
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
