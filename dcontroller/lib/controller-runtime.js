"use strict";

const { resolveResourceGVK, gvkToString } = require("./gvk");

const SOURCE_TYPE_WATCHER = "Watcher";
const SOURCE_TYPE_LISTER = "Lister";
const SOURCE_TYPE_PERIODIC = "Periodic";
const SOURCE_TYPE_ONESHOT = "OneShot";

const TARGET_TYPE_UPDATER = "Updater";
const TARGET_TYPE_PATCHER = "Patcher";

function sanitizeTopicSegment(value) {
  const out = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^[\-._]+/, "")
    .replace(/[\-._]+$/, "");
  return out.length > 0 ? out : "unnamed";
}

function inputTopic(namespace, name) {
  return `${sanitizeTopicSegment(namespace)}/${sanitizeTopicSegment(name)}/input`;
}

function outputTopic(namespace, name) {
  return `${sanitizeTopicSegment(namespace)}/${sanitizeTopicSegment(name)}/output`;
}

function sourceLogicalName(source) {
  if (source && source.kind) {
    return String(source.kind);
  }
  return "source";
}

function targetLogicalName(target) {
  if (target && target.kind) {
    return String(target.kind);
  }
  return "output";
}

function uniqueTargetLogicalName(base, sourceSet, targetSet, index) {
  let candidate = String(base || "").trim();
  if (!candidate) {
    candidate = `output_${index}`;
  }

  if (!sourceSet.has(candidate) && !targetSet.has(candidate)) {
    return candidate;
  }

  for (let i = 1; ; i += 1) {
    const next = `${candidate}_out_${i}`;
    if (sourceSet.has(next) || targetSet.has(next)) {
      continue;
    }
    return next;
  }
}

function parseDurationMillis(raw) {
  const s = String(raw || "").trim();
  if (!s) {
    return 0;
  }

  const match = s.match(/^([0-9]+)(ms|s|m|h)$/);
  if (!match) {
    throw new Error(`unsupported duration format ${JSON.stringify(s)}`);
  }

  const n = Number(match[1]);
  const unit = match[2];
  if (unit === "ms") {
    return n;
  }
  if (unit === "s") {
    return n * 1000;
  }
  if (unit === "m") {
    return n * 60 * 1000;
  }
  return n * 60 * 60 * 1000;
}

function virtualTriggerDocument(sourceType, triggerKind, namespace, triggerName) {
  const doc = {
    type: sourceType,
    kind: triggerKind,
    name: triggerName,
    triggeredAt: new Date().toISOString(),
  };
  if (namespace) {
    doc.namespace = namespace;
  }
  return doc;
}

function noopHandle(name) {
  return {
    close() {},
    name() {
      return name;
    },
  };
}

function timerHandle(name, closeFn) {
  let closed = false;
  return {
    close() {
      if (closed) {
        return;
      }
      closed = true;
      closeFn();
    },
    name() {
      return name;
    },
  };
}

function maybeHandleName(handle) {
  if (!handle || typeof handle.name !== "function") {
    return "";
  }
  try {
    return String(handle.name());
  } catch (_err) {
    return "";
  }
}

function closeHandle(handle, logger) {
  if (!handle || typeof handle.close !== "function") {
    return;
  }
  try {
    handle.close();
  } catch (err) {
    if (logger) {
      logger.warn("failed closing runtime handle", { error: String(err) });
    }
  }
}

function exactlyOneOf3(a, b, c) {
  let n = 0;
  if (a) {
    n += 1;
  }
  if (b) {
    n += 1;
  }
  if (c) {
    n += 1;
  }
  return n === 1;
}

function normalizeOptions(raw) {
  const opts = raw && typeof raw === "object" ? raw : {};
  return {
    disableIncrementalizer: !!opts.disableIncrementalizer,
    disableReconciler: !!opts.disableReconciler,
    disableRegularizer: !!opts.disableRegularizer,
  };
}

function applyTransforms(circuitHandle, options) {
  const opts = normalizeOptions(options);

  if (opts.disableIncrementalizer && !opts.disableReconciler) {
    throw new Error("options.disableIncrementalizer=true requires options.disableReconciler=true");
  }

  const useOptimizer = !opts.disableIncrementalizer && !opts.disableReconciler && !opts.disableRegularizer;
  if (useOptimizer) {
    circuitHandle.transform("Optimizer");
    return;
  }

  if (!opts.disableIncrementalizer && !opts.disableReconciler) {
    circuitHandle.transform("Reconciler");
  }
  if (!opts.disableRegularizer) {
    circuitHandle.transform("Regularizer");
  }
  if (!opts.disableIncrementalizer) {
    circuitHandle.transform("Incrementalizer");
  }
}

function buildSourceBindings(operatorName, controllerSpec, sourceConfigs) {
  const sourceSet = new Set();
  const bindings = [];

  for (const src of sourceConfigs) {
    const logical = sourceLogicalName(src.spec);
    sourceSet.add(logical);
    bindings.push({
      name: inputTopic(`${operatorName}.${controllerSpec.name}`, src.spec.kind),
      logical,
    });
  }

  return {
    sourceSet,
    bindings,
  };
}

function buildTargetBindings(operatorName, controllerSpec, targetConfigs, sourceSet) {
  const targetSet = new Set();
  const bindings = [];

  for (let i = 0; i < targetConfigs.length; i += 1) {
    const targetSpec = targetConfigs[i].spec;
    const logical = uniqueTargetLogicalName(targetLogicalName(targetSpec), sourceSet, targetSet, i);
    targetSet.add(logical);
    bindings.push({
      name: outputTopic(`${operatorName}.${controllerSpec.name}`, targetSpec.kind),
      logical,
    });
  }

  return bindings;
}

function parseSourceConfigs(operatorName, controllerSpec) {
  const specs = Array.isArray(controllerSpec && controllerSpec.sources) ? controllerSpec.sources : [];
  return specs.map((spec) => {
    const gvk = resolveResourceGVK(operatorName, spec);
    return {
      spec,
      gvk,
      gvkRef: gvkToString(gvk),
    };
  });
}

function parseTargetConfigs(operatorName, controllerSpec) {
  const specs = Array.isArray(controllerSpec && controllerSpec.targets) ? controllerSpec.targets : [];
  return specs.map((spec) => {
    const gvk = resolveResourceGVK(operatorName, spec);
    return {
      spec,
      gvk,
      gvkRef: gvkToString(gvk),
    };
  });
}

function startSourceHandle(operatorName, controllerSpec, sourceConfig, logger) {
  const source = sourceConfig.spec;
  const sourceType = source && source.type ? String(source.type) : SOURCE_TYPE_WATCHER;
  const topic = inputTopic(`${operatorName}.${controllerSpec.name}`, source.kind);
  const opts = { gvk: sourceConfig.gvkRef };

  if (source && source.namespace) {
    opts.namespace = source.namespace;
  }

  const labels = source && source.labelSelector && source.labelSelector.matchLabels;
  if (labels && typeof labels === "object" && Object.keys(labels).length > 0) {
    opts.labels = labels;
  }

  if (source && source.predicate) {
    logger.warn("source predicate is not supported in JS runtime and will be ignored", {
      controller: controllerSpec.name,
      sourceKind: source.kind,
    });
  }

  switch (sourceType) {
    case SOURCE_TYPE_WATCHER:
      return kubernetes.watch(topic, opts);
    case SOURCE_TYPE_LISTER:
      return kubernetes.list(topic, opts);
    case SOURCE_TYPE_PERIODIC: {
      const periodRaw = source && source.parameters && source.parameters.period;
      const periodMs = parseDurationMillis(periodRaw);
      if (!periodMs || periodMs <= 0) {
        throw new Error(`periodic source ${JSON.stringify(source.kind)} requires a positive parameters.period`);
      }

      const triggerKind = source && source.kind ? String(source.kind) : "source";
      const namespace = source && source.namespace ? String(source.namespace) : "";
      const triggerName = "periodic-trigger";
      const timer = setInterval(() => {
        const doc = virtualTriggerDocument(SOURCE_TYPE_PERIODIC, triggerKind, namespace, triggerName);
        publish(topic, [[doc, 1]]);
      }, periodMs);

      return timerHandle(`dcontroller-periodic-${controllerSpec.name}-${triggerKind}`, () => clearInterval(timer));
    }
    case SOURCE_TYPE_ONESHOT: {
      const triggerKind = source && source.kind ? String(source.kind) : "source";
      const namespace = source && source.namespace ? String(source.namespace) : "";
      const triggerName = "one-shot-trigger";
      const doc = virtualTriggerDocument(SOURCE_TYPE_ONESHOT, triggerKind, namespace, triggerName);
      publish(topic, [[doc, 1]]);

      return noopHandle(`dcontroller-oneshot-${controllerSpec.name}-${triggerKind}`);
    }
    default:
      throw new Error(`unknown source type ${JSON.stringify(sourceType)} for ${JSON.stringify(source.kind)}`);
  }
}

function startTargetHandle(operatorName, controllerSpec, targetConfig) {
  const target = targetConfig.spec;
  const targetType = target && target.type ? String(target.type) : TARGET_TYPE_UPDATER;
  const topic = outputTopic(`${operatorName}.${controllerSpec.name}`, target.kind);
  const opts = { gvk: targetConfig.gvkRef };

  switch (targetType) {
    case TARGET_TYPE_UPDATER:
      return kubernetes.update(topic, opts);
    case TARGET_TYPE_PATCHER:
      return kubernetes.patch(topic, opts);
    default:
      throw new Error(`unknown target type ${JSON.stringify(targetType)} for ${JSON.stringify(target.kind)}`);
  }
}

function startController(operatorName, controllerSpec, logger, onComponent) {
  if (!controllerSpec || typeof controllerSpec !== "object") {
    throw new Error("controller spec must be an object");
  }
  if (!controllerSpec.name) {
    throw new Error("controller.name is required");
  }

  const sources = Array.isArray(controllerSpec.sources) ? controllerSpec.sources : [];
  if (sources.length === 0) {
    throw new Error(`controller ${JSON.stringify(controllerSpec.name)} must define at least one source`);
  }
  const targets = Array.isArray(controllerSpec.targets) ? controllerSpec.targets : [];
  if (targets.length === 0) {
    throw new Error(`controller ${JSON.stringify(controllerSpec.name)} must define at least one target`);
  }

  if (!exactlyOneOf3(!!controllerSpec.pipeline, !!controllerSpec.sql, !!controllerSpec.circuit)) {
    throw new Error("exactly one of spec.pipeline, spec.sql, or spec.circuit must be set");
  }
  if (!controllerSpec.pipeline) {
    throw new Error(`controller ${JSON.stringify(controllerSpec.name)}: only pipeline is supported in JS runtime`);
  }

  const sourceConfigs = parseSourceConfigs(operatorName, controllerSpec);
  const targetConfigs = parseTargetConfigs(operatorName, controllerSpec);

  const sourceBindingInfo = buildSourceBindings(operatorName, controllerSpec, sourceConfigs);
  const targetBindings = buildTargetBindings(operatorName, controllerSpec, targetConfigs, sourceBindingInfo.sourceSet);

  const circuitName = [
    "dcontroller",
    sanitizeTopicSegment(operatorName),
    sanitizeTopicSegment(controllerSpec.name),
  ].join(".");

  logger.info("compiling controller", {
    event_type: "dbsp runtime event",
    topic: controllerSpec.name,
    sources: sourceBindingInfo.bindings,
    targets: targetBindings,
  });

  const circuitHandle = aggregate.compile(controllerSpec.pipeline, {
    inputs: sourceBindingInfo.bindings,
    outputs: targetBindings,
    name: circuitName,
  });

  applyTransforms(circuitHandle, controllerSpec.options || {});
  circuitHandle.validate();

  if (typeof onComponent === "function") {
    onComponent(circuitName);
  }

  const handles = [];
  try {
    for (const sourceConfig of sourceConfigs) {
      const sourceHandle = startSourceHandle(operatorName, controllerSpec, sourceConfig, logger);
      handles.push(sourceHandle);
      if (typeof onComponent === "function") {
        const name = maybeHandleName(sourceHandle);
        if (name) {
          onComponent(name);
        }
      }
    }

    for (const targetConfig of targetConfigs) {
      const targetHandle = startTargetHandle(operatorName, controllerSpec, targetConfig);
      handles.push(targetHandle);
      if (typeof onComponent === "function") {
        const name = maybeHandleName(targetHandle);
        if (name) {
          onComponent(name);
        }
      }
    }
  } catch (err) {
    for (let i = handles.length - 1; i >= 0; i -= 1) {
      closeHandle(handles[i], logger);
    }
    closeHandle(circuitHandle, logger);
    throw err;
  }

  return {
    name: String(controllerSpec.name),
    close() {
      for (let i = handles.length - 1; i >= 0; i -= 1) {
        closeHandle(handles[i], logger);
      }
      closeHandle(circuitHandle, logger);
    },
  };
}

module.exports = {
  startController,
};
