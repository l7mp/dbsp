"use strict";

const { resolveResourceGVK, gvkToString } = require("./gvk");

const SOURCE_TYPE_WATCHER = "Watcher";
const SOURCE_TYPE_LISTER = "Lister";
const SOURCE_TYPE_PERIODIC = "Periodic";
const SOURCE_TYPE_ONESHOT = "OneShot";

const TARGET_TYPE_UPDATER = "Updater";
const TARGET_TYPE_PATCHER = "Patcher";

function parseDurationMillis(raw) {
  const s = String(raw).trim();
  const match = s.match(/^([0-9]+)(ms|s|m|h)$/);
  if (!match) {
    throw new Error(`unsupported duration format ${JSON.stringify(s)}`);
  }
  const n = Number(match[1]);
  switch (match[2]) {
    case "ms": return n;
    case "s":  return n * 1000;
    case "m":  return n * 60 * 1000;
    default:   return n * 60 * 60 * 1000;
  }
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
  return { close() {}, name() { return name; } };
}

function timerHandle(name, closeFn) {
  let closed = false;
  return {
    close() {
      if (closed) return;
      closed = true;
      closeFn();
    },
    name() { return name; },
  };
}

function closeHandle(handle, logger) {
  try {
    handle.close();
  } catch (err) {
    logger.warn({
      event_type: "runtime_handle_close_failed",
      error: String(err),
    }, "failed closing runtime handle");
  }
}

function exactlyOneOf3(a, b, c) {
  return [a, b, c].filter(Boolean).length === 1;
}

function applyTransforms(circuitHandle, opts) {
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

function parseConfigs(operatorName, specs) {
  return specs.map((spec) => {
    const gvk = resolveResourceGVK(operatorName, spec);
    return { spec, gvk, gvkRef: gvkToString(gvk) };
  });
}

function startSourceHandle(controllerPrefix, controllerSpec, sourceConfig) {
  const source = sourceConfig.spec;
  const sourceType = source.type || SOURCE_TYPE_WATCHER;
  const topic = `${controllerPrefix}/${source.kind}/input`;
  const opts = { gvk: sourceConfig.gvkRef };

  if (source.namespace) {
    opts.namespace = source.namespace;
  }
  if (source.labelSelector?.matchLabels) {
    opts.labels = source.labelSelector.matchLabels;
  }
  if (source.predicate) {
    opts.predicate = source.predicate;
  }

  switch (sourceType) {
    case SOURCE_TYPE_WATCHER:
      return kubernetes.watch(topic, opts);
    case SOURCE_TYPE_LISTER:
      return kubernetes.list(topic, opts);
    case SOURCE_TYPE_PERIODIC: {
      if (source.parameters?.period == null) {
        throw new Error(`periodic source ${JSON.stringify(source.kind)} requires parameters.period`);
      }
      const periodMs = parseDurationMillis(source.parameters.period);
      const timer = setInterval(() => {
        const doc = virtualTriggerDocument(SOURCE_TYPE_PERIODIC, source.kind, source.namespace || "", "periodic-trigger");
        publish(topic, [[doc, 1]]);
      }, periodMs);
      return timerHandle(`dcontroller-periodic-${controllerSpec.name}-${source.kind}`, () => clearInterval(timer));
    }
    case SOURCE_TYPE_ONESHOT: {
      const doc = virtualTriggerDocument(SOURCE_TYPE_ONESHOT, source.kind, source.namespace || "", "one-shot-trigger");
      publish(topic, [[doc, 1]]);
      return noopHandle(`dcontroller-oneshot-${controllerSpec.name}-${source.kind}`);
    }
    default:
      throw new Error(`unknown source type ${JSON.stringify(sourceType)} for ${JSON.stringify(source.kind)}`);
  }
}

function startTargetHandle(controllerPrefix, targetConfig) {
  const target = targetConfig.spec;
  const targetType = target.type || TARGET_TYPE_UPDATER;
  const topic = `${controllerPrefix}/${target.kind}/output`;
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

function startController(operatorName, controllerSpec, logger) {
  if (!exactlyOneOf3(controllerSpec.pipeline, controllerSpec.sql, controllerSpec.circuit)) {
    throw new Error("exactly one of spec.pipeline, spec.sql, or spec.circuit must be set");
  }
  if (!controllerSpec.pipeline) {
    throw new Error(`controller ${JSON.stringify(controllerSpec.name)}: only pipeline is supported in JS runtime`);
  }

  const controllerPrefix = `${operatorName}.${controllerSpec.name}`;
  const sourceConfigs = parseConfigs(operatorName, controllerSpec.sources);
  const targetConfigs = parseConfigs(operatorName, controllerSpec.targets);

  const sourceBindings = sourceConfigs.map((src) => ({
    name: `${controllerPrefix}/${src.spec.kind}/input`,
    logical: src.spec.kind,
  }));
  const targetBindings = targetConfigs.map((tgt) => ({
    name: `${controllerPrefix}/${tgt.spec.kind}/output`,
    logical: tgt.spec.kind,
  }));

  const circuitName = `dcontroller.${controllerPrefix}`;

  logger.info({
    event_type: "controller_compiling",
    topic: controllerSpec.name,
    sources: sourceBindings,
    targets: targetBindings,
  }, "compiling controller");

  const circuitHandle = aggregate.compile(controllerSpec.pipeline, {
    inputs: sourceBindings,
    outputs: targetBindings,
    name: circuitName,
  });

  applyTransforms(circuitHandle, controllerSpec.options || {});
  circuitHandle.validate();

  const components = new Set([circuitName]);
  const handles = [];
  try {
    for (const sourceConfig of sourceConfigs) {
      const h = startSourceHandle(controllerPrefix, controllerSpec, sourceConfig);
      handles.push(h);
      components.add(h.name());
    }
    for (const targetConfig of targetConfigs) {
      const h = startTargetHandle(controllerPrefix, targetConfig);
      handles.push(h);
      components.add(h.name());
    }
  } catch (err) {
    for (let i = handles.length - 1; i >= 0; i -= 1) {
      closeHandle(handles[i], logger);
    }
    closeHandle(circuitHandle, logger);
    throw err;
  }

  return {
    name: controllerSpec.name,
    components,
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
