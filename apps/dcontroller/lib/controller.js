"use strict";

const { resolveResourceGVK, gvkToString } = require("./gvk");

const SOURCE_TYPE_WATCHER = "Watcher";

const TARGET_TYPE_UPDATER = "Updater";
const TARGET_TYPE_PATCHER = "Patcher";

// Group routing: the resource group of a source/target selects the
// connector that binds it, and the source/target type names the
// connector's verb. Absent and Kubernetes API groups (and the operator's
// own view group) bind through the Kubernetes connector; every other
// connector lives under connector.dcontroller.io.
const XDS_GROUP = "xds.connector.dcontroller.io";
const MISC_GROUP = "misc.connector.dcontroller.io";
// xds resource kinds to delta-ADS type URLs shorthands.
const XDS_TYPES = {
    Listener: "lds",
    RouteConfiguration: "rds",
    Cluster: "cds",
    ClusterLoadAssignment: "eds",
};

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

function applyTransforms(circuitHandle, controllerSpec) {
    // The transforms list states WHAT the controller is; the engine
    // applies the set in canonical order as one atomic step. An absent
    // list means the default chain.
    const transforms = controllerSpec.transforms || [
        { name: "Reconciler" },
        { name: "Distincter" },
        { name: "Incrementalizer" },
    ];
    if (transforms.length > 0) {
        circuitHandle.transform(transforms);
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

    if (sourceConfig.gvk.group === XDS_GROUP) {
        return startXdsSourceHandle(topic, source);
    }
    if (sourceConfig.gvk.group === MISC_GROUP) {
        return startMiscSourceHandle(topic, source, sourceType);
    }

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
        // level: full snapshots per event instead of deltas.
        return source.level ? kubernetes.list(topic, opts) : kubernetes.watch(topic, opts);
    default:
        throw new Error(`unknown source type ${JSON.stringify(sourceType)} for ${JSON.stringify(source.kind)}`);
    }
}

// Misc-group sources bind through the connector's own verb table: the
// source type names the verb (Tick -> misc.tick), and kind and
// parameters pass through as is; the connector rejects what it does not
// offer. Only the connector's default verb is known here.
function startMiscSourceHandle(topic, source, sourceType) {
    const type = sourceType === SOURCE_TYPE_WATCHER ? "Tick" : sourceType;
    const bind = misc[type.toLowerCase()];
    if (typeof bind !== "function") {
        throw new Error(`unknown misc source type ${JSON.stringify(type)} for ${JSON.stringify(source.kind)}`);
    }
    const opts = { kind: source.kind, ...(source.parameters || {}) };
    if (source.namespace) {
        opts.namespace = source.namespace;
    }
    return bind(topic, opts);
}

function startTargetHandle(operatorName, controllerPrefix, targetConfig) {
    const target = targetConfig.spec;
    const targetType = target.type || TARGET_TYPE_UPDATER;
    const topic = `${controllerPrefix}/${target.kind}/output`;

    if (targetConfig.gvk.group === XDS_GROUP) {
        return startXdsTargetHandle(operatorName, topic, target, targetType);
    }

    const opts = { gvk: targetConfig.gvkRef };
    switch (targetType) {
    case TARGET_TYPE_UPDATER:
        // level: state-of-the-world ownership of the kind.
        return target.level ? kubernetes.set(topic, opts) : kubernetes.update(topic, opts);
    case TARGET_TYPE_PATCHER:
        // level has no Patcher mode; it is ignored here.
        return kubernetes.patch(topic, opts);
    default:
        throw new Error(`unknown target type ${JSON.stringify(targetType)} for ${JSON.stringify(target.kind)}`);
    }
}

function xdsTypeOf(kind) {
    const type = XDS_TYPES[kind];
    if (!type) {
        throw new Error(`unknown xds resource kind ${JSON.stringify(kind)}; one of ${Object.keys(XDS_TYPES).join(", ")}`);
    }
    return type;
}

// The per-operator xds egress servers, started lazily by the first target
// naming an address (the server name is "<operator>[/<name>]", so
// operators never share a server and a restarted operator reuses its
// running one).
const xdsServers = new Set();

function ensureXdsServer(operatorName, parameters) {
    const name = parameters?.server ? `${operatorName}/${parameters.server}` : operatorName;
    if (!xdsServers.has(name)) {
        if (!parameters?.address) {
            throw new Error(`xds server ${JSON.stringify(name)} is not running and the target names no parameters.address to start it`);
        }
        xds.server.start({ name, address: parameters.address });
        xdsServers.add(name);
    }
    return name;
}

function startXdsTargetHandle(operatorName, topic, target, targetType) {
    if (targetType !== TARGET_TYPE_UPDATER) {
        throw new Error(`xds target ${JSON.stringify(target.kind)}: only Updater targets exist on the xds connector`);
    }
    const server = ensureXdsServer(operatorName, target.parameters);
    const opts = { type: xdsTypeOf(target.kind), server };
    // level: state-of-the-world egress.
    return target.level ? xds.set(topic, opts) : xds.update(topic, opts);
}

function startXdsSourceHandle(topic, source) {
    if (!source.parameters?.address) {
        throw new Error(`xds source ${JSON.stringify(source.kind)} requires parameters.address`);
    }
    const opts = { type: xdsTypeOf(source.kind), address: source.parameters.address };
    if (source.level) {
        opts.level = true;
    }
    return xds.watch(topic, opts);
}

function startController(operatorName, controllerSpec, logger) {
    if (!exactlyOneOf3(controllerSpec.pipeline, controllerSpec.sql, controllerSpec.circuit)) {
        throw new Error("exactly one of spec.pipeline, spec.sql, or spec.circuit must be set");
    }
    if (controllerSpec.options) {
        throw new Error("spec.options is gone; state the controller's transforms explicitly in spec.transforms");
    }
    if (controllerSpec.type) {
        throw new Error("spec.type is gone; a state-of-the-world controller lists spec.transforms without the Incrementalizer");
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

    // The controller owns its topics: start from clean retained integrals
    // so this instance never bootstraps from a predecessor's leftovers (a
    // stopped circuit is cancelled, not drained, so its last output may
    // land after its close).
    for (const binding of sourceBindings.concat(targetBindings)) {
        runtime.resetTopic(binding.name);
    }

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

    applyTransforms(circuitHandle, controllerSpec);
    circuitHandle.commit();

    const components = new Set([circuitName]);
    const handles = [];
    try {
        // Targets first: the output consumers must be subscribed before any
        // source starts flowing, or the circuit's first outputs land in the
        // topic integral and replay as one batched delta.
        for (const targetConfig of targetConfigs) {
            const h = startTargetHandle(operatorName, controllerPrefix, targetConfig);
            handles.push(h);
            components.add(h.name());
        }
        for (const sourceConfig of sourceConfigs) {
            const h = startSourceHandle(controllerPrefix, controllerSpec, sourceConfig);
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
            // The controller owns its topics: reset their retained
            // integrals so a restarted instance bootstraps from its own
            // output alone, not from a multiset mixing the dead circuit's
            // state with the new one's.
            for (const binding of sourceBindings.concat(targetBindings)) {
                runtime.resetTopic(binding.name);
            }
        },
    };
}

module.exports = {
    startController,
    exactlyOneOf3,
};
