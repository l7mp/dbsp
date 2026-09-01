"use strict";

const { ErrorRing } = require("./status");
const { formatError } = require("log");

const ERROR_RING_CAPACITY = 5;

// startOperatorInstance creates one operator from its Operator document:
// an operator is one private DBSP runtime, constructed and assembled by
// runtime.create and started here. Compilation, transforms and the
// source/target bindings all happen in the engine loader. What remains
// here is the manager's bookkeeping: the status error ring; runtime
// errors arrive per runtime through the handle's onError.
function startOperatorInstance(operatorDoc, logger) {
    const name = operatorDoc.metadata.name;
    const rtSpec = operatorDoc.spec;
    if (!rtSpec || !rtSpec.circuits || rtSpec.circuits.length === 0) {
        throw new Error(`operator ${JSON.stringify(name)} must define at least one circuit`);
    }

    const handle = runtime.create(name, {
        sources: rtSpec.sources,
        circuits: rtSpec.circuits,
        targets: rtSpec.targets,
    });
    handle.start();
    const state = {
        name,
        generation: operatorDoc.metadata.generation,
        doc: JSON.parse(JSON.stringify(operatorDoc)),
        handle,
        errors: new ErrorRing(ERROR_RING_CAPACITY),
    };

    logger.info({
        event_type: "operator_started",
        topic: name,
        circuits: rtSpec.circuits.length,
    }, "operator started");

    return state;
}

function stopOperatorInstance(state, logger) {
    try {
        state.handle.close();
    } catch (err) {
        logger.warn({
            event_type: "operator_close_failed",
            operator: state.name,
            error: formatError(err),
        }, "failed to close operator");
    }

    logger.info({
        event_type: "operator_stopped",
        topic: state.name,
    }, "operator stopped");
}

module.exports = {
    startOperatorInstance,
    stopOperatorInstance,
};
