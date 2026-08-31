"use strict";

const { ErrorRing } = require("./status");
const { formatError } = require("log");

const ERROR_RING_CAPACITY = 5;

// startOperatorInstance loads one Operator document through the engine's
// serialized-operator loader (operator.load): compilation, transforms,
// and the source/target bindings all happen there. What remains here is
// the manager's bookkeeping: the component set for runtime-error routing
// and the status error ring.
function startOperatorInstance(operatorDoc, logger) {
    const name = operatorDoc.metadata.name;
    const controllers = operatorDoc.spec.controllers;
    if (!controllers || controllers.length === 0) {
        throw new Error(`operator ${JSON.stringify(name)} must define at least one controller`);
    }

    const handle = operator.load(name, { controllers });
    const state = {
        name,
        generation: operatorDoc.metadata.generation,
        doc: JSON.parse(JSON.stringify(operatorDoc)),
        handle,
        components: new Set(handle.components()),
        errors: new ErrorRing(ERROR_RING_CAPACITY),
    };

    logger.info({
        event_type: "operator_started",
        topic: name,
        controllers: controllers.length,
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
