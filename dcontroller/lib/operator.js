"use strict";

const { startController } = require("./controller");
const { collectOwnedViewGVKs } = require("./gvk");
const { ErrorRing } = require("./status");
const { formatError } = require("log");

const ERROR_RING_CAPACITY = 5;

function deepClone(value) {
    return JSON.parse(JSON.stringify(value));
}

function registerViewGVKs(logger, gvks, operator) {
    if (gvks.length === 0) {
        return;
    }
    kubernetes.runtime.registerViews({ gvks });
    logger.debug({
        event_type: "operator_view_gvks_registered",
        topic: operator,
        gvks,
    }, "registered operator view GVKs");
}

function unregisterViewGVKs(logger, gvks, operator) {
    if (gvks.length === 0) {
        return;
    }
    kubernetes.runtime.unregisterViews({ gvks });
    logger.debug({
        event_type: "operator_view_gvks_unregistered",
        topic: operator,
        gvks,
    }, "unregistered operator view GVKs");
}

function startOperatorInstance(operatorDoc, logger) {
    const name = operatorDoc.metadata.name;
    const controllers = operatorDoc.spec.controllers;
    if (controllers.length === 0) {
        throw new Error(`operator ${JSON.stringify(name)} must define at least one controller`);
    }

    const state = {
        name,
        generation: operatorDoc.metadata.generation,
        doc: deepClone(operatorDoc),
        controllers: [],
        errors: new ErrorRing(ERROR_RING_CAPACITY),
        viewGVKs: [],
    };

    try {
        state.viewGVKs = collectOwnedViewGVKs(name, controllers);
        registerViewGVKs(logger, state.viewGVKs, name);

        for (const controllerSpec of controllers) {
            const controllerLogger = logger.child({
                operator: name,
                controller: controllerSpec.name,
            });
            state.controllers.push(startController(name, controllerSpec, controllerLogger));
        }
    } catch (err) {
        stopOperatorInstance(state, logger);
        throw err;
    }

    logger.info({
        event_type: "operator_started",
        topic: name,
        controllers: state.controllers.length,
    }, "operator started");

    return state;
}

function stopOperatorInstance(state, logger) {
    for (let i = state.controllers.length - 1; i >= 0; i -= 1) {
        const controllerRuntime = state.controllers[i];
        try {
            controllerRuntime.close();
        } catch (err) {
            logger.warn({
                event_type: "operator_controller_close_failed",
                operator: state.name,
                controller: controllerRuntime.name,
                error: formatError(err),
            }, "failed to close controller runtime");
        }
    }
    state.controllers = [];

    try {
        unregisterViewGVKs(logger, state.viewGVKs, state.name);
    } catch (err) {
        logger.warn({
            event_type: "operator_view_gvks_unregister_failed",
            operator: state.name,
            error: formatError(err),
        }, "failed to unregister view GVKs");
    }
    state.viewGVKs = [];

    logger.info({
        event_type: "operator_stopped",
        topic: state.name,
    }, "operator stopped");
}

module.exports = {
    startOperatorInstance,
    stopOperatorInstance,
};
