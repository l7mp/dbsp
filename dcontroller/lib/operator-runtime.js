"use strict";

const { startController } = require("./controller-runtime");
const { collectOwnedViewGVKs } = require("./gvk");
const { ErrorRing, normalizeGeneration } = require("./status");
const { formatError } = require("./logging");

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function operatorName(operatorDoc) {
  return operatorDoc && operatorDoc.metadata && operatorDoc.metadata.name
    ? String(operatorDoc.metadata.name)
    : "";
}

function generationOf(operatorDoc) {
  const value = operatorDoc && operatorDoc.metadata ? operatorDoc.metadata.generation : 0;
  return normalizeGeneration(value);
}

function registerViewGVKs(logger, gvks, operator) {
  if (!Array.isArray(gvks) || gvks.length === 0) {
    return;
  }
  kubernetes.runtime.registerViews({ gvks });
  logger.debug("registered operator view GVKs", {
    event_type: "dbsp runtime event",
    topic: operator,
    gvks,
  });
}

function unregisterViewGVKs(logger, gvks, operator) {
  if (!Array.isArray(gvks) || gvks.length === 0) {
    return;
  }
  kubernetes.runtime.unregisterViews({ gvks });
  logger.debug("unregistered operator view GVKs", {
    event_type: "dbsp runtime event",
    topic: operator,
    gvks,
  });
}

function startOperatorInstance(operatorDoc, logger, onComponentName) {
  const name = operatorName(operatorDoc);
  if (!name) {
    throw new Error("operator object is missing metadata.name");
  }

  const spec = operatorDoc && operatorDoc.spec ? operatorDoc.spec : {};
  const controllers = Array.isArray(spec.controllers) ? spec.controllers : [];
  if (controllers.length === 0) {
    throw new Error(`operator ${JSON.stringify(name)} must define at least one controller`);
  }

  const state = {
    name,
    generation: generationOf(operatorDoc),
    doc: deepClone(operatorDoc),
    controllers: [],
    components: new Set(),
    errors: new ErrorRing(5),
    viewGVKs: [],
  };

  const registerComponentName = (componentName) => {
    const id = String(componentName || "").trim();
    if (!id) {
      return;
    }
    state.components.add(id);
    if (typeof onComponentName === "function") {
      onComponentName(id);
    }
  };

  try {
    state.viewGVKs = collectOwnedViewGVKs(name, controllers);
    registerViewGVKs(logger, state.viewGVKs, name);

    for (const controllerSpec of controllers) {
      const controllerLogger = logger.child("controller", {
        operator: name,
        controller: controllerSpec && controllerSpec.name ? String(controllerSpec.name) : "",
      });
      const controllerRuntime = startController(name, controllerSpec, controllerLogger, registerComponentName);
      state.controllers.push(controllerRuntime);
    }
  } catch (err) {
    stopOperatorInstance(state, logger);
    throw err;
  }

  logger.info("operator started", {
    event_type: "dbsp runtime event",
    topic: name,
    controllers: state.controllers.length,
  });

  return state;
}

function stopOperatorInstance(state, logger) {
  if (!state || !Array.isArray(state.controllers)) {
    return;
  }

  for (let i = state.controllers.length - 1; i >= 0; i -= 1) {
    const controllerRuntime = state.controllers[i];
    if (!controllerRuntime || typeof controllerRuntime.close !== "function") {
      continue;
    }
    try {
      controllerRuntime.close();
    } catch (err) {
      logger.warn("failed to close controller runtime", {
        operator: state.name,
        controller: controllerRuntime.name,
        error: formatError(err),
      });
    }
  }

  state.controllers = [];

  try {
    unregisterViewGVKs(logger, state.viewGVKs, state.name);
  } catch (err) {
    logger.warn("failed to unregister view GVKs", {
      operator: state.name,
      error: formatError(err),
    });
  }
  state.viewGVKs = [];

  logger.info("operator stopped", {
    event_type: "dbsp runtime event",
    topic: state.name,
  });
}

module.exports = {
  startOperatorInstance,
  stopOperatorInstance,
  operatorName,
  generationOf,
};
