"use strict";

const VIEW_GROUP_SUFFIX = ".view.dcontroller.io";

function operatorViewGroup(operatorName) {
  return `${String(operatorName || "").trim()}${VIEW_GROUP_SUFFIX}`;
}

function resolveResourceGVK(operatorName, resource) {
  if (!resource || typeof resource !== "object") {
    throw new Error("resource must be an object");
  }
  if (!resource.kind) {
    throw new Error("resource.kind is required");
  }

  const opts = {
    kind: String(resource.kind),
  };

  const hasAPIGroup = Object.prototype.hasOwnProperty.call(resource, "apiGroup");
  if (hasAPIGroup) {
    opts.apiGroup = resource.apiGroup;
  } else {
    opts.operator = operatorName;
  }

  if (Object.prototype.hasOwnProperty.call(resource, "version") && resource.version != null) {
    opts.version = resource.version;
  }

  return kubernetes.runtime.resolveGVK(opts);
}

function gvkToString(gvk) {
  if (gvk && typeof gvk.gvk === "string" && gvk.gvk.length > 0) {
    if (gvk.gvk.startsWith("/")) {
      const parts = gvk.gvk.split("/");
      if (parts.length === 3 && parts[0] === "" && parts[1] && parts[2]) {
        return `${parts[1]}/${parts[2]}`;
      }
    }
    return gvk.gvk;
  }
  const group = gvk && typeof gvk.group === "string" ? gvk.group : "";
  const version = gvk && typeof gvk.version === "string" ? gvk.version : "";
  const kind = gvk && typeof gvk.kind === "string" ? gvk.kind : "";
  if (!group) {
    return `${version}/${kind}`;
  }
  return `${group}/${version}/${kind}`;
}

function collectOwnedViewGVKs(operatorName, controllers) {
  const ownGroup = operatorViewGroup(operatorName);
  const out = [];
  const seen = new Set();

  const list = Array.isArray(controllers) ? controllers : [];
  for (const controller of list) {
    const sources = Array.isArray(controller && controller.sources) ? controller.sources : [];
    const targets = Array.isArray(controller && controller.targets) ? controller.targets : [];
    const resources = sources.concat(targets);

    for (const resource of resources) {
      const gvk = resolveResourceGVK(operatorName, resource);
      if (!gvk || gvk.group !== ownGroup) {
        continue;
      }
      const key = gvkToString(gvk);
      if (!seen.has(key)) {
        seen.add(key);
        out.push(key);
      }
    }
  }

  return out;
}

module.exports = {
  resolveResourceGVK,
  gvkToString,
  collectOwnedViewGVKs,
  operatorViewGroup,
};
