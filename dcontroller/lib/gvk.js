"use strict";

const VIEW_GROUP_SUFFIX = ".view.dcontroller.io";

function resolveResourceGVK(operatorName, resource) {
  const opts = { kind: resource.kind };

  if (Object.prototype.hasOwnProperty.call(resource, "apiGroup")) {
    opts.apiGroup = resource.apiGroup;
  } else {
    opts.operator = operatorName;
  }

  if (resource.version != null) {
    opts.version = resource.version;
  }

  return kubernetes.runtime.resolveGVK(opts);
}

function gvkToString(gvk) {
  if (gvk.gvk) {
    // Core-group canonical form ("/v1/Pod") collapses to "v1/Pod".
    if (gvk.gvk.startsWith("/")) {
      const parts = gvk.gvk.split("/");
      if (parts.length === 3 && parts[1] && parts[2]) {
        return `${parts[1]}/${parts[2]}`;
      }
    }
    return gvk.gvk;
  }
  if (!gvk.group) {
    return `${gvk.version}/${gvk.kind}`;
  }
  return `${gvk.group}/${gvk.version}/${gvk.kind}`;
}

function collectOwnedViewGVKs(operatorName, controllers) {
  const ownGroup = `${operatorName}${VIEW_GROUP_SUFFIX}`;
  const out = [];
  const seen = new Set();

  for (const controller of controllers) {
    const resources = (controller.sources || []).concat(controller.targets || []);
    for (const resource of resources) {
      const gvk = resolveResourceGVK(operatorName, resource);
      if (gvk.group !== ownGroup) {
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
};
