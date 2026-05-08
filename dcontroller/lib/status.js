"use strict";

class ErrorRing {
  constructor(capacity) {
    const parsed = Number(capacity);
    this.capacity = Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : 5;
    this.items = [];
  }

  push(err) {
    const message = formatError(err);
    if (!message) {
      return;
    }
    this.items.push(message);
    while (this.items.length > this.capacity) {
      this.items.shift();
    }
  }

  values() {
    return this.items.slice();
  }
}

function formatError(err) {
  if (!err) {
    return "unknown error";
  }
  if (typeof err === "string") {
    return err;
  }
  if (err && typeof err.message === "string" && err.message.length > 0) {
    return err.message;
  }
  return String(err);
}

function normalizeGeneration(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    return 0;
  }
  return Math.trunc(n);
}

function buildReadyStatus(generation, errorRing) {
  const errors = errorRing ? errorRing.values() : [];
  const now = new Date().toISOString();

  if (errors.length === 0) {
    return {
      conditions: [
        {
          type: "Ready",
          status: "True",
          reason: "Ready",
          observedGeneration: normalizeGeneration(generation),
          lastTransitionTime: now,
          message: "controllers report no reconciliation error",
        },
      ],
    };
  }

  return {
    conditions: [
      {
        type: "Ready",
        status: "False",
        reason: "ReconciliationFailed",
        observedGeneration: normalizeGeneration(generation),
        lastTransitionTime: now,
        message: "reconciliation failed for at least one controller",
      },
    ],
    lastErrors: errors,
  };
}

function buildNotReadyStatus(generation, err) {
  const now = new Date().toISOString();
  return {
    conditions: [
      {
        type: "Ready",
        status: "False",
        reason: "NotReady",
        observedGeneration: normalizeGeneration(generation),
        lastTransitionTime: now,
        message: "failed to initialize operator",
      },
    ],
    lastErrors: [formatError(err)],
  };
}

module.exports = {
  ErrorRing,
  buildReadyStatus,
  buildNotReadyStatus,
  normalizeGeneration,
};
