"use strict";

class ErrorRing {
  constructor(capacity) {
    this.capacity = capacity;
    this.items = [];
  }

  push(err) {
    this.items.push(formatError(err));
    while (this.items.length > this.capacity) {
      this.items.shift();
    }
  }

  values() {
    return this.items.slice();
  }
}

function formatError(err) {
  if (typeof err === "string") {
    return err;
  }
  if (err && err.message) {
    return err.message;
  }
  return String(err);
}

function buildReadyStatus(generation, errorRing) {
  const errors = errorRing.values();
  const now = new Date().toISOString();

  if (errors.length === 0) {
    return {
      conditions: [
        {
          type: "Ready",
          status: "True",
          reason: "Ready",
          observedGeneration: generation,
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
        observedGeneration: generation,
        lastTransitionTime: now,
        message: "reconciliation failed for at least one controller",
      },
    ],
    lastErrors: errors,
  };
}

function buildNotReadyStatus(generation, err) {
  return {
    conditions: [
      {
        type: "Ready",
        status: "False",
        reason: "NotReady",
        observedGeneration: generation,
        lastTransitionTime: new Date().toISOString(),
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
};
