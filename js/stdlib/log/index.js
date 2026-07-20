"use strict";

const pino = require("pino/browser");

const env = (process && process.env) || {};
const level = env.LOG_LEVEL || env.DBSP_LOG_LEVEL || "info";

const root = pino({
  level,
  messageKey: "message",
  timestamp: pino.stdTimeFunctions.isoTime,
  browser: {
    write(o) {
      const line = JSON.stringify(o);
      (o.levelValue >= 50 ? console.error : console.log)(line);
    },
    formatters: {
      level: (label, value) => ({ level: label, levelValue: value }),
    },
  },
});

function createLogger(component, fields) {
  return root.child(Object.assign({ component }, fields || {}));
}

function formatError(err) {
  return err && err.message ? err.message : String(err || "unknown error");
}

module.exports = { createLogger, formatError };
