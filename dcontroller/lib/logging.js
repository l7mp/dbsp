"use strict";

const LEVELS = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function normalizeLevel(raw) {
  const key = String(raw || "info").trim().toLowerCase();
  if (Object.prototype.hasOwnProperty.call(LEVELS, key)) {
    return key;
  }
  return "info";
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

function createLogger(component, baseFields) {
  const threshold = LEVELS[normalizeLevel(process.env.DCONTROLLER_LOG_LEVEL)];
  const common = Object.assign({}, baseFields || {}, { component });

  function write(level, message, fields) {
    const levelKey = normalizeLevel(level);
    if (LEVELS[levelKey] < threshold) {
      return;
    }

    const payload = Object.assign({}, common, fields || {}, {
      ts: new Date().toISOString(),
      level: levelKey,
      message: String(message || ""),
    });

    const line = JSON.stringify(payload);
    if (levelKey === "error") {
      console.error(line);
      return;
    }
    console.log(line);
  }

  return {
    child(childComponent, childFields) {
      const suffix = childComponent ? `.${childComponent}` : "";
      return createLogger(`${component}${suffix}`, Object.assign({}, common, childFields || {}));
    },
    debug(message, fields) {
      write("debug", message, fields);
    },
    info(message, fields) {
      write("info", message, fields);
    },
    warn(message, fields) {
      write("warn", message, fields);
    },
    error(message, fields) {
      write("error", message, fields);
    },
  };
}

module.exports = {
  createLogger,
  formatError,
};
