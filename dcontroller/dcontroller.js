"use strict";

const fs = require("fs");
const minimist = require("minimist");
const { DControllerManager } = require("./lib/manager");
const { createLogger, formatError } = require("log");

function parseBool(raw, fallback) {
  if (raw == null || raw === "") {
    return fallback;
  }
  const s = String(raw).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "yes" || s === "on") {
    return true;
  }
  if (s === "0" || s === "false" || s === "no" || s === "off") {
    return false;
  }
  return fallback;
}

function parseIntOr(raw, fallback) {
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.trunc(n);
}

function parseArgs(argv) {
  const parsed = minimist(argv, {
    string: ["runtime-config", "config"],
    alias: { config: "runtime-config" },
  });
  let raw = parsed["runtime-config"];
  if (Array.isArray(raw)) {
    raw = raw.length > 0 ? raw[raw.length - 1] : "";
  }
  return { runtimeConfigFile: raw == null ? "" : String(raw) };
}

function readRuntimeConfigFromFile(path) {
  if (!path) {
    return {};
  }
  const raw = fs.readFileSync(path, "utf8");
  const decoded = JSON.parse(raw);
  if (!decoded || typeof decoded !== "object") {
    return {};
  }

  if (decoded.kubernetes && typeof decoded.kubernetes === "object") {
    return decoded.kubernetes;
  }
  return decoded;
}

function runtimeConfigFromEnv() {
  const cfg = {};

  if (process.env.DCONTROLLER_KUBECONFIG) {
    cfg.kubeconfig = process.env.DCONTROLLER_KUBECONFIG;
  }

  const enabled = parseBool(process.env.DCONTROLLER_API_SERVER_ENABLED, true);
  if (!enabled) {
    return cfg;
  }

  const mode = String(process.env.DCONTROLLER_API_SERVER_MODE || "development").trim().toLowerCase();
  const httpMode = parseBool(process.env.DCONTROLLER_API_SERVER_HTTP, mode === "development");

  cfg.apiServer = {
    addr: process.env.DCONTROLLER_API_SERVER_ADDR || "0.0.0.0",
    port: parseIntOr(process.env.DCONTROLLER_API_SERVER_PORT, 8443),
    http: httpMode,
    insecure: parseBool(process.env.DCONTROLLER_API_SERVER_INSECURE, mode === "development"),
    certFile: process.env.DCONTROLLER_API_SERVER_CERT_FILE || "apiserver.crt",
    keyFile: process.env.DCONTROLLER_API_SERVER_KEY_FILE || "apiserver.key",
    enableOpenAPI: parseBool(process.env.DCONTROLLER_API_SERVER_OPENAPI, true),
  };

  const privateKeyFile = process.env.DCONTROLLER_AUTH_PRIVATE_KEY_FILE;
  const publicKeyFile = process.env.DCONTROLLER_AUTH_PUBLIC_KEY_FILE;
  if (privateKeyFile || publicKeyFile) {
    cfg.auth = {
      privateKeyFile: privateKeyFile || "",
      publicKeyFile: publicKeyFile || "",
    };
  }

  return cfg;
}

function mergeRuntimeConfig(base, override) {
  const out = Object.assign({}, base || {});
  const src = override && typeof override === "object" ? override : {};

  if (Object.prototype.hasOwnProperty.call(src, "kubeconfig")) {
    out.kubeconfig = src.kubeconfig;
  }

  if (src.apiServer && typeof src.apiServer === "object") {
    out.apiServer = Object.assign({}, out.apiServer || {}, src.apiServer);
  }

  if (src.auth && typeof src.auth === "object") {
    out.auth = Object.assign({}, out.auth || {}, src.auth);
  }

  return out;
}

function main() {
  const logger = createLogger("dcontroller.main");
  const args = parseArgs(process.argv.slice(2));

  const filePath = args.runtimeConfigFile || process.env.DCONTROLLER_RUNTIME_CONFIG || "";
  let runtimeConfig = runtimeConfigFromEnv();

  if (filePath) {
    const fromFile = readRuntimeConfigFromFile(filePath);
    runtimeConfig = mergeRuntimeConfig(runtimeConfig, fromFile);
  }

  const manager = new DControllerManager({
    runtimeConfig,
    logger: createLogger("dcontroller"),
  });

  manager.start();

  logger.info({
    event_type: "dbsp runtime event",
    runtime_config_file: filePath || "",
  }, "dcontroller script initialized");
}

try {
  main();
} catch (err) {
  createLogger("dcontroller.main").error({
    event_type: "dbsp runtime event",
    error: formatError(err),
  }, "failed to start dcontroller");
  throw err;
}
