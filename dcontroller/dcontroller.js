"use strict";

const fs = require("fs");
const minimist = require("minimist");
const { DControllerManager } = require("./lib/manager");
const { createLogger } = require("log");

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

function parseRuntimeConfigArg(argv) {
  const parsed = minimist(argv, {
    string: ["runtime-config", "config"],
    alias: { config: "runtime-config" },
  });
  const raw = parsed["runtime-config"];
  return Array.isArray(raw) ? raw[raw.length - 1] : raw;
}

function readRuntimeConfigFromFile(path) {
  const decoded = JSON.parse(fs.readFileSync(path, "utf8"));
  return decoded.kubernetes || decoded;
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
  const out = Object.assign({}, base);

  if (Object.prototype.hasOwnProperty.call(override, "kubeconfig")) {
    out.kubeconfig = override.kubeconfig;
  }
  if (override.apiServer) {
    out.apiServer = Object.assign({}, out.apiServer || {}, override.apiServer);
  }
  if (override.auth) {
    out.auth = Object.assign({}, out.auth || {}, override.auth);
  }

  return out;
}

const logger = createLogger("dcontroller.main");
const runtimeConfigFile = parseRuntimeConfigArg(process.argv.slice(2))
  || process.env.DCONTROLLER_RUNTIME_CONFIG
  || "";

let runtimeConfig = runtimeConfigFromEnv();
if (runtimeConfigFile) {
  runtimeConfig = mergeRuntimeConfig(runtimeConfig, readRuntimeConfigFromFile(runtimeConfigFile));
}

const manager = new DControllerManager({
  runtimeConfig,
  logger: createLogger("dcontroller"),
});

manager.start();

logger.info({
  event_type: "dcontroller_script_initialized",
  runtime_config_file: runtimeConfigFile,
}, "dcontroller script initialized");
