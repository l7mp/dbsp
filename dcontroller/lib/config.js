"use strict";

const fs = require("fs");
const minimist = require("minimist");

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

class RuntimeConfig {
    constructor() {
        this.config = null;
    }

    parseArgs(argv) {
        const parsed = minimist(argv, {
            string: ["runtime-config", "config"],
            alias: { config: "runtime-config" },
        });
        const raw = parsed["runtime-config"];
        return Array.isArray(raw) ? raw[raw.length - 1] : raw;
    }

    readFromFile(path) {
        const decoded = JSON.parse(fs.readFileSync(path, "utf8"));
        return decoded.kubernetes || decoded;
    }

    makeFromEnv() {
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

    merge(base, override) {
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
};

module.exports = {
  RuntimeConfig,
};
