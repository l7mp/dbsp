"use strict";

const minimist = require("minimist");
const process = require("process");

const DEFAULT_STRING_FLAGS = [
    "addr",
    "api-server-addr",
    "api-server-port",
    "cert-file",
    "default-namespace",
    "expiry",
    "hostnames",
    "key-file",
    "kubeconfig",
    "namespaces",
    "output-file",
    "port",
    "profile",
    "resource-names",
    "rules",
    "rules-file",
    "server-address",
    "tls-cert-file",
    "tls-key-file",
    "user",
];

const DEFAULT_BOOLEAN_FLAGS = ["help", "http", "insecure", "json"];

const PROFILE_RULES = {
    viewer: [{ verbs: ["get", "list", "watch"], apiGroups: ["*"], resources: ["*"] }],
    editor: [{ verbs: ["get", "list", "watch", "create", "update", "patch", "delete"], apiGroups: ["*"], resources: ["*"] }],
    admin: [{ verbs: ["*"], apiGroups: ["*"], resources: ["*"] }],
};

function parseArgs(input, opts) {
    if (Array.isArray(input)) {
        return minimist(input, Object.assign({
            string: DEFAULT_STRING_FLAGS,
            boolean: DEFAULT_BOOLEAN_FLAGS,
            alias: {
                h: "help",
                u: "user",
                n: "namespaces",
            },
        }, opts || {}));
    }
    if (input == null) {
        return parseArgs(process.argv.slice(2), opts);
    }
    return Object.assign({ _: [] }, input);
}

function isArgvInput(input) {
    return input == null || Array.isArray(input);
}

function pick(obj, names) {
    for (const name of names) {
        if (Object.prototype.hasOwnProperty.call(obj, name) && obj[name] !== undefined) {
            const value = obj[name];
            return Array.isArray(value) ? value[value.length - 1] : value;
        }
    }
    return undefined;
}

function hasAny(obj, names) {
    return names.some((name) => Object.prototype.hasOwnProperty.call(obj, name));
}

function list(value) {
    if (value == null || value === false) {
        return [];
    }
    const values = Array.isArray(value) ? value : [value];
    const out = [];
    for (const item of values) {
        for (const part of String(item).split(",")) {
            const trimmed = part.trim();
            if (trimmed !== "") {
                out.push(trimmed);
            }
        }
    }
    return out;
}

function intValue(value) {
    if (value == null || value === "") {
        return undefined;
    }
    const n = Number(value);
    if (!Number.isFinite(n)) {
        throw new Error(`invalid integer ${JSON.stringify(value)}`);
    }
    return Math.trunc(n);
}

function parseJSON(value, flagName) {
    if (value == null || value === "") {
        return undefined;
    }
    if (typeof value !== "string") {
        return value;
    }
    try {
        return JSON.parse(value);
    } catch (err) {
        throw new Error(`invalid JSON in ${flagName}: ${err.message}`);
    }
}

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

function runtimeConfig(input) {
    const parsed = parseArgs(input);
    const cfg = {};
    const api = {};

    const addr = pick(parsed, ["api-server-addr", "addr"]);
    const port = pick(parsed, ["api-server-port", "port"]);
    const certFile = pick(parsed, ["tls-cert-file", "cert-file", "certFile"]);
    const keyFile = pick(parsed, ["tls-key-file", "key-file", "keyFile"]);

    if (addr != null && addr !== "") {
        api.addr = String(addr);
    }
    const parsedPort = intValue(port);
    if (parsedPort !== undefined) {
        api.port = parsedPort;
    }
    if (hasAny(parsed, ["http"])) {
        api.http = Boolean(parsed.http);
    }
    if (hasAny(parsed, ["insecure"])) {
        api.insecure = Boolean(parsed.insecure);
    }
    if (certFile != null && certFile !== "") {
        api.certFile = String(certFile);
    }
    if (keyFile != null && keyFile !== "") {
        api.keyFile = String(keyFile);
    }
    if (Object.keys(api).length > 0) {
        cfg.apiServer = api;
    }

    return cfg;
}

function configObject(input) {
    return kubernetes.runtime.config(runtimeConfig(input));
}

function generateKeysOptions(parsed) {
    const keyFile = pick(parsed, ["tls-key-file", "key-file", "keyFile"]);
    const certFile = pick(parsed, ["tls-cert-file", "cert-file", "certFile"]);
    const opts = {};

    const hostnames = list(pick(parsed, ["hostnames", "hostname"]));
    if (hostnames.length > 0) {
        opts.hostnames = hostnames;
    }
    if (keyFile != null && keyFile !== "") {
        opts.keyFile = String(keyFile);
    }
    if (certFile != null && certFile !== "") {
        opts.certFile = String(certFile);
    }

    return opts;
}

function generateKeys(input) {
    const parsed = parseArgs(input);
    if (parsed.help) {
        usage("generate-keys");
        return undefined;
    }
    return configObject(parsed).generateKeys(generateKeysOptions(parsed));
}

function rulesFromProfile(profile) {
    if (profile == null || profile === "") {
        return undefined;
    }
    const key = String(profile).trim().toLowerCase();
    if (!Object.prototype.hasOwnProperty.call(PROFILE_RULES, key)) {
        throw new Error(`unknown profile ${JSON.stringify(profile)}; use viewer, editor, or admin`);
    }
    return clone(PROFILE_RULES[key]);
}

function generateConfigOptions(parsed) {
    const user = pick(parsed, ["user", "username"]);
    const keyFile = pick(parsed, ["tls-key-file", "key-file", "keyFile"]);
    const rulesFile = pick(parsed, ["rules-file", "rulesFile"]);
    const serverAddress = pick(parsed, ["server-address", "serverAddress"]);
    const defaultNamespace = pick(parsed, ["default-namespace", "defaultNamespace"]);
    const outputFile = pick(parsed, ["output-file", "outputFile"]);
    const expiry = pick(parsed, ["expiry"]);
    const opts = {};

    if (user == null || user === "") {
        throw new Error("missing --user");
    }
    opts.user = String(user);

    const namespaces = list(pick(parsed, ["namespaces", "namespace"]));
    if (namespaces.length > 0) {
        opts.namespaces = namespaces;
    }

    const rawRules = pick(parsed, ["rules"]);
    if (rawRules != null && rawRules !== "") {
        opts.rules = parseJSON(rawRules, "--rules");
    } else {
        const profileRules = rulesFromProfile(pick(parsed, ["profile"]));
        if (profileRules !== undefined) {
            opts.rules = profileRules;
        }
    }

    if (rulesFile != null && rulesFile !== "") {
        opts.rulesFile = String(rulesFile);
        delete opts.rules;
    }

    const resourceNames = list(pick(parsed, ["resource-names", "resourceNames"]));
    if (resourceNames.length > 0) {
        opts.resourceNames = resourceNames;
    }
    if (expiry != null && expiry !== "") {
        opts.expiry = String(expiry);
    }
    if (keyFile != null && keyFile !== "") {
        opts.keyFile = String(keyFile);
    }
    if (serverAddress != null && serverAddress !== "") {
        opts.serverAddress = String(serverAddress);
    }
    if (defaultNamespace != null && defaultNamespace !== "") {
        opts.defaultNamespace = String(defaultNamespace);
    }
    if (hasAny(parsed, ["http"])) {
        opts.http = Boolean(parsed.http);
    }
    if (hasAny(parsed, ["insecure"])) {
        opts.insecure = Boolean(parsed.insecure);
    }
    if (outputFile != null && outputFile !== "") {
        opts.outputFile = String(outputFile);
    }

    return opts;
}

function generateConfig(input) {
    const parsed = parseArgs(input);
    if (parsed.help) {
        usage("generate-config");
        return undefined;
    }
    const yaml = configObject(parsed).generateConfig(generateConfigOptions(parsed));
    if (isArgvInput(input) && !pick(parsed, ["output-file", "outputFile"])) {
        process.stdout.write(String(yaml));
    }
    return yaml;
}

function inspectOptions(parsed) {
    const kubeconfig = pick(parsed, ["kubeconfig"]);
    const certFile = pick(parsed, ["tls-cert-file", "cert-file", "certFile"]);
    const opts = {};
    if (kubeconfig != null && kubeconfig !== "") {
        opts.kubeconfig = String(kubeconfig);
    }
    if (certFile != null && certFile !== "") {
        opts.certFile = String(certFile);
    }
    return opts;
}

function formatRule(rule, index) {
    const parts = [
        `  [${index + 1}] verbs=${JSON.stringify(rule.verbs || [])}`,
        `apiGroups=${JSON.stringify(rule.apiGroups || [])}`,
        `resources=${JSON.stringify(rule.resources || [])}`,
    ];
    if (rule.resourceNames && rule.resourceNames.length > 0) {
        parts.push(`resourceNames=${JSON.stringify(rule.resourceNames)}`);
    }
    return parts.join(" ");
}

function plainObject(value) {
    return JSON.parse(JSON.stringify(value));
}

function normalizeInspection(info) {
    const out = plainObject(info);
    if (!Array.isArray(out.rules)) {
        out.rules = [];
        return out;
    }
    out.rules = out.rules.map((rule) => ({
        verbs: rule.verbs || rule.Verbs || [],
        apiGroups: rule.apiGroups || rule.APIGroups || [],
        resources: rule.resources || rule.Resources || [],
        resourceNames: rule.resourceNames || rule.ResourceNames || [],
        nonResourceURLs: rule.nonResourceURLs || rule.NonResourceURLs || [],
    }));
    return out;
}

function formatInspection(info) {
    const lines = [];
    lines.push("User Information:");
    lines.push(`  Username:   ${info.username || ""}`);
    lines.push(`  Namespaces: ${JSON.stringify(info.namespaces || [])}`);
    const rules = info.rules || [];
    lines.push(`  Rules:      ${rules.length} RBAC policy rule${rules.length === 1 ? "" : "s"}`);
    for (let i = 0; i < rules.length; i++) {
        lines.push(formatRule(rules[i], i));
    }
    lines.push("");
    lines.push("Token Metadata:");
    lines.push(`  Issuer:     ${info.issuer || ""}`);
    if (info.issuedAt) {
        lines.push(`  Issued At:  ${info.issuedAt}`);
    }
    if (info.expiresAt) {
        lines.push(`  Expires At: ${info.expiresAt}`);
    }
    if (info.notBefore) {
        lines.push(`  Not Before: ${info.notBefore}`);
    }
    lines.push(`  Valid:      ${info.valid ? "true" : "false"}`);
    return `${lines.join("\n")}\n`;
}

function getConfig(input) {
    const parsed = parseArgs(input);
    if (parsed.help) {
        usage("get-config");
        return undefined;
    }
    const info = normalizeInspection(configObject(parsed).getConfig(inspectOptions(parsed)));
    if (isArgvInput(input)) {
        const body = parsed.json ? `${JSON.stringify(info, null, 2)}\n` : formatInspection(info);
        process.stdout.write(body);
    }
    return info;
}

function usage(command) {
    const common = [
        "Usage:",
        "  export DBSP_STDLIB=/path/to/js/stdlib",
        "  dbsp apiserver/generate_keys [flags]",
        "  dbsp apiserver/generate_config --user=<name> [flags] > user.config",
        "  dbsp apiserver/get_config --tls-cert-file=apiserver.crt [flags]",
        "",
        "Common flags:",
        "  --hostnames=localhost,127.0.0.1",
        "  --user=viewer",
        "  --namespaces=default,team-a or --namespaces='*'",
        "  --profile=viewer|editor|admin",
        "  --rules='[{\"verbs\":[\"get\"],\"apiGroups\":[\"*\"],\"resources\":[\"*\"]}]'",
        "  --rules-file=rules.json",
        "  --resource-names=name-a,name-b",
        "  --tls-key-file=apiserver.key",
        "  --tls-cert-file=apiserver.crt",
        "  --server-address=localhost:8443",
        "  --http",
        "  --insecure",
    ];
    const title = command ? `apiserver ${command}` : "apiserver";
    process.stderr.write(`${title}\n${common.join("\n")}\n`);
}

function cli(input) {
    const args = input == null ? process.argv.slice(2) : input;
    const command = args[0];
    const rest = args.slice(1);
    switch (command) {
    case "generate-keys":
        return generateKeys(rest);
    case "generate-config":
    case "generate-kubeconfig":
        return generateConfig(rest);
    case "get-config":
    case "inspect-kubeconfig":
        return getConfig(rest);
    case "help":
    case "--help":
    case "-h":
    case undefined:
        usage();
        return undefined;
    default:
        throw new Error(`unknown apiserver command ${JSON.stringify(command)}`);
    }
}

module.exports = {
    cli,
    generateConfig,
    generateKubeConfig: generateConfig,
    generateKeys,
    getConfig,
    inspectKubeConfig: getConfig,
    profiles: Object.keys(PROFILE_RULES),
    runtimeConfig,
};
