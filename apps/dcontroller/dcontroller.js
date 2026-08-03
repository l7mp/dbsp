"use strict";

const { DControllerManager } = require("./lib/manager");
const { createLogger } = require("log");
const { RuntimeConfig } = require("./lib/config");

const logger = createLogger("dcontroller.main");
const config = new RuntimeConfig();
const runtimeConfigFile = config.parseArgs(process.argv.slice(2))
      || process.env.DCONTROLLER_RUNTIME_CONFIG
      || "";

let runtimeConfig = config.makeFromEnv();
if (runtimeConfigFile) {
    runtimeConfig = config.merge(runtimeConfig, config.readFromFile(runtimeConfigFile));
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
