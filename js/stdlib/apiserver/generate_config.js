"use strict";

const apiserver = require("./index");

function main(args) {
    return apiserver.generateConfig(args);
}

module.exports = main;
module.exports.main = main;
