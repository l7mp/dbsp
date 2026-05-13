"use strict";

const apiserver = require("./index");

function main(args) {
    return apiserver.getConfig(args);
}

module.exports = main;
module.exports.main = main;
