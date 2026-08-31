// Wiring that pumps the pipeline's xDS row topics into an in-process xDS
// server: the rows are Envoy protojson resources, so the xDS updaters
// consume them directly.

const { TOPICS } = require("./config.js");

// installXds starts an in-process xDS server and attaches its delta
// consumers to the row topics. Returns the server handle ({name, address}).
function installXds(options = {}) {
  const topics = options.topics || TOPICS;
  const server = xds.server.start({ address: options.address || "127.0.0.1:18000" });

  xds.update(topics.xds.listeners, { type: "lds" });
  xds.update(topics.xds.routes, { type: "rds" });
  xds.update(topics.xds.clusters, { type: "cds" });
  xds.update(topics.xds.endpoints, { type: "eds" });

  return server;
}

module.exports = { installXds };
