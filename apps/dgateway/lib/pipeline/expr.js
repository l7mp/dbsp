// Shared expression fragments and constants for the delta-gateway pipeline
// modules. Everything here is a plain data structure (or a function
// returning one) in the aggregation expression language.

const SUPPORTED_PROTOCOLS = ["HTTP", "HTTPS"];

// kindInKinds builds the "the route kind appears in the listener's
// supported kinds" predicate; kindsExpr evaluates to a list of
// {group, kind} entries.
function kindInKinds(kindExpr, kindsExpr) {
  return { "@any": [{ "@eq": ["$$.kind", kindExpr] }, kindsExpr] };
}

// Hostname matching. A route hostname h matches a listener hostname L when
// they are equal, when L is a wildcard (*.suffix) and h ends in .suffix, or
// when h is a wildcard and L ends in h's suffix. Wildcards stand for one or
// more leading labels, so "example.com" does not match "*.example.com".
// hostnameMatch builds the predicate with $$ bound to the route hostname;
// listenerExpr must evaluate to the listener hostname.
function hostnameMatch(listenerExpr) {
  // @substring is 1-based, so position 2 keeps the leading dot of the
  // suffix: "*.example.com" -> ".example.com".
  const listenerSuffix = { "@substring": [listenerExpr, 2] };
  const routeSuffix = { "@substring": ["$$.", 2] };
  return {
    "@or": [
      { "@eq": ["$$.", listenerExpr] },
      {
        "@and": [
          { "@startswith": [listenerExpr, "*."] },
          { "@endswith": ["$$.", listenerSuffix] },
        ],
      },
      {
        "@and": [
          { "@startswith": ["$$.", "*."] },
          { "@endswith": [listenerExpr, routeSuffix] },
        ],
      },
    ],
  };
}

// hostnameAdmitted builds the attachment-time hostname check between a route
// row and a listener row: either side declaring no hostname passes;
// otherwise at least one route hostname must intersect the listener
// hostname (wildcards included). hostnamesExpr addresses the route side,
// listenerHostnameExpr the listener side ("" when undeclared).
function hostnameAdmitted(hostnamesExpr, listenerHostnameExpr) {
  return {
    "@or": [
      { "@eq": [listenerHostnameExpr, ""] },
      { "@eq": [{ "@len": [hostnamesExpr] }, 0] },
      { "@any": [hostnameMatch(listenerHostnameExpr), hostnamesExpr] },
    ],
  };
}

// namespaceAllowed builds the allowedRoutes namespace policy check between
// a route row and a listener row: All admits everything, Same admits the
// gateway's own namespace, Selector matches the label selector against the
// route namespace's labels (@selectorMatches; the labels are joined onto the
// route rows from the Namespace input).
function namespaceAllowed(
  routeNamespaceExpr,
  fromNamespacesExpr,
  gatewayNamespaceExpr,
  selectorExpr,
  nsLabelsExpr,
) {
  return {
    "@or": [
      { "@eq": [fromNamespacesExpr, "All"] },
      {
        "@and": [
          { "@eq": [fromNamespacesExpr, "Same"] },
          { "@eq": [routeNamespaceExpr, gatewayNamespaceExpr] },
        ],
      },
      {
        "@and": [
          { "@eq": [fromNamespacesExpr, "Selector"] },
          { "@selectorMatches": [selectorExpr, nsLabelsExpr] },
        ],
      },
    ],
  };
}

// sectionMatches builds the parentRef sectionName check against a listener.
function sectionMatches(parentRefRoot, listenerNameExpr) {
  return {
    "@or": [
      { "@not": { "@exists": `${parentRefRoot}.sectionName` } },
      { "@eq": [`${parentRefRoot}.sectionName`, listenerNameExpr] },
    ],
  };
}

// effectiveDomains computes the virtual-host domains of an attached route
// row ({hostnames, listener.hostname} fields, the hostname is "" when the
// listener declares none): the route hostnames when only the route has
// any, the listener hostname when only the listener has one, "*" when
// neither, and the intersection otherwise - taking the more specific side of
// each matching pair, so a wildcard route hostname narrows to an exact
// listener hostname.
const effectiveDomains = {
  "@cond": [
    { "@eq": [{ "@len": ["$.hostnames"] }, 0] },
    {
      "@cond": [
        { "@eq": ["$.listener.hostname", ""] },
        ["*"],
        { "@list": ["$.listener.hostname"] },
      ],
    },
    {
      "@cond": [
        { "@eq": ["$.listener.hostname", ""] },
        "$.hostnames",
        {
          "@map": [
            {
              "@cond": [
                {
                  "@and": [
                    { "@startswith": ["$$.", "*."] },
                    { "@not": { "@startswith": ["$.listener.hostname", "*."] } },
                  ],
                },
                "$.listener.hostname",
                "$$.",
              ],
            },
            { "@filter": [hostnameMatch("$.listener.hostname"), "$.hostnames"] },
          ],
        },
      ],
    },
  ],
};

// transitionStamp builds the @stamp stage that samples a condition's
// lastTransitionTime: the round clock is sampled when the key (the object,
// the entry within it, and the condition's status) first appears and held
// while it lasts, so a status flip restamps and a reason or message edit
// holds. The sampled value is written to every path listed (conditions that
// share a verdict share a sample). The sample is state of the circuit: a
// restarted controller samples afresh and rewrites every condition once.
const transitionStamp = (keyExprs, paths) => ({
  "@stamp": [keyExprs, Object.fromEntries(paths.map((p) => [p, { "@now": null }]))],
});

// sortByKeys builds a nested stable @sortByKey chain: keys in significance
// order, most significant first, each ascending (negate a numeric key for
// descending). Stability makes the composition a lexicographic multi-key
// sort.
function sortByKeys(keys, list) {
  return keys.reduceRight((acc, k) => ({ "@sortByKey": [k, acc] }), list);
}

// gatewayAddressExpr assigns a gateway its loopback data-plane address from
// the pool: base octets from the pool CIDR, the two host octets folded from
// a content hash of namespace/name (24-bit slices of the @hash hex string
// cast through @int, reduced with @mod). The two call sites (Gateway status
// and the Envoy listener bind address) must agree, which is why this is a
// shared builder.
function gatewayAddressExpr(pool, gatewayRoot) {
  const base = pool.split("/")[0].split(".").slice(0, 2).join(".");
  const hash = {
    "@hash": { "@concat": [`${gatewayRoot}.namespace`, "/", `${gatewayRoot}.name`] },
  };
  // @substring is 1-based SQL-style [string, start, count]; a 6-hex-char
  // slice is 24 bits, comfortably inside @int's int64.
  const octet = (start, mod) => ({
    "@mod": [{ "@int": { "@substring": [hash, start, 6] } }, mod],
  });
  return {
    "@concat": [
      `${base}.`,
      octet(1, 254),
      ".",
      { "@add": [1, octet(7, 253)] },
    ],
  };
}

module.exports = {
  SUPPORTED_PROTOCOLS,
  transitionStamp,
  sortByKeys,
  gatewayAddressExpr,
  kindInKinds,
  hostnameAdmitted,
  namespaceAllowed,
  sectionMatches,
  effectiveDomains,
};
