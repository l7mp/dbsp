// Shared expression fragments for the dpolicy pipeline modules. Everything
// here is a build-time macro: a plain data structure (or a function
// returning one) in the aggregation expression language - the pipeline
// stays fully declarative and freezes to YAML as is.

// nsGlob builds the Gatekeeper namespace wildcard check with $$ bound to
// the pattern: a trailing * matches by prefix, anything else exactly.
const nsGlob = (nsExpr) => ({
  "@cond": [
    { "@endswith": ["$$.", "*"] },
    { "@startswith": [nsExpr, { "@substring": ["$$.", 1, { "@sub": [{ "@len": ["$$."] }, 1] }] }] },
    { "@eq": [nsExpr, "$$."] },
  ],
});

// gkMatches builds the supported subset of a Gatekeeper match block
// (kinds, namespaces, excludedNamespaces) evaluated against a target's
// group/kind/namespace expressions. labelSelector is evaluated separately
// with @selectorMatches; anything else fails constraint validation in
// matchReason before this predicate ever runs.
function gkMatches(matchExpr, target) {
  const kinds = { "@definedOr": [`${matchExpr}.kinds`, []] };
  const namespaces = { "@definedOr": [`${matchExpr}.namespaces`, []] };
  const excluded = { "@definedOr": [`${matchExpr}.excludedNamespaces`, []] };
  const inOrStar = (valueExpr, listExpr) => ({
    "@or": [
      { "@in": ["*", listExpr] },
      { "@in": [valueExpr, listExpr] },
    ],
  });
  return {
    "@and": [
      {
        "@or": [
          { "@eq": [{ "@len": [kinds] }, 0] },
          {
            "@any": [
              {
                "@and": [
                  inOrStar(target.group, { "@definedOr": ["$$.apiGroups", ["*"]] }),
                  inOrStar(target.kind, { "@definedOr": ["$$.kinds", ["*"]] }),
                ],
              },
              kinds,
            ],
          },
        ],
      },
      {
        "@or": [
          { "@eq": [{ "@len": [namespaces] }, 0] },
          { "@any": [nsGlob(target.namespace), namespaces] },
        ],
      },
      { "@not": { "@any": [nsGlob(target.namespace), excluded] } },
    ],
  };
}

// matchReason is the reject-over-wrong gate: a constraint using a match
// field the auditor does not implement (scope, name, namespaceSelector,
// ...) is invalidated with a reason naming the fields instead of silently
// mis-matching; null when the match block is fully supported.
const SUPPORTED_MATCH_FIELDS = ["kinds", "namespaces", "excludedNamespaces", "labelSelector"];

function matchReason(matchExpr) {
  const unsupported = {
    "@filter": [
      { "@not": { "@in": ["$$.", SUPPORTED_MATCH_FIELDS] } },
      { "@keys": matchExpr },
    ],
  };
  return {
    "@cond": [
      { "@eq": [{ "@len": [unsupported] }, 0] },
      null,
      { "@concat": ["unsupported match fields: ", { "@join": [unsupported, ", "] }] },
    ],
  };
}

// capViolations is the status violation list: canonically ordered (by
// content hash, the same order @groupBy emits) and capped at the limit,
// so the status document is a deterministic function of the violation
// set.
function capViolations(valuesExpr, limit) {
  return { "@slice": [{ "@sortByKey": [{ "@hash": "$$." }, valuesExpr] }, limit] };
}

module.exports = {
  gkMatches,
  matchReason,
  capViolations,
};
