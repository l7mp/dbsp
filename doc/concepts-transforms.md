# Circuit transforms

A *transform* maps compiled circuit to a new circuit. Thus guide defines the high-level semantics
and catalogs the transforms.

## Execution model

Every transform clones the input circuit and never mutates it. In JavaScript, `sql.compile` /
`aggregate.compile` and every `.transform()` only build the circuit and validate the result; none
of them installs it. `.commit()` is the single install point, and a circuit starts running exactly
there. A failed transform leaves the handle on its previous circuit.

The `.transform()` call takes a set of `{ name, ...opts }` entries. The engine applies the
transforms in canonical order regardless of the order given, and the whole set applies as one step
(all transforms apply, or the previous circuit stays); the ordering is enforced in the
implementation.

The below chain reorders the transforms to SmithPredictor -> Incrementalizer.
```javascript
aggregate.compile(pipeline, { inputs, outputs })
  .transform([
    { name: "Incrementalizer" },
    { name: "SmithPredictor", pairs: [["observed", "out"]], k: 2 },
  ])
  .commit();
```

## Transforms

All transforms are snapshot-side constructions: they inject standard operators into the snapshot
circuit, and the `Incrementalizer` compiles the result mechanically into incremental form.

| **Transform** | **Mode** | **Description** |
|---|---|---|
| `Reconciler` | snapshot | Turns selected outputs into desired-state reconciled outputs: `U = ∫(δD − δY_U)` against an observed-feedback input. Configured with `pairs: [[observedInput, output], ...]`; with no pairs, `input_X`/`output_X` name pairs are auto-detected. |
| `SmithPredictor` | snapshot | The Reconciler with dead-time compensation: the feedback is compared against a prediction, the observation with the window of the last `k−1` emissions superimposed, so every emission is followed by at least `k−1` silent steps. Takes the same `pairs` as the Reconciler plus the compensation window `k` (at least 2; `k = 1` is the raw Reconciler). |
| `Distincter` | snapshot | Makes every output *set-valued*: per output it appends a `Distinct` DBSP operator so each document is emitted with a single multiplicity. A closed-loop output must be set-typed for the loop to quiesce. |
| `Incrementalizer` | incremental | Compiles the snapshot circuit into its incremental form `Q^Δ = D∘Q∘∫` (per-operator rules; linear operators pass through unchanged). |

The canonical order, owned by the engine and applied to any `.transform([...])` list is `Reconciler
| SmithPredictor -> Distincter -> Incrementalizer`.
