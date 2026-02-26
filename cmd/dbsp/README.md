# dbsp — Command-line DBSP Shell

`dbsp` is a command-line tool for building, inspecting, and executing DBSP
circuits interactively. It exposes SQL table management, Z-set manipulation,
circuit construction, and an incremental executor through a unified shell.

## Building and Running

```bash
# Build
go build ./cmd/dbsp

# Interactive shell (readline, tab-completion)
./dbsp shell

# Basic line-by-line shell (no readline dependency)
./dbsp shell --no-readline

# Run a script file non-interactively
./dbsp run workflow.dbsp
```

## Command Groups

| Group | Description |
|-------|-------------|
| `sql` | Create tables, insert rows, compile and run SQL queries |
| `zset` | Create and populate named Z-sets (weighted multisets) |
| `circuit` | Inspect, validate, and incrementalize saved circuits |
| `executor` | Run circuits step-by-step; derive incremental twins |

All groups work both in the interactive shell and in `dbsp run` scripts. In
the shell, typing a group name (e.g. `sql`) enters the corresponding sub-menu;
`exit` or Ctrl-D returns to the parent menu.

---

## Sample Workflow: SQL JOIN with Incremental Execution

This workflow demonstrates the full pipeline:

1. Define two SQL tables.
2. Insert rows.
3. Compile and run a JOIN query (state-of-the-world, SotW).
4. Save the compiled circuit.
5. Evaluate SotW via the executor.
6. Derive an incremental twin.
7. Evaluate a single delta step.

Save the following as `workflow.dbsp` and run it with `./dbsp run workflow.dbsp`,
or paste each line into the interactive shell.

### Step 1 — Create the schema

Column names are chosen to be globally unique across both tables so that the
JOIN condition and SELECT list can use unqualified names; the same circuit then
works with both typed `relation.Row` inputs (produced by `sql select`) and flat
JSON Z-sets (fed via `executor execute`).

```
sql create TABLE products (pid INT PRIMARY KEY, name TEXT, price FLOAT)
sql create TABLE orders (oid INT PRIMARY KEY, product_id INT, qty INT)
```

Inspect the result:

```
sql tables
sql schema products
sql schema orders
```

### Step 2 — Insert rows

```
sql insert INTO products VALUES (1, 'Widget', 9.99)
sql insert INTO products VALUES (2, 'Gadget', 24.99)
sql insert INTO orders VALUES (101, 1, 3)
sql insert INTO orders VALUES (102, 2, 1)
```

### Step 3 — Compile and run the JOIN (SotW), save the circuit

The `--save` flag compiles the query into a DBSP circuit, executes it once
against the current table contents, prints the results, and stores the circuit
under the given name.

```
sql select --save join_q oid, product_id, pid, name, price, qty FROM orders JOIN products ON product_id = pid
```

Expected output (row order may differ):

```
[1]  {"name":"Widget","oid":101,"pid":1,"price":9.99,"product_id":1,"qty":3}  weight=+1
[2]  {"name":"Gadget","oid":102,"pid":2,"price":24.99,"product_id":2,"qty":1}  weight=+1
Circuit saved as join_q.
```

Inspect the compiled circuit:

```
circuit update join_q
circuit validate
circuit print nodes
```

### Step 4 — Build Z-sets that mirror the SQL tables

The executor accepts named Z-sets rather than live SQL tables. Create one Z-set
per input table and populate it with JSON documents whose field names match the
unqualified column names used in the SELECT list.

```
zset create products_z
zset insert {"pid":1,"name":"Widget","price":9.99}
zset insert {"pid":2,"name":"Gadget","price":24.99}

zset create orders_z
zset insert {"oid":101,"product_id":1,"qty":3}
zset insert {"oid":102,"product_id":2,"qty":1}
```

`zset create` sets the newly created Z-set as the current one, so subsequent
`zset insert` calls write into it without an explicit `zset update`.

### Step 5 — Evaluate SotW via the executor

```
executor create --circuit join_q sotw_exec
executor execute input_orders=orders_z input_products=products_z
zset get sotw_exec-output
```

`executor execute` maps each `<node>=<zset>` argument to the corresponding
input node of the circuit. The compiled `join_q` circuit exposes two input
nodes: `input_orders` and `input_products`. Outputs are stored as Z-sets named
`<executor>-<output-node>`.

Expected:

```
Output: sotw_exec-output  (+2 docs)
[1]  {"name":"Widget","oid":101,"pid":1,"price":9.99,"product_id":1,"qty":3}  weight=+1
[2]  {"name":"Gadget","oid":102,"pid":2,"price":24.99,"product_id":2,"qty":1}  weight=+1
```

### Step 6 — Derive an incremental twin

`executor incrementalize` applies Algorithm 6.4 to the circuit backing the
current executor, producing a new circuit and executor that operate on deltas
(Z-set differences) rather than full snapshots.

```
executor incrementalize inc_exec
executor update inc_exec
```

Seed the incremental executor with the full SotW as its first delta step:

```
executor execute input_orders=orders_z input_products=products_z
zset get inc_exec-output
```

The output matches the SotW exactly (the first delta step bootstraps the
integrated state):

```
Output: inc_exec-output  (+2 docs)
[1]  {"name":"Widget","oid":101,"pid":1,"price":9.99,"product_id":1,"qty":3}  weight=+1
[2]  {"name":"Gadget","oid":102,"pid":2,"price":24.99,"product_id":2,"qty":1}  weight=+1
```

### Step 7 — Evaluate a delta

A new order arrives. The products catalogue does not change, so its delta is
an empty Z-set.

```
zset create delta_orders
zset insert {"oid":103,"product_id":1,"qty":7}

zset create empty_z

executor execute input_orders=delta_orders input_products=empty_z
zset get inc_exec-output
```

Expected:

```
Output: inc_exec-output  (+1 docs)
[1]  {"name":"Widget","oid":103,"pid":1,"price":9.99,"product_id":1,"qty":7}  weight=+1
```

The incremental circuit computes exactly the new join tuple without
reprocessing any existing data.

---

## Quick Reference

### SQL commands

```
sql create TABLE <name> (<col> <type> [PRIMARY KEY], ...)
sql insert INTO <name> VALUES (...)
sql select [--save <circuit>] <cols> FROM <table> [JOIN <table> ON <cond>]
sql eval [--save <circuit>] [--save-zset <name>] [--incr] <full SQL query>
sql drop TABLE <name>
sql tables
sql schema <name>
```

`sql select` prepends `SELECT` automatically; `sql eval` takes the complete
statement including the `SELECT` keyword.

`sql eval --incr` compiles the incremental version of the circuit (Algorithm 6.4)
and runs one step against the current table contents — useful for one-shot
verification without manually creating an executor.

`sql eval --save-zset <name>` stores the output Z-set in the registry so it can
be inspected with `zset get <name>` or fed to a subsequent `executor execute`.

### Z-set commands

```
zset create <name>          # create and select
zset update <name>          # select existing
zset insert <json>          # insert into selected Z-set (weight defaults to +1)
zset insert --weight -1 <json>  # insert with custom weight
zset get [<name>]           # print entries
zset negate                 # flip all weights
zset clear                  # remove all entries
zset list
```

### Circuit commands

```
circuit create <name>
circuit update <name>
circuit validate
circuit print [nodes|edges|all]
circuit incrementalize <new-name>
circuit list
```

### Executor commands

```
executor create --circuit <circuit> <name>
executor update <name>
executor execute [<node>=<zset> ...]
executor incrementalize <new-name>
executor reset
executor list
```
