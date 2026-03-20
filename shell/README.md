# dbsp shell

`dbsp` is now an interactive runtime shell. A shell session owns one runtime for
its full lifetime.

## Build and run

```bash
go build .

# Interactive shell.
./dbsp

# Positional script execution.
./dbsp shell/examples/join_project.dbsp
```

There is no `dbsp shell` subcommand anymore.

## Concepts

- `zset` is storage plus endpoint wiring.
- `zset produce <zset-name> <input-name>` publishes the zset to topic `<input-name>`.
- `zset consume <zset-name> <output-name>` subscribes the zset to topic `<output-name>`.
- zset mutations (`set`, `insert`, `clear`, `negate`, `weight`) create snapshots:
  - buffered when not producing;
  - published immediately when producing.
- `zset print <name>` is destructive: it pops and prints the first buffered event.
- `circuit validate <name>` validates and, for SQL-saved circuits, installs runtime processing.
- Use `-l, --loglevel debug|info|warn|error` to control runtime logs.
- `-v` is shorthand for `-l debug`.

## Quick workflow: SQL JOIN through runtime

Paste the following commands into the interactive shell:

```text
sql table create TABLE products (pid INT PRIMARY KEY, name TEXT, price FLOAT)
sql table create TABLE orders (oid INT PRIMARY KEY, product_id INT, qty INT)

sql create --output joined-orders join_stmt SELECT oid, product_id, pid, name, price, qty FROM orders JOIN products ON product_id = pid
sql compile join_stmt join_q
circuit incrementalize join_q join_q_inc
circuit validate join_q_inc

zset create products
zset set products '[({"pid":1,"name":"Widget","price":9.99},1),({"pid":2,"name":"Gadget","price":24.99},1)]'

zset create orders
zset set orders '[({"oid":101,"product_id":1,"qty":3},1),({"oid":102,"product_id":2,"qty":1},1)]'

zset create output
zset consume output joined-orders

zset produce products products
zset produce orders orders

zset print output
```

Tip: the first output event may be empty depending on input event order; call
`zset print output` again to pop the next buffered result.

## Command groups

- `sql`: table DDL/DML and SQL compilation.
- `circuit`: inspect, edit, validate, incrementalize circuits.
- `zset`: manage zsets and runtime produce/consume wiring.

## Quick reference

### sql

```text
sql create <name> <statement>
sql create --output <output-name> <name> <statement>
sql compile <name> <circuit-name>
sql get <name>
sql list
sql delete <name>
sql table create TABLE <name> (<col> <type> [PRIMARY KEY], ...)
sql insert INTO <name> VALUES (...)
sql select [--save <circuit>] <cols> FROM <table> [JOIN <table> ON <cond>]
sql eval [--save <circuit>] [--save-zset <name>] [--incr] <full SQL query>
sql table drop TABLE <name>
sql tables
sql schema <name>
```

### aggregate

```text
aggregate create [--input <input-name>] [--output <output-name>] <name> <pipeline-json>
aggregate compile <name> <circuit-name>
aggregate get <name>
aggregate list
aggregate delete <name>
```

`--input` can also be a comma-separated list (for example `orders,products`).

Default output naming after compile:
- SQL: `<sql-statement-name>-output` unless overridden with `sql create --output`.
- Aggregate: `<aggregate-statement-name>-output` unless `aggregate create --output` is set.

### circuit

```text
circuit create <name>
circuit get <name>
circuit delete <name>
circuit validate <name>
circuit print <name> [nodes|edges|all]
circuit incrementalize <name> <new-name>
circuit node add|get|update|delete|set ...
circuit edge add|get|update|delete ...
circuit list
```

### zset

```text
zset create <name>
zset produce <zset-name> <input-name>
zset consume <zset-name> <output-name>
zset print <name>
zset set <name> (json,weight)|[(json,weight),...]
zset insert <name> (json,weight)|[(json,weight),...]
zset weight <name> (json,weight)
zset negate <name>
zset clear <name>
zset delete <name>
zset list
```
