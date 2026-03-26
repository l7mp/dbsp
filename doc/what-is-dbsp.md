# What is DBSP?

DBSP (Database Stream Processing) is a theory that solves one specific problem: given a computation
over some data, and a small change to that data, how do you compute the corresponding small change
to the result without re-running the whole thing?

The theory was introduced by Budiu, Chajed, McSherry, Ryzhyk, and Tannen and published at VLDB 2023
[1]. Feldera, Inc. maintains the reference implementation in Rust [2] and a SQL-to-DBSP
compiler. This Go module (`l7mp/DBSP`) is an independent, simplified implementation of the same
theory.

## The expensive default

Most data systems recompute from scratch. Consider a SQL view that joins
two tables, say, an `orders` table and a `customers` table, to produce a
combined report:

```sql
CREATE VIEW order_report AS
  SELECT o.id, o.item, c.name, c.region
  FROM orders o
  JOIN customers c ON o.customer_id = c.id;
```

Every time you query this view, the database joins the entire `orders`
table against the entire `customers` table. If there are a million orders
and a hundred thousand customers, the engine touches all of them, even if
the only thing that changed since the last query was a single new order.

This is what we call the **snapshot model**: treat the full current state as
input, run the full computation, get the full output. It is simple,
correct, and expensive. Cost scales with the total data size, not with how
much changed.

The same pattern appears outside databases. A Kubernetes controller that
lists all pods, filters, and reconciles on every event is doing snapshot
recomputation. An ETL pipeline that re-processes the whole dataset every
hour is doing snapshot recomputation. Any system that answers "something
changed" by re-reading everything is paying the snapshot tax.

## The incremental alternative

The incremental approach works differently. Instead of re-running the join
over a million orders, it asks: what is the one new order, and what does
that one new order contribute to the result?

Going back to the `order_report` example: suppose a new order arrives for
customer 42. The incremental version of the join does not touch the
`orders` table at all. It takes the single new order row, looks up customer
42 in its maintained copy of the `customers` table, and emits a single new
row in the result. If the `customers` table did not change, the work is
proportional to the number of new orders, not the total number of orders.

This sounds straightforward for a single join on a primary key, and it is.  But real computations
are rarely a single join. They are chains of filters, projections, joins, and aggregations,
sometimes with recursion. Writing correct incremental logic by hand for a multi-step pipeline is
notoriously difficult. Deletions are easy to forget. Updates to one side of a join require careful
bookkeeping of the other side's accumulated state. A bug in any stage compounds
downstream. Engineers who try this by hand tend to discover subtle correctness issues weeks into
production.

## What DBSP does

DBSP removes the manual work. You describe your computation in the snapshot style, a pipeline of
standard relational operators like filter, project, and join, and DBSP mechanically derives an
incremental version that processes only changes. The incremental version is guaranteed to produce
the same accumulated result as re-running the snapshot computation at every step.

The key insight is that DBSP models data as weighted multisets (called Z-sets) where every row
carries an integer weight: +1 means "this row exists," −1 means "this row was removed," and an
update is simply a removal of the old row plus an insertion of the new one. Because these weights
support addition and subtraction, DBSP can reason algebraically about how changes propagate through
a pipeline. For some operators (like filter and project) the change just flows straight
through. For others (like join) the change interacts with accumulated state in a way that DBSP can
derive automatically.

The end result is a computation that takes small input changes and produces small output changes,
with correctness guaranteed by construction rather than by hand-written tests.

## Where DBSP fits

DBSP is not a database and it is not a query engine. It is a framework for
turning snapshot computations into incremental ones. Think of it as a
compiler pass: it takes a circuit of operators and rewrites it into a new
circuit that does the same thing, but on changes instead of full state.

This library implements that framework in Go. You build a circuit of
operators (filter, project, join, distinct, group), call
`transform.Incrementalize`, and get back a new circuit that consumes and
produces deltas. The theory guarantees that the incremental circuit agrees
with the original snapshot circuit at every step.

## References

- [1] M. Budiu, T. Chajed, F. McSherry, L. Ryzhyk, V. Tannen.
  *DBSP: Automatic Incremental View Maintenance for Rich Query Languages.*
  PVLDB 16(7): 1601-1614, 2023.
  ([paper](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf))
- [2] Feldera, Inc. *The Feldera Incremental Computation Engine.*
  ([github.com/feldera/feldera](https://github.com/feldera/feldera))
- [3] M. Budiu, F. McSherry, L. Ryzhyk, V. Tannen.
  *DBSP specification.* Extended technical report, December 2022.
  ([spec.pdf](https://github.com/feldera/feldera/blob/main/papers/spec.pdf))
