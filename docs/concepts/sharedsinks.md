??? "API Documentation"
    [`laktory.models.DataSinkSharedOptions`][laktory.models.DataSinkSharedOptions]<br>

A shared sink is a table written by several writers: nodes of the same pipeline and/or other
pipelines. For example, several feeds pooled into one table, nodes sending their quarantined rows
to the same table, or one pipeline per client appending to a cross-tenant table.

Each writer **owns its rows**: a [full refresh](refresh.md) of a writer only deletes and
reprocesses its own rows. Writers are therefore independent: they run in their own task, in any
order, in parallel, alone or together.

## Declaring a Shared Sink

Declare `shared` on every sink writing to the table (`shared: true` for the default options).

**Several nodes of a pipeline:**

```yaml
nodes:
- name: feed_a
  sinks:
  - table_name: prices
    mode: APPEND
    shared: true
- name: feed_b
  sinks:
  - table_name: prices
    mode: APPEND
    shared: true
```

Nodes of a pipeline writing to the same table without `shared` fail validation: declaring it
makes the writer column added to the table explicit.

**Several pipelines:**

```yaml
# pl-client-acme.yaml, pl-client-globex.yaml, ...
sinks:
- table_name: all_orders
  mode: APPEND
  shared: true
```

Shared sinks must be DELTA table or file sinks in `APPEND` mode: other modes (`OVERWRITE`,
`MERGE`) would modify the rows of the other writers. Write to separate tables instead.

## Identifying the Rows of a Writer

**Writer column (default):** each row carries its writer in a `_laktory_writer` column (first
column of the table).

| `_laktory_writer` | feed |
|---|---|
| `pl-prices.feed_a` | a |
| `pl-prices.feed_b` | b |

| Option | Default | Description |
|---|---|---|
| `writer_id` | `{pipeline_name}.{node_name}` | writer identifier |
| `column` | `_laktory_writer` | writer column name |

**Predicate:** to avoid adding a column, declare the rows owned by the writer with `where`. A full
refresh deletes the rows matching it.

```yaml
# pl-client-23.yaml
sinks:
- table_name: all_orders
  mode: APPEND
  shared:
    where: client_id = 23    # full refresh: DELETE FROM all_orders WHERE client_id = 23
```

- A predicate also works for a single writer, when other processes (backfills, manual loads)
  write to the same table.
- All the writers of a table must use the same kind: writer column or `where`.
- Predicates must not overlap, and a writer must only write rows matching its predicate: other
  rows are not deleted by its full refresh.
- The number of deleted rows is logged: check it to catch a wrong predicate.

## Reading a Shared Sink

| Source | Reads |
|---|---|
| `node_name: feed_a` | the output of `feed_a` only: its rows, without the writer column |
| `table_name: prices` | the whole table: the rows of all the writers |

`node_name` returns the same data whether the node output is read from memory (same run) or
from the table (e.g. a separate job task).

- With declarative orchestrators, rows carry no writer: `node_name` reads the whole table. Use
  separate tables when a node needs the output of a single writer.
- A full refresh of a writer deletes rows from the table, which fails streaming reads of the
  table (as for any Delta table): run a full refresh of these readers too.

## Resetting a Shared Table

| Goal | Run |
|---|---|
| Reprocess one writer | `refresh=FULL` on its task (or node): deletes its rows, `reset_mode` doesn't apply |
| Drop or empty the whole table | `refresh=RESET` with `reset_mode=DROP` or `TRUNCATE`, on any of its writers' tasks, then a normal run |

When the whole table is reset:

- The checkpoints of all its writers in the pipeline are reset too, whether or not they're part
  of the run: they reprocess all their data on their next run.
- Other pipelines writing to the table need a full refresh: resuming from their checkpoints,
  they would skip the data they already wrote, and the table would silently miss their rows.
- `refresh=FULL` with the override is rejected: a full refresh never deletes the rows of the
  other writers. Reset the table with `refresh=RESET`, then run normally.

## Keeping Ownership Consistent

| Situation | What to do |
|---|---|
| Renaming a writer (node or pipeline) | pin `writer_id` first: rows of the old identifier are no longer deleted by a full refresh |
| Removing a writer | delete its rows (`DELETE FROM ... WHERE _laktory_writer = '...'`) or reset the whole table |
| Table written before being shared (no writer column) | writes fail until the table is dropped once (`refresh=RESET`, `reset_mode=DROP`) |
| Switching between writer column and `where` | drop the table once |
| Parallel writers adding different columns | declare the full `schema`, or order the writers with `depends_on` |

## Validation

- Every sink of a table written by several nodes of a pipeline declares `shared`.
- Shared sinks are DELTA sinks in `APPEND` mode.
- Writers of a table use the same kind of ownership, the same `column`, and distinct writer
  identifiers or predicates.

Pipelines writing to the same table are validated together when they are deployed together, in a
Stack or a Databricks Asset Bundle:

| Situation | Result |
|---|---|
| A declarative pipeline writes to the table | error: the engine owns the table |
| Sinks declaring `shared` identify their rows inconsistently (kind, `column`, identifiers) | error |
| Some sinks don't declare `shared` | warning: their full refresh or overwrite deletes the rows of the other writers |

Sharing a table across pipelines is otherwise the responsibility of the user: pipelines deployed
separately (other stacks, bundles or repos) can't be validated together, and a table is
identified by its name or path as written in each pipeline.

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipelines, the engine owns the tables: `shared` is not
supported and a table written by a declarative pipeline can't be shared with other pipelines.

Several nodes can still write to the same table, without declaring `shared`: it's declared once as
a streaming table, and each node appends to it through its own append flow,
`{table_name}__{node_name}`. On a full refresh, the engine clears the table once and resets every
flow.

- All sinks must be streaming, non-CDC (`MERGE`) table sinks.
- Table properties (`comment`, `table_properties`, `format`) can be set on any of the sinks, but
  must not conflict.
- With Lakeflow Declarative Pipelines, expectations apply to the whole table: all its writers must
  declare the same expectations.
- A flow is identified by its name: adding a second writer to a table, or renaming a node,
  restarts the existing flow from scratch. Run a full refresh of the table once after the change.
