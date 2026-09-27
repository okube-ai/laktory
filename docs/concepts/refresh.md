A pipeline run either processes new data on top of what its sinks already contain, or first
resets its sinks - data and checkpoints - to reprocess everything. `refresh` selects what a run
does, `reset_mode` how sinks are reset.

## Run Modes

| `refresh` | What the run does |
|---|---|
| `incremental` (default) | writes the sinks according to their `mode` (`APPEND` adds rows, `OVERWRITE` replaces them); streaming sources resume from their checkpoint |
| `full` | resets the sinks of the selected nodes, then runs: all the data is reprocessed |
| `reset` | only resets the sinks of the selected nodes, without reading or writing data |

Set it wherever a pipeline runs:

| Where | How |
|---|---|
| Python | `pl.execute(refresh="full")` |
| `LAKEFLOW_JOB` | `refresh` job parameter (*Run now with different parameters*) |
| `AIRFLOW` | `refresh` DAG param |

`refresh="full"` replaces `full_refresh=True` (Python, job parameter and DAG param).

## Reset Modes

| `reset_mode` | Effect |
|---|---|
| `DROP` (default) | drops the table; it's recreated on the next write |
| `TRUNCATE` | deletes all rows, keeping the table, its schema, location and grants |

```yaml
name: pl-stocks
reset_mode: TRUNCATE        # pipeline default
nodes:
- name: slv_prices
  sinks:
  - table_name: slv_prices
    reset_mode: DROP        # this sink only
```

- A sink uses the first value set on the sink, its node, its pipeline, or the settings
  (`settings.reset_mode` / `LAKTORY_RESET_MODE`, see
  [Laktory Settings](laktorysettings.md#reset-mode)).
- `TRUNCATE` is only supported by table sinks: file sinks are always dropped.
- To reset only part of a table, declare the rows the sink owns with `shared.where` (see
  [Shared Sinks](sharedsinks.md#identifying-the-rows-of-a-writer)).

## Overriding the Reset Mode

Override `reset_mode` for a single run, e.g. to drop a table configured with `TRUNCATE` after a
schema change:

| Where | How |
|---|---|
| Python | `pl.execute(refresh="full", reset_mode="DROP")` |
| `LAKEFLOW_JOB` | `reset_mode` job parameter |
| `AIRFLOW` | `reset_mode` DAG param |

The override requires `refresh` `full` or `reset`.

## Resetting a Table

To reset a table before a breaking schema change or after a data corruption - e.g. by someone who
can run the pipeline but not drop tables - run with `refresh=reset`, then normally:

- Lakeflow Job: *Run now with different parameters* with `refresh=reset` and `reset_mode=DROP`
  (optionally on a selection of tasks), then *Run now*.
- Python: `pl.execute(refresh="reset", reset_mode="DROP")`, then `pl.execute()`.

## Shared Sinks

A [shared sink](sharedsinks.md) is written by several nodes or pipelines, each owning its rows. A
full refresh of a writer only deletes its own rows, and `reset_mode` doesn't apply - unless it's
overridden:

| Run | Regular sink | Shared sink |
|---|---|---|
| `full` | reset (`reset_mode`), reprocess | delete the writer's rows, reprocess |
| `reset` | reset (`reset_mode`) | delete the writer's rows |
| `reset` + override | reset (override) | reset the whole table (override), from any writer's task |
| `full` + override | reset (override), reprocess | rejected if several nodes of the pipeline write to the table |

See [Resetting a Shared Table](sharedsinks.md#resetting-a-shared-table).

## Declarative Orchestrators

With Lakeflow / Spark Declarative Pipelines, the engine performs the full refresh itself: it
clears the tables and resets their flows. `reset_mode` must stay `DROP` and `refresh="reset"` is
not supported.
