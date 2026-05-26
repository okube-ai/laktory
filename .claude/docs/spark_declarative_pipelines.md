# Spark Declarative Pipelines (SDP)

Research notes from prototype exploration (May 2026). Reference for F6 integration.

---

## Overview

Spark Declarative Pipelines (`pyspark.pipelines`) is Apache Spark's open-source declarative pipeline framework, introduced in Spark 4.0 (available from PySpark 4.1.1+). It is the open-source counterpart of Databricks' Lakeflow Declarative Pipelines (formerly Delta Live Tables). SDP enables the same declarative pipeline patterns locally without a Databricks cluster.

---

## Installation & CLI

```bash
pip install pyspark>=4.1.1
spark-pipelines run --spec path/to/spark-pipeline.yml
spark-pipelines dry-run --spec path/to/spark-pipeline.yml
spark-pipelines init --name my_pipeline
```

The `spark-pipelines` CLI is the only execution entry point — there is no programmatic Python runner API.

---

## Python API

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()  # must be called explicitly; not injected
```

### Dataset decorators

| Decorator | Output type | Typical read pattern |
|-----------|------------|----------------------|
| `@dp.materialized_view` | `MaterializedView` | `spark.read.*` (batch) |
| `@dp.table` | `StreamingTable` | `spark.readStream.*` (streaming) |
| `@dp.temporary_view` | `TemporaryView` | either |

All decorators accept optional kwargs: `name`, `comment`, `spark_conf`, `table_properties`, `partition_cols`, `cluster_by`, `schema`, `format`.

### Example

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.getActiveSession()

@dp.materialized_view
def brz():
    return spark.read.parquet("path/to/source.parquet")

@dp.materialized_view
def slv():
    return spark.table("brz").withColumn("x2", col("x1") * 2)

# Streaming variant
@dp.table(name="slv_streaming")
def slv_streaming():
    return spark.readStream.table("brz").withColumn("x2", col("x1") * 2)
```

### Multiple flows to one target

```python
dp.create_streaming_table("merged")

@dp.append_flow(target="merged")
def flow_west():
    return spark.readStream.table("source_west")

@dp.append_flow(target="merged")
def flow_east():
    return spark.readStream.table("source_east")
```

---

## Pipeline spec (YAML)

```yaml
name: my-pipeline
libraries:
  - glob:
      include: pipeline_definitions.py   # relative to spec file directory
storage: file:///tmp/my-pipeline-checkpoints  # must be an absolute URI with scheme
catalog: my_catalog     # optional default catalog
database: my_schema     # optional default schema
configuration:
  spark.sql.shuffle.partitions: "8"
```

**Glob constraints:**
- Path is relative to the directory of the spec file (not the working directory)
- Only file paths or folder paths ending with `/**` are allowed — `*.py` is not valid
- Absolute paths are not supported in globs

**Storage:** must include a URI scheme: `file://`, `s3a://`, `hdfs://`, etc.

---

## How dependency tracking works

SDP does NOT require explicit dependency declarations. Dependencies are inferred server-side from Spark logical plans:

1. Each decorated function is called during registration inside two context managers:
   - `add_pipeline_analysis_context`: attaches a `PipelineAnalysisContext` protobuf extension to the Spark Connect session so the JVM server knows it's analyzing a pipeline flow
   - `block_spark_connect_execution_and_analysis`: intercepts `AnalyzePlan` and `ExecutePlan` RPCs — **no data is read or analyzed** at registration time
2. The function runs and produces a lazy `ConnectDataFrame` — a client-side logical plan with `UnresolvedRelation` nodes
3. SDP serializes that plan as a protobuf `Relation` and sends it to the Spark Connect JVM server via `DefineFlow`
4. The server resolves all `UnresolvedRelation` nodes against the known pipeline datasets, inferring the dependency graph

**Consequence:** `spark.table("brz")` inside `slv()` creates an `UnresolvedRelation ["brz"]` in the plan → server sees `slv` depends on `brz` → `brz` runs first.

**Laktory implication:** `PipelineNodeDataSource._read_spark()` does NOT need special SDP/LDP handling. The normal reader path (`primary_sink.read()` → `spark.table(full_name)`) creates the right `UnresolvedRelation` during the registration phase. The old `is_ldp` branch was removed (2026-05-21).

---

## Inter-dataset read patterns

These are the SDP equivalents of DLT's `dlt.read()` / `dlt.read_stream()`:

| DLT | SDP | Notes |
|-----|-----|-------|
| `dlt.read("brz")` | `spark.table("brz")` | Batch read from pipeline dataset |
| `dlt.read_stream("brz")` | `spark.readStream.table("brz")` | Streaming read from pipeline dataset |

SDP has **no special read functions** — standard Spark DataFrame APIs are used. The dependency is inferred from the reference in the logical plan.

---

## External data sources

SDP's Spark Connect server spins up its own isolated catalog. Tables created in other Spark sessions are not visible.

| Read pattern | Resolves? | Dependency edge? | Notes |
|---|---|---|---|
| `spark.table("pipeline_dataset")` | ✓ | Yes | In-pipeline datasets only |
| `spark.table("external_table")` | ✗ locally | No | Fails `TABLE_OR_VIEW_NOT_FOUND` unless in a shared catalog (e.g. Unity Catalog on Databricks) |
| `spark.read.parquet(path)` | ✓ | No | Correct pattern for file sources |
| `spark.read.format(...).load(path)` | ✓ | No | Same — any Spark format works |
| `spark.sql("CREATE TABLE ...")` | ✗ | — | Blocked: `UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND` |

**Rule:** external data must be read via `spark.read.*`, not `spark.table()`. On Databricks, Unity Catalog tables (`catalog.schema.table`) resolve fine as external sources.

---

## Mapping to Laktory source types

| Laktory source | SDP read pattern | Creates SDP dependency? |
|---|---|---|
| `PipelineNodeDataSource` (batch) | `spark.table("sink_name")` | Yes |
| `PipelineNodeDataSource` (streaming) | `spark.readStream.table("sink_name")` | Yes |
| `FileDataSource` | `spark.read.format(...).load(path)` | No — leaf source |
| `UnityCatalogDataSource` | `spark.table("cat.schema.table")` | No — external catalog |
| `HiveMetastoreDataSource` | `spark.table("schema.table")` | No — external catalog |

This maps cleanly onto Laktory's existing source architecture. Only `PipelineNodeDataSource` needs special SDP handling; all other sources use their existing `_read_spark()` methods unchanged.

---

## Output location (local)

Tables are written to `spark-warehouse/<table_name>/` as Parquet (default) or Delta if configured. The `storage` path stores streaming checkpoints only — it is not the table data location.

---

## Constraints and restrictions

- **No programmatic runner:** must use `spark-pipelines run` CLI; there is no Python API to execute a pipeline
- **No DAG visualization:** the dependency graph is maintained server-side (JVM); no Python-accessible graph object
- **No DDL in pipeline code:** `spark.sql("CREATE TABLE ...")` and other DDL is blocked at module level
- **Isolated catalog:** external tables from other sessions are not visible; use `spark.read.*` for external data
- **`spark` must be obtained explicitly:** `SparkSession.getActiveSession()` — it is not injected into the module namespace
- **Glob paths are relative to spec file directory**, not the working directory where the CLI is invoked
- **CDC / `create_auto_cdc_flow`:** not available in open-source SDP (Databricks-only via LDP)
- **`collect()`, `count()`, `toPandas()`** are forbidden inside dataset functions (would trigger blocked RPCs)

---

## F6 integration implications

### Dual-mode design

`SPARK_DECLARATIVE_PIPELINE` is a **single orchestrator type that serves two execution contexts**:

| Mode | Execution | Infrastructure |
|------|-----------|----------------|
| **Local** | `spark-pipelines run --spec …` via subprocess | Local PySpark 4.1+ install |
| **Lakeflow Job** | Same Python script as a Databricks Job task | DBR 16.x cluster; **no DLT license required** |

The generated artifacts are identical in both modes — the orchestrator does not branch on mode at artifact-generation time. The difference is only at execution time.

> **Open question:** how the mode is selected (e.g. a field on the orchestrator, deploy-time flag, or inferred from context) is TBD pending testing of the Databricks Job path.

1. **New orchestrator type** `SPARK_DECLARATIVE_PIPELINE` (class `SparkDeclarativePipelineOrchestrator`) generates three artifacts per pipeline:
   - `sdp_laktory_pl.py` — Python definition script using `@dp.materialized_view` / `@dp.table`
   - `{pipeline_name}.json` — serialized pipeline config (reuses `PipelineConfigWorkspaceFile`)
   - `{pipeline_name}-spec.yml` — SDP YAML spec pointing to the script and config

2. **Local execution** — `pl.execute()` shells out via `subprocess.run(["spark-pipelines", "run", "--spec", spec_path])`. No programmatic Python runner exists; subprocess is the only viable approach.

3. **Lakeflow Job execution** — the same `sdp_laktory_pl.py` script is submitted as a task in a Databricks Job on a DBR 16.x cluster (PySpark 4.1+). No DLT/Lakeflow Declarative Pipelines license required — this is plain Spark.

4. **Narwhals fit:** unchanged — `node.execute()` runs inside the decorated function body; `node.output_df.to_native()` returns a PySpark DataFrame to SDP. Same boundary as Lakeflow today.

5. **SDP execution detection** — `is_sdp_execute()` checks:
   - `spark.conf.get("spark.pipelines.flow.name")` — set by SDP runtime
   - `spark.conf.get("laktory.is_sdp_execute")` — set via the generated YAML `configuration` block as fallback

6. **Source handling:** no special SDP logic needed in `PipelineNodeDataSource._read_spark()` — the normal reader path (`primary_sink.read()`) produces the `spark.table()` call that SDP uses for dependency tracking.

---

## 0.12 naming decisions (2026-05-20)

Part of a major 0.12 release with intentional breaking changes. Orchestrator type strings and class names are aligned with current Databricks product names.

### Orchestrator type strings

| 0.11 | 0.12 | Acronym | Class |
|------|------|---------|-------|
| `DATABRICKS_PIPELINE` | `LAKEFLOW_DECLARATIVE_PIPELINE` | LDP | `LakeflowDeclarativePipelineOrchestrator` |
| `DATABRICKS_JOB` | `LAKEFLOW_JOB` | LJ | `LakeflowJobOrchestrator` |
| *(new)* | `SPARK_DECLARATIVE_PIPELINE` | SDP | `SparkDeclarativePipelineOrchestrator` |

**Rationale:** LDP/SDP pair cleanly — Lakeflow-hosted vs open-source Spark. "Lakeflow" encodes "Databricks + Spark + production." Including "SPARK" in the Databricks variant (`LAKEFLOW_SPARK_DECLARATIVE_PIPELINE`) would make the type string too long and produce an awkward LSDP acronym.

### Migration (hard break)

0.11 YAML files using `DATABRICKS_PIPELINE` or `DATABRICKS_JOB` raise a `ValueError` immediately on parse with a message pointing to the new name. No deprecation period. Same for `dlt_template` → `ldp_template`.

### Internal property renames

| 0.11 | 0.12 |
|------|------|
| `pipeline.is_orchestrator_dlt` | `pipeline.is_orchestrator_ldp` |
| `node.is_orchestrator_dlt` | `node.is_orchestrator_ldp` |
| `node.is_dlt_execute` | `node.is_ldp_execute` |
| `is_dlt_execute()` in `laktory/__init__` | `is_ldp_execute()` |
| *(new)* | `pipeline.is_orchestrator_sdp` |
| *(new)* | `node.is_orchestrator_sdp` |
| *(new)* | `node.is_sdp_execute` |
| *(new)* | `is_sdp_execute()` in `laktory/__init__` |

### `import dlt` → `from pyspark import pipelines as dp`

All direct `dlt.*` calls have been replaced with `dp.*` equivalents in `ldp_laktory_pl.py`. The `dlt` module is no longer imported anywhere in Laktory source. `dp.expect_all`, `dp.create_auto_cdc_flow`, etc. are Databricks-only extensions to `pyspark.pipelines` — not in open-source PySpark, but available on Databricks DBR 16.x via the same import.

Internal property renames:

| Old (`dlt_*`) | New | Scope |
|---------------|-----|-------|
| `dlt_table_or_view_name` | `sdp_table_or_view_name` | SDP + LDP |
| `dlt_table_or_view_kwargs` | `sdp_table_or_view_kwargs` | SDP + LDP |
| `dlt_warning_expectations` | `ldp_warning_expectations` | LDP only |
| `dlt_drop_expectations` | `ldp_drop_expectations` | LDP only |
| `dlt_fail_expectations` | `ldp_fail_expectations` | LDP only |
| `dlt_pre_merge_view_name` | `ldp_pre_merge_view_name` | LDP only |
| `dlt_apply_changes_kwargs` | `ldp_auto_cdc_flow_kwargs` | LDP only |
| `dlt_template` (YAML field) | `ldp_template` | LDP only |
| `is_dlt_compatible` | `is_ldp_compatible` | LDP only |
| `is_dlt_managed` | `is_ldp_managed` | LDP only |

SDP-specific properties added (parallel to the `ldp_*` ones, filter by `is_sdp_compatible`):

| Property | Location | Notes |
|----------|----------|-------|
| `sdp_warning_expectations` | `PipelineNode`, `TableDataSink`, `PipelineViewDataSink` | Empty until SDP adds `@dp.expect_*` |
| `sdp_drop_expectations` | same | same |
| `sdp_fail_expectations` | same | same |

Template notebook renamed: `dlt_laktory_pl.py` → `ldp_laktory_pl.py`.

### `apply_changes` → `create_auto_cdc_flow` ✓ Done (2026-05-26)

Databricks confirmed (docs) that `dp.apply_changes()` is replaced by `dp.create_auto_cdc_flow()` with **identical signature** — no parameter changes. The earlier note about `dp.auto_merge()` was incorrect.

Changes applied:
- `dp.apply_changes(...)` → `dp.create_auto_cdc_flow(...)` in both `laktory_ldp.py` scripts
- `ldp_apply_changes_kwargs` property on `BaseDataSink` renamed to `ldp_auto_cdc_flow_kwargs`
- `dp.create_streaming_table()` confirmed still current — required before `create_auto_cdc_flow`

---

## Implementation status (as of 2026-05-26)

### Completed ✓

**`SparkDeclarativePipelineOrchestrator`** — `laktory/models/pipeline/orchestrators/sparkdeclarativepipelineorchestrator.py`
- Generates three artifacts: `laktory_sdp.py`, `{pipeline_name}.json`, `spark-pipeline.yaml`
- `build()` and `execute()` (subprocess `spark-pipelines run`) both wired; `full_refresh` / `selects` flags passed through
- `read_output=True` reads node outputs from `spark-warehouse/` after execution
- `is_orchestrator_sdp` on `Pipeline` and `PipelineNode` fully wired
- `is_sdp_execute()` in `laktory/__init__` checks Spark conf flags
- Wired into orchestrator union type and migration validator in `pipeline.py`
- 5 tests in `tests/pipeline/orchestrators/test_orchestrator_sdp.py` (all passing)

**Expectations scaffolding**
- `is_sdp_compatible` → `False` (single place to flip when SDP ships `@dp.expect_*`)
- `is_sdp_managed` — independent implementation: checks `is_sdp_compatible` + `is_orchestrator_sdp` + `is_sdp_execute()`
- `check_expectations()` raises `TypeError` for non-SDP-compatible expectations when `is_sdp_execute` — mirrors LDP streaming behavior; test added
- `laktory_sdp.py` has a comment marking where `@dp.expect_*` decorators should be added

**CDC rename** — `dp.apply_changes` → `dp.create_auto_cdc_flow` (identical signature, confirmed via Databricks docs)

**Bug fixes**
- `update_from_parent()` in `SparkDeclarativePipelineOrchestrator`: removed stale `or self.target` reference (LDP-only field)
- `schema_name` propagation from `orchestrator.schema_` to sinks verified and tested

### Remaining open item

**Lakeflow Job dual-mode path** — how the SDP orchestrator selects between local (`spark-pipelines run`) and Databricks Job execution (DBR 16.x) is TBD. The generated artifacts are identical in both modes. Open question: orchestrator field, deploy-time flag, or context inference. Blocked on Databricks Job path testing.

resources/databricks/pipeline.py -> resource_type_id(). Rename output to `dlp`? I think there is another one that needs to be renamed as well.
