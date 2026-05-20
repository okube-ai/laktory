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
- **CDC / `apply_changes`:** not yet available in open-source SDP (Databricks-only via DLT)
- **`collect()`, `count()`, `toPandas()`** are forbidden inside dataset functions (would trigger blocked RPCs)

---

## F6 integration implications

1. **New orchestrator type** `SPARK_DECLARATIVE_PIPELINE` (class `SparkDeclarativePipelineOrchestrator`) generates three artifacts per pipeline:
   - `sdp_laktory_pl.py` — Python definition script using `@dp.materialized_view` / `@dp.table`
   - `{pipeline_name}.json` — serialized pipeline config (reuses `PipelineConfigWorkspaceFile`)
   - `{pipeline_name}-spec.yml` — SDP YAML spec pointing to the script and config

2. **Local execution** — `pl.execute()` shells out via `subprocess.run(["spark-pipelines", "run", "--spec", spec_path])`. No programmatic Python runner exists; subprocess is the only viable approach.

3. **Databricks deployment** — same Python script submitted as a Databricks Job task on a cluster with DBR 16.x (PySpark 4.1+). No DLT license required.

4. **Narwhals fit:** unchanged — `node.execute()` runs inside the decorated function body; `node.output_df.to_native()` returns a PySpark DataFrame to SDP. Same boundary as Lakeflow today.

5. **SDP execution detection** — `is_sdp_execute()` checks:
   - `spark.conf.get("spark.pipelines.flow.name")` — set by SDP runtime
   - `spark.conf.get("laktory.is_sdp_execute")` — set via the generated YAML `configuration` block as fallback

6. **Source handling:** only `PipelineNodeDataSource` needs SDP-specific logic (`spark.table()` for batch, `spark.readStream.table()` for streaming). All other source types use their existing `_read_spark()` unchanged.

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

0.11 YAML files using `DATABRICKS_PIPELINE` or `DATABRICKS_JOB` raise a `ValueError` immediately on parse with a message pointing to the new name. No deprecation period.

### Internal property renames

| 0.11 | 0.12 |
|------|------|
| `pipeline.is_orchestrator_dlt` | `pipeline.is_orchestrator_lakeflow` |
| `node.is_orchestrator_dlt` | `node.is_orchestrator_lakeflow` |
| `node.is_dlt_execute` | `node.is_lakeflow_execute` |
| `is_dlt_execute()` in `laktory/__init__` | `is_lakeflow_execute()` |
