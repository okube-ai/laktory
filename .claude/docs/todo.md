# Laktory TODO List

Planned features, bug fixes and architectural changes

---

## 1. Features

Things that would be nice for the user and enhance usability.

| #      | Description                                                                                                                                                |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ~~F1~~ | ~~Add the ability to deploy the terraform state file to Databricks workspace like DAB~~                                                                    |
| ~~F2~~ | ~~Add the ability to register Spark Extensions~~ ✓                                                                                                         |
| ~~F3~~ | ~~Add lookup existing resources on all relevant resources~~ ✓                                                                                              |
| ~~F4~~ | ~~Better error message. During validation, it's almost impossible to understand what's going on because multiple models are possible (sources / sinks)~~ ✓ |
| ~~F5~~ | ~~Let the user run SQL tasks on warehouse instead of job compute~~ Too complex (metadata, full refresh, local execution) for the added benefits.           |
| ~~F6~~ | ~~Integration of Spark-native declarative piplines~~ ✓                                                                                                    |
| ~~F8~~ | ~~Expand `TableDataSink.format` beyond `Literal["PARQUET", "DELTA"]` to include ORC and AVRO (Spark `USING` clause supports them for managed Hive tables)~~ ✓ |
| ~~F7~~ | ~~Allow the user to pass variables through the CLI~~ `--var key=value` (repeatable) and `--var-file` (YAML); auto-discovers `variables.yaml` next to stack ✓ |


---

## 2. Bug Fixes

Issues that need to be resolved

| # | Description                                          |
|---|------------------------------------------------------|
| ~~B1~~ | ~~`DataFrameMethodArg.value` deserialized as plain dict when `DataSourcesUnion \| Any` strict-mode enum coercion fails for `dataframe_backend` string — fixed via `parse_datasource_value` field_validator (PR #573)~~ ✓ |
| ~~B2~~ | ~~`DType(name="Datetime").to_pyspark()` returns `TimestampNTZType()` instead of `TimestampType()`. Root cause: `to_narwhals()` calls `nw.Datetime()` with no `time_zone`, and Narwhals only maps to `TimestampType` when a timezone is set. Fix: add `time_unit` and `time_zone` fields to `DType` and thread them through `to_narwhals()` for `Datetime`/`Duration` types.~~ ✓ |
| ~~B3~~ | ~~`TableDataSink.create(df)` is called before `write()` during pipeline node execution and creates the table from the raw DataFrame schema. When `merge_cdc_options.scd_type == 2`, the merge logic in `DataSinkMergeCDCOptions._init_target()` is responsible for creating the table with extra SCD type 2 columns (`__hash_cols`, `__start_at`, `__end_at`), but it is skipped because the table already exists. Fix: in `TableDataSink.create()`, when `merge_cdc_options` is set, delegate to `_init_target()` so the correct schema (including SCD2 extra columns) is used.~~ ✓ |

## 3. Internal

Internal improvements


| #      | Description                                                                                                                                                 |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ~~I1~~ | ~~Ask Claude to review its internal documentation, restructure and optimize. What are we missing?~~ Created `testing.md`; archived roadmap and audit docs ✓ |
| ~~I2~~ | ~~Always authorize Claude to [reduce permission prompts]~~ Replaced 96 one-off entries with 14 broad patterns in `settings.local.json` ✓                    |
| ~~I3~~ | ~~Add instructions for Claude to have access to secrets for running unit tests?~~ Documented in `testing.md` + added `source secrets/.env.dev` permission ✓ |
| I4     | Run `run_quickstarts.yml`, `run_quickstart_da.yml` and ideally lakehouse-as-code as part of the release.yml to prevent regression.                          |

 
## 4. Architecture

Internal improvements


| #  | Description                                                                                                       |
|----|-------------------------------------------------------------------------------------------------------------------|
| A1 | How can I ensure that Claude / GPT knows Laktory and use cases so that users can benefit from it ? MCP?           |
| A2 | How can I offer an AI first solution? Agents that understand lineage and proposes solutions from natural language |
| A3 | **Refactor `DataFrameMethodArg` / datasource argument parsing** — three structural tensions keep producing bugs at this seam: (1) `value: DataSourcesUnion \| Any` conflates two responsibilities (datasource reference vs. arbitrary expression value); Pydantic's union resolver wasn't designed for this and needs hand-written validator layers to compensate. (2) `DataFrameMethodArg.eval()` scans strings for magic tokens (`"F."`, `"col("`, …) and calls `eval()` — backend-coupled, validated only at runtime, fragile at the boundary; `DataFrameExpr` already exists for this. (3) `dataframe_backend`/`dataframe_api` are context fields (inherited via `PipelineChild`) but are serialized into every nested child, causing round-trip deserialization failures when a `PipelineNodeDataSource` lives inside a `func_kwarg`. Suggested direction: exclude context fields from child serialization; unify string expression handling under `DataFrameExpr`; add a Literal `type` to `CustomDataSource` to enable a Pydantic discriminated union on `DataSourcesUnion`. |
