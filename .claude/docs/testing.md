# Testing Guide

## Pytest Markers

| Marker | Meaning | Included in `make test`? |
|--------|---------|--------------------------|
| `databricks_connect` | Requires a live Databricks workspace (DatabricksSession or SDK calls) | No — excluded |
| `parametrize` | Standard pytest parametrization | Yes |

Tests that call `skip_dbks_test()` inside the test body also skip automatically when no credentials are available, without a marker.

---

## Running Tests

### Standard run (no live connection needed)

```bash
make test
# equivalent:
uv run pytest -m "not databricks_connect" --junitxml=junit/test-results.xml --cov=laktory --cov-report=xml --cov-report=html tests
```

### Live tests (Databricks Connect)

Requires credentials. See [Live Test Credentials](#live-test-credentials) below.

```bash
set -a && source secrets/.env.dev && set +a && uv run pytest -m "databricks_connect" tests/
```

### Specific file or test

```bash
uv run pytest tests/test_stack.py -v
uv run pytest tests/test_stack.py::test_terraform -v
```

---

## Fixtures (conftest.py)

All fixtures are defined in `tests/conftest.py`.

| Fixture / Helper | Type | Description |
|-----------------|------|-------------|
| `spark` | fixture | Returns a local PySpark session, or a `DatabricksSession` if `databricks-connect` is installed and credentials are available |
| `wsclient` | fixture | Returns a `databricks.sdk.WorkspaceClient`, or `None` if no credentials |
| `assert_dfs_equal(result, expected, sort=True)` | helper | Compares two DataFrames (any Narwhals-compatible backend) by columns and row content |
| `skip_dbks_test()` | helper | Calls `pytest.skip()` if no Databricks credentials can be loaded; use at the top of live tests that don't use the `databricks_connect` marker |

---

## Backend Parametrization

Tests that should run on both Polars and PySpark follow this pattern:

```python
@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_something(backend):
    if backend == "POLARS":
        df = pl.DataFrame(...)
    else:
        df = spark.createDataFrame(...)
    ...
```

The `spark` fixture is injected separately when needed:

```python
@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_something(backend, spark):
    ...
```

---

## Test Directory Layout

```
tests/
  conftest.py              — shared fixtures
  data/                    — static test data files (parquet, CSV)
  dataframe/               — DataFrameTransformer / DataFrameMethod tests
  dataframe_ext/           — Narwhals extension tests
  dataquality/             — DataQualityExpectation tests
  datasinks/               — DataSink tests
  datasources/             — DataSource tests
  pipeline/
    node/                  — PipelineNode execution tests
    orchestrators/         — One file per orchestrator type (sdp, ldp, job, airflow)
    pipeline/              — Pipeline-level tests (quality monitors, etc.)
  resources/               — Databricks resource model tests
  stack/                   — Stack / TerraformStack / DABs tests
  test_cli.py              — CLI command tests (all 7 commands mocked)
  test_perf.py             — Performance regression tests
  test_recursiveloader.py  — YAML loader tests
  test_stack.py            — Stack serialization and variable injection
  test_quickstarts.py      — Quickstart template smoke tests
```

---

## Orchestrator Tests (`tests/pipeline/orchestrators/`)

One test file per orchestrator type. All run without a live Databricks connection unless marked `databricks_connect`. End-to-end execution tests that spawn a subprocess are conditionally skipped when the required CLI is not installed.

### `test_orchestrator_sdp.py` — SparkDeclarativePipelineOrchestrator

Structure mirrors the feature surface of the orchestrator:

| Section | What it covers |
|---------|---------------|
| Build / `spec_dict` | `spec_dict` shape, `storage` path, `laktory.executor = "SDP"` injected, `laktory.config_filepath` present |
| `is_sdp_execute` Spark conf | `test_is_sdp_execute_flag_true/false` — set/unset `laktory.executor` in Spark conf, assert function return value |
| `is_declarative_execute` | Parametrized over `"SDP"` → True, `"LDP"` → True, `"AIRFLOW"` → False, `""` → False |
| `is_orchestrator_sdp` | Property propagates to nodes and sinks |
| `is_streaming` | Sink picks up `as_stream` from source |
| Expectations | `test_expectations_raise_on_sdp_execute` — monkeypatches `laktory.is_sdp_execute` to `True`, asserts `TypeError` |
| CLI flags | `spark-pipelines` CLI args assembled correctly |
| End-to-end execution | `test_execute`, `test_execute_declarative`, `test_streaming_incremental` — run the full pipeline locally via `orchestrator.execute()`; guarded by `is_sdp_available()` |
| Sentinel file | `test_is_sdp_execute_true_during_execution` — `laktory_sdp.py` writes `.laktory_is_sdp_execute` to `root_path`; test reads it after execution. Robust to future changes in `is_sdp_compatible` |

**Key pattern — testing runtime flag detection:**
```python
def test_is_sdp_execute_flag_true(spark):
    _spark = laktory.get_spark_session()
    _spark.conf.set("laktory.executor", "SDP")
    try:
        assert laktory.is_sdp_execute() is True
    finally:
        _spark.conf.unset("laktory.executor")
```
Always clean up with `try/finally`; the `spark` fixture is required to ensure a session exists.

### `test_orchestrator_lakeflow_declarative.py` — LakeflowDeclarativePipelineOrchestrator

Parametrized across both orchestrator types (`ldp` / `sdp`) where behaviour is shared, and LDP-only tests for features specific to Lakeflow.

| Section | What it covers |
|---------|---------------|
| Orchestrator type flags | `is_orchestrator_ldp / is_orchestrator_sdp` booleans |
| Schema propagation | `catalog_name` / `schema_name` pushed to sinks; SDP pushes only `schema_name` |
| `sdp_table_or_view_kwargs` | LDP uses fully-qualified UC name (`dev.sandbox.brz`); SDP uses table name only |
| `is_streaming` | Same as SDP; parametrized |
| Expectations — LDP | `ldp_warning/drop/fail_expectations` dicts on node and sink; empty on `brz` |
| Expectations — SDP | All expectation dicts are empty under SDP orchestrator |
| Expectations — SDP execute | Monkeypatches `is_sdp_execute`; asserts `TypeError` |
| Expectations — LDP streaming | Monkeypatches `is_ldp_execute`; asserts `TypeError` for AGGREGATE on streaming df |
| LDP configuration | `laktory.executor = "LDP"` injected; `laktory.requirements` is valid JSON list containing laktory; `laktory.config_filepath` starts with `/Workspace` and encodes pipeline name |
| Custom dependencies | Extra `dependencies` on Pipeline appear in `laktory.requirements` |
| View node guard | `TableDataSink(table_type="VIEW")` raises `ValueError` / `ValidationError` |

`_get_pl()` is the shared pipeline factory — four nodes: `brz` (file source), `slv` (node source + expectations), `slv_stream` (streaming), `gld_view` (pipeline view sink).

### `test_orchestrator_lakeflow_job.py` — LakeflowJobOrchestrator

| Section | What it covers |
|---------|---------------|
| Task structure | One task per node, DAG edges match pipeline DAG |
| Libraries | `library_list` on job cluster includes laktory package |
| Entry point | Task entry point resolves correctly |
| Config file | `PipelineConfigWorkspaceFile` appears in `additional_core_resources`; path encodes pipeline name |
| DABs | `to_dab_resource()` returns a Job; name is preserved |
| Serverless | Missing `environment_version` raises `ValueError`; environment is populated from pipeline dependencies; task uses `environment_key` |
| Cluster | Task uses `job_cluster_key`; library is `.whl` type; wrong cluster name raises `ValueError` |

---

## Data Quality Tests (`tests/dataquality/test_expectation.py`)

### Existing tests (no orchestrator context)

`test_expectations_abs/rel/agg/empty/exceptions_warnings` — all parametrized over `POLARS` / `PYSPARK`. Cover `run_check()` result counts, filter expressions, `WARN` / `DROP` / `QUARANTINE` / `FAIL` actions, and `DataQualityCheckFailedError`.

### `is_ldp_managed` / `is_sdp_managed` (feat/sdp addition)

These properties combine three conditions (compatibility + orchestrator type + runtime flag). Tests cover the full truth table:

| Test | Condition forced to False | Expected |
|------|--------------------------|----------|
| `test_is_ldp_managed_true` | — (all True via monkeypatch) | `True` |
| `test_is_ldp_managed_false_not_compatible` | AGGREGATE expectation (not `is_ldp_compatible`) | `False` |
| `test_is_ldp_managed_false_wrong_orchestrator` | SDP orchestrator (not `is_orchestrator_ldp`) | `False` |
| `test_is_ldp_managed_false_not_executing` | no monkeypatch (`is_ldp_execute()` → False) | `False` |
| `test_is_sdp_managed_always_false` | `is_sdp_compatible` is always `False` | `False` |

`test_is_sdp_managed_always_false` acts as a canary: it will start failing the moment open-source SDP gains `@dp.expect_*` support, which is the signal to revisit the property logic.

**Shared factory:**
```python
def _get_pl(orchestrator_dict, expectation_dict):
    return models.Pipeline.model_validate({
        "name": "pl-test",
        "orchestrator": orchestrator_dict,
        "nodes": [{"name": "brz", "source": ..., "expectations": [expectation_dict]}],
    })
```

---

## Runtime Flag Detection Pattern

`laktory.is_sdp_execute()`, `is_ldp_execute()`, `is_declarative_execute()` all read a single Spark conf key:

```
laktory.executor = "SDP" | "LDP" | <future: "AIRFLOW", "JOB", "LOCAL">
```

Injected by orchestrators at build time:
- `SparkDeclarativePipelineOrchestrator.spec_dict` → `"SDP"`
- `LakeflowDeclarativePipelineOrchestrator.update_from_parent()` → `"LDP"`

**Monkeypatching for unit tests** (when you don't want to touch Spark conf):
```python
monkeypatch.setattr(laktory, "is_sdp_execute", lambda: True)
```

**Spark conf manipulation for integration tests** (verifies the real read path):
```python
_spark.conf.set("laktory.executor", "SDP")
try:
    assert laktory.is_sdp_execute() is True
finally:
    _spark.conf.unset("laktory.executor")
```

Use monkeypatch when you're testing *consumer* logic (e.g. `check_expectations`). Use Spark conf manipulation when you're testing the *detection function itself*.

---

## Writing New Tests

### Unit test (no live connection)

```python
def test_catalog_name():
    cat = Catalog(name="dev")
    assert cat.resource_name == "catalog-dev"
```

### Parametrized backend test

```python
@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])
def test_transformer(backend, spark):
    if backend == "POLARS":
        df = pl.DataFrame({"x": [1, 2, 3]})
    else:
        df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    # ... apply transformer, assert result
```

### Live Databricks test

```python
@pytest.mark.databricks_connect
def test_read_table(spark, wsclient):
    skip_dbks_test()
    df = spark.read.table("dev.finance.slv_stock_prices")
    assert df.count() > 0
```

### Mocking settings (use monkeypatch, never manual save/restore)

```python
def test_with_setting(monkeypatch):
    monkeypatch.setattr(settings, "some_flag", True)
    # settings is automatically restored after the test
```

---

## Live Test Credentials

Credentials are stored in `secrets/.env.dev` (gitignored). Never commit this file.

```
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net/
DATABRICKS_TOKEN=dapi...
PULUMI_ACCESS_TOKEN=pul-...
```

**Auth priority in `conftest.py`:**
1. Named Databricks SDK profile: `laktory-dev-sp` or `laktory-dev-pat` (from `~/.databricks/profiles.csv`)
2. `DATABRICKS_TOKEN` env var → PAT auth against `DATABRICKS_HOST`
3. `AZURE_CLIENT_SECRET` env var → service principal auth (also needs `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`)

**Load credentials before running live tests:**
```bash
set -a && source secrets/.env.dev && set +a && uv run pytest -m "databricks_connect" tests/
```

On CI, these variables are injected automatically as environment secrets — no sourcing needed.
