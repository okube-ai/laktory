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
  datasinks/               — DataSink tests
  datasources/             — DataSource tests
  pipeline/                — Pipeline / PipelineNode tests
  resources/               — Databricks resource model tests
  stack/                   — Stack / TerraformStack / DABs tests
  test_cli.py              — CLI command tests (all 7 commands mocked)
  test_perf.py             — Performance regression tests
  test_recursiveloader.py  — YAML loader tests
  test_stack.py            — Stack serialization and variable injection
  test_quickstarts.py      — Quickstart template smoke tests
```

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
