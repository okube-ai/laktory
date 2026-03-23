# Architectural Patterns

## 1. Model Hierarchy

All models follow a strict inheritance chain:

```
pydantic.BaseModel
  └── BaseModel (laktory/models/basemodel.py)
        ├── PipelineChild (laktory/models/pipelinechild.py) — tracks pipeline context
        ├── BaseChild (laktory/models/basechild.py) — tracks parent reference
        └── BaseResource (laktory/models/resources/baseresource.py) — adds options, lookup_existing
              └── Xyz(BaseModel, PulumiResource, TerraformResource) — concrete resources
```

- `BaseModel` uses a custom metaclass (`ModelMetaclass`) that injects `VariableType` into every field, enabling `${vars.x}` substitution in all string values.
- `BaseChild` adds `_parent` tracking and propagates parent assignments down the tree.
- `BaseResource` adds `options`, `lookup_existing`, and `resource_type_id` fields present on all cloud resources.

Key files: `laktory/models/basemodel.py`, `laktory/models/basechild.py`, `laktory/models/resources/baseresource.py`

---

## 2. Dual IaC Backend (Pulumi + Terraform)

Every Databricks resource uses multiple inheritance to support both IaC backends simultaneously:

```python
class Catalog(BaseModel, PulumiResource, TerraformResource):
    ...
```

- `PulumiResource` (`laktory/models/resources/pulumiresource.py`) — provides `pulumi_dump()` and Pulumi resource registration
- `TerraformResource` (`laktory/models/resources/terraformresource.py`) — provides `terraform_dump()` and HCL serialization

Both mixins rely on `pulumi_dump()` / `terraform_dump()` using `model_dump(by_alias=True, exclude_unset=True)` with backend-specific transforms.

---

## 3. Union-Based Type Dispatch

Instead of a single abstract base class with polymorphism, related types are combined into `Union` type aliases used in model fields. Pydantic uses the `discriminator` field for efficient dispatch.

Examples:
- `laktory/models/datasources/__init__.py` — `DataSourcesUnion`
- `laktory/models/datasinks/__init__.py` — `DataSinksUnion`
- `laktory/models/grants/__init__.py` — `GrantsUnion`

```python
source: DataSourcesUnion = Field(..., discriminator="source_type")
```

---

## 4. Composite Resource & Parent Propagation

Resources can contain child resources. Parent assignments cascade via `@model_validator(mode="after")`:

```python
# laktory/models/resources/databricks/catalog.py
@model_validator(mode="after")
def assign_names(self):
    for schema in self.schemas:
        schema.catalog_name = self.name
        for table in schema.tables:
            table.catalog_name = self.name
            table.schema_name = schema.name
    return self
```

This pattern ensures that when a `Catalog` is constructed from YAML with nested `Schema` and `Table` objects, all hierarchical names are automatically resolved without requiring the user to repeat them.

---

## 5. Hierarchical Naming via Properties

Resources expose `full_name` and `parent_full_name` properties for Unity Catalog dot-notation paths:

```python
@property
def full_name(self) -> str:
    return f"{self.catalog_name}.{self.schema_name}.{self.name}"
```

Applies consistently to: `Catalog`, `Schema`, `Table`, `Volume`, `Function`, `Grant`, etc.

---

## 6. Lookup / Existing Resource Pattern

Every resource has an optional `lookup_existing` field (typed as `XyzLookup`) to reference a pre-existing cloud resource instead of creating a new one:

```python
class Table(BaseModel, PulumiResource, TerraformResource):
    lookup_existing: TableLookup | None = Field(None, exclude=True)
```

`XyzLookup` classes are defined alongside the resource in the same file (e.g., `laktory/models/resources/databricks/table.py`).

---

## 7. Variable Injection

All model fields accept `${vars.name}` (simple substitution) and `${{ expression }}` (evaluated Python expression) syntax. This is enabled by the custom metaclass in `BaseModel`.

Key methods on every model:
- `inject_vars(vars: dict)` — recursively substitutes variables in all fields
- `push_vars(vars: dict)` — propagates variables down to child models

Usage pattern in YAML configs:
```yaml
name: cluster-${vars.env}
num_workers: ${{ 4 if vars.env == 'prod' else 1 }}
```

---

## 8. YAML Custom Tags

`laktory/yaml/recursiveloader.py` extends PyYAML with three custom tags:

| Tag | Behavior |
|-----|---------|
| `!use filepath` | Replace node with contents of another YAML file |
| `!extend filepath` | Append items from another YAML file into a list |
| `<<: !update filepath` | Deep-merge a dict from another YAML file |

Variables can be used in filepaths: `!use catalogs_${vars.env}.yaml`

Loading entry point: `BaseModel.model_validate_yaml(fp, vars=None)` in `laktory/models/basemodel.py`.

---

## 9. Pipeline as DAG

A `Pipeline` (`laktory/models/pipeline/pipeline.py`) contains a list of `PipelineNode` objects forming a DAG tracked by NetworkX.

Each `PipelineNode`:
- Has one `source` (a `DataSource` or another node via `PipelineNodeDataSource`)
- Has a `transformer` (`DataFrameTransformer`) — an ordered list of `DataFrameMethod` or `DataFrameExpr` steps
- Has zero or more `sinks` (list of `DataSink`)

Transformation chain (applied sequentially):
```
source_df → method1(df) → expr2(df) → method3(df) → sink
```

`DataFrameTransformer`: `laktory/models/dataframe/dataframetransformer.py`
`PipelineNode`: `laktory/models/pipeline/pipelinenode.py`

---

## 10. Stack Composition

A `Stack` (`laktory/models/stacks/stack.py`) is the top-level deployment unit combining:
- Named `Pipeline` objects (`dict[str, Pipeline]`)
- Named resource objects via `StackResources` — which contains one `dict[str, XyzResource]` field per resource type (50+ fields)

This flat dictionary structure (rather than nesting) means all resources are directly addressable and can be iterated uniformly.

---

## 11. Testing Patterns

- Tests are parametrized over backends: `@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])`
- Databricks-only tests are marked: `@pytest.mark.databricks_connect` and excluded from standard test run
- `conftest.py` provides `spark`, `wsclient`, and `assert_dfs_equal()` fixtures
- Test data and fixtures are in `tests/` alongside test files; sample stack configs are in `tests/*/`
