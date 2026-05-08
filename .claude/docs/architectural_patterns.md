# Architectural Patterns

## 1. Model Hierarchy

All models follow a strict inheritance chain:

```
pydantic.BaseModel
  └── BaseModel (laktory/models/basemodel.py)
        ├── PipelineChild (laktory/models/pipelinechild.py) — tracks pipeline context
        ├── BaseChild (laktory/models/basechild.py) — tracks parent reference
        └── BaseResource (laktory/models/resources/baseresource.py) — adds options, lookup_existing
              └── XyzBase(BaseModel, TerraformResource) — generated base (e.g. catalog_base.py)
                    └── Xyz(XyzBase) — hand-written override (e.g. catalog.py)
```

- `BaseModel` uses a custom metaclass (`ModelMetaclass`) that injects `VariableType` into every field, enabling `${vars.x}` substitution in all string values.
- `BaseChild` adds `_parent` tracking and propagates parent assignments down the tree.
- `BaseResource` adds `options`, `lookup_existing`, and `resource_type_id` fields present on all cloud resources.

Key files: `laktory/models/basemodel.py`, `laktory/models/basechild.py`, `laktory/models/resources/baseresource.py`

---

## 2. IaC Backend (Terraform)

All Databricks resource base classes inherit from `TerraformResource`:

```python
# generated — laktory/models/resources/databricks/catalog_base.py
class CatalogBase(BaseModel, TerraformResource):
    ...

# hand-written — laktory/models/resources/databricks/catalog.py
class Catalog(CatalogBase):
    ...
```

- `TerraformResource` (`laktory/models/resources/terraformresource.py`) — provides `terraform_dump()`, HCL serialization, and the `terraform_resource_type` property
- Pulumi was dropped in v0.11 (PR #537). There is no `PulumiResource` mixin.
- The `iac_backend` property on `Stack` identifies which backend to use (only `"terraform"` is valid now)

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
class Table(TableBase):
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

`TerraformStack` (`laktory/models/stacks/terraformstack.py`) wraps `Stack` for Terraform-specific output. There is no `PulumiStack`.

---

## 11. Generated Base Class Pattern

All Databricks resource models follow a two-file split: a generated base and a hand-written override.

**Generated base** (`*_base.py`) — produced by `scripts/build_resources/01_build.py`:
```python
class CatalogBase(BaseModel, TerraformResource):
    """Generated base class for `databricks_catalog`."""
    __doc_generated_base__ = True

    name: str | None = None
    comment: str | None = None
    force_destroy: bool | None = None
    ...

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_catalog"
```

**Hand-written override** (`catalog.py`) — adds Laktory-specific logic:
```python
from laktory.models.resources.databricks.catalog_base import *  # NOQA: F403
from laktory.models.resources.databricks.catalog_base import CatalogBase

class Catalog(CatalogBase):
    # grants, full_name, custom validators, doc examples, default overrides
```

Rules:
- **Never hand-edit `*_base.py` files** — they are overwritten the next time `01_build.py` runs
- To add or fix a field, either update the generation script or override it in the hand-written class
- `__doc_generated_base__ = True` on the base class tells the griffe extension to split documentation into "Base" and "Laktory" field sections

Generation workflow:
```bash
cd scripts/build_resources
python 00_fetch.py   # fetch terraform provider schema + descriptions
python 01_build.py   # regenerate all *_base.py files
python 02_update_api.py  # regenerate docs/api/models/resources/databricks/*.md stubs
```

Key files: `scripts/build_resources/`, `laktory/models/resources/databricks/*_base.py`

---

## 12. DABs Deployment Path

Databricks Declarative Automation Bundles (DABs) is a second deployment path for pipeline-level resources (Jobs, DLT Pipelines). Unity Catalog and account-level resources still require Terraform.

**Entry point**: `laktory/dab.py` — exports `build_resources(bundle)`, which the Databricks CLI calls via `databricks.yml`:

```yaml
variables:
  laktory_pipelines_dir: ./laktory/pipelines/
  dab_workspace_root: ${workspace.root_path}
python:
  venv_path: .venv
  resources:
    - 'laktory.dab:build_resources'
```

**What `build_resources` does**:
1. Discovers Laktory pipeline YAML files in the configured directory
2. Injects DAB bundle variables into pipelines
3. Writes pipeline JSON config files to the build directory for workspace sync
4. Returns a DABs `Resources` object with all Job and DLT Pipeline definitions

**Orchestrator integration**: `DatabricksJobOrchestrator` and `DatabricksPipelineOrchestrator` both have `to_dab_resource()` methods that convert Laktory orchestrator definitions to DAB-compatible resource dicts.

Key files: `laktory/dab.py`, `laktory/models/orchestrators/databricksjoborchestrator.py`, `laktory/models/orchestrators/databrickspipelineorchestrator.py`

---

## 13. Resource Naming

Every resource has a `resource_name` property that serves two roles:
1. **IaC state address** — the IaC backend uses it to track the resource across deployments. Renaming a resource causes destroy-and-recreate.
2. **Cross-reference key** — other resources and YAML configs reference this resource via `${resources.<name>.<property>}` (e.g., `${resources.catalog-dev.id}`).

### Auto-generated name algorithm

If `resource_options.name` is not set, the name is built as:

```
{resource_type_id}-{resource_safe_key}
```

- `resource_type_id` = class name converted to kebab-case (`SecretScope` → `secret-scope`)
- `resource_safe_key` = the resource's `name` property with special characters (`.`, `@`, spaces, etc.) replaced by `-`
- If `resource_safe_key` is empty → use `resource_type_id` alone
- If `resource_type_id` is already a prefix of `resource_safe_key` → skip the prefix (avoids `catalog-catalog-dev`)

Examples:

| Resource | Auto-generated name |
|----------|-------------------|
| `Catalog(name="dev")` | `catalog-dev` |
| `Schema(name="finance", catalog_name="dev")` | `schema-dev-finance` |
| `Table(name="slv_stock_prices", catalog_name="dev", schema_name="finance")` | `table-dev-finance-slv_stock_prices` |
| `SecretScope(name="my-scope")` | `secret-scope-my-scope` |

### Overriding the name

Set `resource_options.name` to pin the address and decouple it from the resource's `name` property:

```yaml
name: dev
resource_options:
  name: catalog-prod   # IaC address stays "catalog-prod" even if name changes
```

The override value must start with a letter and contain only letters, digits, hyphens, underscores, or `${vars.*}` placeholders.

### Cross-referencing resources

Use `${resources.<name>.<property>}` to inject a resource attribute as the value of another field:

```yaml
# In stack.yaml — reference the catalog's id in a grant
grant:
  principal: account users
  privileges: [USE_CATALOG]
catalog: ${resources.catalog-dev.id}
```

Common properties: `.id`, `.name`, `.full_name`, `.path`. The available properties depend on the resource type.

Key files: `laktory/models/resources/baseresource.py` — `resource_name`, `resource_type_id`, `resource_key`, `to_safe_name()`

---

## 14. Grant Model

Unity Catalog access control is expressed through four overlapping constructs. Understanding which to use prevents accidental privilege loss.

### The two authority modes

| Field / Class | Authority scope | Terraform resource | Behaviour |
|---|---|---|---|
| `Resource.grant` (embedded) | Per-principal | `databricks_grant` | Adds/updates privileges for listed principal(s); all other principals untouched |
| `Resource.grants` (embedded) | All principals | `databricks_grants` | Replaces **every** grant on the resource, including those set outside Laktory |
| Standalone `Grant` resource | Per-principal | `databricks_grant` | Same as embedded `grant` |
| Standalone `Grants` resource | All principals | `databricks_grants` | Same as embedded `grants` |

### Decision guide

**Use `grant` (singular) when:**
- Access is also managed from other sources (Databricks UI, another tool, another stack)
- You want to add Laktory-managed principals without disturbing existing grants
- Safe default: no risk of accidentally revoking access

**Use `grants` (plural) when:**
- Laktory is the single source of truth for all access on this resource
- You want a "complete picture" declaration — exactly this list, nothing more
- Be aware: any grant not in the list is deleted on the next `terraform apply`

**Use standalone `Grant` / `Grants` when:**
- The target securable is not created by Laktory (pre-existing catalog, external table)
- You need to manage grants without owning the resource definition

**For resources Laktory creates, always prefer the embedded fields** (`Catalog.grant`, `Schema.grants`, etc.) — they render the same Terraform resource but keep grants co-located with the resource definition.

### Mutual exclusivity

`grant` and `grants` are mutually exclusive on any resource. Setting both raises a `ValidationError` at construction time (enforced by `BaseResource.grants_validator` in `laktory/models/resources/baseresource.py`).

### Key files

| File | Purpose |
|------|---------|
| `laktory/models/resources/baseresource.py` | `grants_validator()` (mutual exclusivity), `get_grants_additional_resources()` (renders embedded fields into TF resources) |
| `laktory/models/resources/databricks/grant.py` | Standalone per-principal grant resource |
| `laktory/models/resources/databricks/grants.py` | Standalone all-principals grant resource |
| `laktory/models/grants/` | Per-resource grant types (`CatalogGrant`, `SchemaGrant`, etc.) |
| `tests/resources/test_resource_grants.py` | Behaviour tests for all three patterns |

---

## 14. Testing Patterns

- Tests are parametrized over backends: `@pytest.mark.parametrize("backend", ["POLARS", "PYSPARK"])`
- Databricks-only tests are marked: `@pytest.mark.databricks_connect` and excluded from standard test run
- `conftest.py` provides `spark`, `wsclient`, and `assert_dfs_equal()` fixtures
- Test data and fixtures are in `tests/` alongside test files; sample stack configs are in `tests/*/`
