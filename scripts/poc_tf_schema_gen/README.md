# POC: Generate Resource Models from Terraform Provider Schema

Explores auto-generating Pydantic resource models from the Databricks Terraform
provider schema, to replace the 50+ hand-maintained models in
`laktory/models/resources/databricks/`.

---

## Setup

```sh
cd scripts/poc_tf_schema_gen

# Fetch schema (requires terraform CLI)
terraform init
terraform providers schema -json > databricks_schema.json

# Run generator
python approach_a_templates.py

# Run field coverage diff
python field_coverage.py

# Run layering tests
python test_layering.py
```

---

## Generator: `approach_a_templates.py`

Parses `databricks_schema.json` directly and emits Python source code.

**Key behaviors:**
- Skips `computed`-only fields (set by the provider after apply, not user-settable) and `id`
- Maps Terraform types to Python annotations (`"string"` → `str`, `["list","string"]` → `list[str]`, etc.)
- Recursively emits `block_types` as named nested models with parent-prefixed names
  (e.g., `CatalogEffectivePredictiveOptimizationFlag`) to avoid import collisions
- `max_items: 1` blocks emitted as `Optional[Model]`, not `list`
- Handles `RESERVED_FIELD_RENAMES`: field names that collide with `BaseResource`/`BaseModel`
  (e.g., `options` → `options_` with `serialization_alias="options"`)
- Generated classes inherit `BaseModel, TerraformResource` (Pulumi dropped per roadmap)
- Field descriptions preserved from schema

**Usage:**
```sh
python approach_a_templates.py                   # all default targets
python approach_a_templates.py databricks_volume # single resource
```

Output lands in `compare_output/`.

---

## Field Coverage Report

Run `python field_coverage.py` for a full diff. Summary:

### `databricks_volume`
- Schema fields (user-settable): 7 | Laktory fields: 8 | Overlap: 6
- **Gaps** (in schema, missing from Laktory): `owner`
- **Laktory extensions** (not in schema): `grant`, `grants`

### `databricks_catalog`
- Schema fields: 16 | Laktory fields: 20 | Overlap: 15
- **Gaps**: `options` _(Laktory uses `foreign_options` with a rename — not a real gap)_
- **Laktory extensions**: `foreign_options`, `grant`, `grants`, `schemas`, `workspace_bindings`

### `databricks_metastore`
- Schema fields: 13 | Laktory fields: 21 | Overlap: 10
- **Gaps**: `external_access_enabled`, `privilege_model_version`, `storage_root_credential_name`
- **Laktory extensions**: `created_at`, `created_by`, `data_accesses`, `global_metastore_id`,
  `grant`, `grants`, `grants_provider`, `metastore_id`, `updated_at`, `updated_by`, `workspace_assignments`
  _(many are computed-only fields Laktory kept for read access — worth reviewing)_

### `databricks_schema`
- Schema fields: 9 | Laktory fields: 9 | Overlap: 5
- **Gaps**: `enable_predictive_optimization`, `metastore_id`, `owner`, `properties`
- **Laktory extensions**: `grant`, `grants`, `tables`, `volumes`

---

## Layering Pattern

Generated base classes carry only what Terraform knows (field names, types, defaults,
descriptions). Hand-written business logic lives in a subclass:

```python
# Generated base (do not edit):
class VolumeBase(BaseModel, TerraformResource):
    name: str = Field(...)
    catalog_name: str = Field(...)
    ...
    @property
    def terraform_resource_type(self) -> str:
        return "databricks_volume"

# Hand-written override (lives in laktory/models/resources/databricks/):
class Volume(VolumeBase):
    grants: list[VolumeGrant] = Field(None, ...)

    @property
    def terraform_excludes(self): return ["grant", "grants"]
    @property
    def additional_core_resources(self): ...
```

Validated by `test_layering.py`: `isinstance`, `model_dump`, `terraform_properties`
exclusions, variable injection, and custom properties all work correctly.

---

## Next Steps

1. Extend generator to cover all 150 resources (currently targets 5)
2. Handle `RESERVED_FIELD_RENAMES` by introspecting `BaseResource.model_fields` at generation
   time rather than a hardcoded list
3. Establish file system split:
   - `laktory/models/resources/databricks/generated/volume_base.py` — auto-generated
   - `laktory/models/resources/databricks/volume.py` — hand-written override importing `VolumeBase`
4. Replace `StackResources` 51 hardcoded fields with a registry + `create_model` pattern
