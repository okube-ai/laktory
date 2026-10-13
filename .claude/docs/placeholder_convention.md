# Placeholder Convention: Variables, Expressions, and References

Established 2026-06-01. Apply this terminology consistently in docstrings, code comments, and documentation.

---

## The three concepts

| Name | Syntax | Resolved | Valid in |
|------|--------|----------|----------|
| **Variable** | `${vars.X}` · `${resources.X.id}` | Config / deployment time | Any model field[^1] |
| **Expression** | `${{ python expr }}` | Config / deployment time | Any model field[^1] |
| **Parameter** | `${params.X}` | Run time, before any data is read | Pipeline nodes (and `root_path`) only |
| **Reference** | `{df}` · `{sources.X}` · `{nodes.X}` | Execution time | Transformer nodes only |

[^1]: Also resolvable inside the *content* of a handful of local files referenced by a model field (`Dashboard.file_path`, `WorkspaceFile`/`Notebook`/`DbfsFile.source`, `WorkspaceTree`-generated files) — opt-in only, off by default. See "Rendered file content" in `docs/concepts/variables.md` and pattern 17 in `architectural_patterns.md`.

**The `$` rule** - the dollar sign is the tell. Variables, Expressions and Parameters always start with `$` and are resolved before any data is read (Variables / Expressions at deployment, Parameters when a run starts). References have no `$` and are resolved at runtime by the transformer engine when DataFrames are live in memory.

---

## Variables

`${vars.X}` substitutes a named value into any model field. Declared in `variables:` blocks, CLI `--var` flags, variable files, environment variables, or `laktory.settings`. Resolved during config processing / deployment.

`${resources.X.id}` is also a Variable - the `resources.*` namespace exposes deployed IaC resource output attributes. Auto-populated by Laktory from the Terraform backend. Same `${}` syntax, same resolution time. Not a fourth category.

## Parameters

`${params.X}` references a pipeline `parameters` entry (#694). Left unresolved in the deployed config and resolved by `Pipeline.resolve_parameters()` at the start of a run (job parameters, Airflow `conf`, `execute(params=...)`). Resolution goes through `objs["params"]` in `_parsers._resolve_value`. Not supported with declarative orchestrators.

## Expressions

`${{ python expr }}` evaluates an inline Python statement and injects the result. Full Python supported. Certain context objects available (`pipeline`, `pipeline_node`) depending on nesting level.

## References

`{df}`, `{sources.name}`, `{nodes.X}` identify DataFrames inside transformer nodes. Only valid in `DataFrameExpr` SQL expressions and `DataFrameMethod` `func_args` / `func_kwargs`. Never valid in arbitrary model fields.

| Reference | Points to |
|-----------|-----------|
| `{df}` | Flowing DataFrame - primary source on step 1, output of previous step on step N |
| `{sources.name}` | Named source declared on the pipeline node (`PipelineNode.sources`) |
| `{nodes.X}` | Output DataFrame of upstream pipeline node `X` |

---

## Why "Reference" (not Placeholder, Token, Binding, Alias, or Ref)

- **Placeholder** - describes the mechanism (slot to fill), not the intent
- **Token** - internal/compiler connotation, not user-facing
- **Binding** - carries UI framework baggage, implies configuration-time
- **Alias** - fits SQL joins but breaks down for method arguments
- **Ref** - good domain recognition from dbt but informal/borrowed
- **Reference** - self-explanatory, runtime-appropriate, pairs cleanly with Variable and Expression

---

## Canonical user-facing documentation

`docs/concepts/variables.md` - titled "Variables, Expressions, and References" - is the single source of truth for the full user-facing explanation. `docs/concepts/transformer.md` has the full Reference detail under `## DataFrame References`.
