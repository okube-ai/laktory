## The default root

`settings.workspace_root` is the root directory in your Databricks workspace where deployed objects land - notebooks, workspace files, dashboards, alerts, queries, and more (see [What lands under `workspace_root`](#what-lands-under-workspace_root) below). It defaults to a flat, fixed path:

```
/.laktory/
```

Simple, but it doesn't scope by who deployed it, which stack, or which environment. Two different stacks - or two environments of the same stack - deployed to the same workspace by default write their objects under that same `/.laktory/` root. If they happen to use overlapping relative paths (e.g. both have a `notebooks/ingest.py`), they collide.

## Scoping the root to your user, stack, and environment: `workspace_root: "user_root"`

Set `settings.workspace_root` to the reserved value `user_root` to auto-compute a root scoped to your own Databricks identity, this stack, and this environment instead:

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
```

This requires a `DatabricksProvider` in the stack (it resolves your username via a live SDK call - see [How the username is resolved](#how-the-username-is-resolved)) and computes:

```
/Users/{you}/.laktory/{stack_name}/{env_name}/
```

The name is a mnemonic, not a literal promise that the value is *just* `/Users/{you}/` - it's your own root, further scoped down to this specific stack and environment so it can't collide with anyone else's deployment, or your own other stacks/environments.

## What lands under `workspace_root`

Whatever `workspace_root` is set to (the default, `user_root`, or a fully custom value), it's the default deployment location for every resource type below, unless that resource's own explicit path field overrides it. Verified directly against the current implementation:

| Resource | Field(s) | Can fully override (escape `workspace_root`)? |
|---|---|---|
| `Notebook` | `dirpath` (relative) / `path` (absolute) | Yes, via `path` |
| `WorkspaceFile` | `dirpath` / `path` | Yes, via `path` |
| `PythonPackage` | `dirpath` / `path` | Yes, via `path` |
| `WorkspaceTree` | `path` (whole tree) | Yes, via `path` - otherwise every file inside follows the same rules as `Notebook`/`WorkspaceFile` above |
| `Dashboard` | `parent_path` (a subfolder *inside* `workspace_root`) | **No** - always under `workspace_root` |
| `Alert` | `parent_path` | **No** - always under `workspace_root` |
| `Query` | `parent_path` | **No** - always under `workspace_root` |
| A pipeline's config file (auto-created for orchestrators like `LAKEFLOW_DECLARATIVE_PIPELINE`) | n/a | No |

`Dashboard`, `Alert`, and `Query` are worth calling out specifically: unlike `Notebook`/`WorkspaceFile`/`PythonPackage`/`WorkspaceTree`, they have no equivalent of `path` to escape `workspace_root` entirely - `parent_path` only picks a subfolder *within* it.

## Using it alongside Terraform state: `backend.databricks_workspace`

`workspace_root` only controls where deployed *objects* land - it has no effect on where Terraform *state* is stored. That's a separate, independent mechanism: setting `terraform.backend.databricks_workspace: true` auto-configures a Terraform HTTP backend that stores state as a file in your own Databricks user directory, scoped by stack name and environment - no separate cloud storage account needed:

```yaml title="stack.yaml"
terraform:
  backend:
    databricks_workspace: true
```

This is unrelated to Terraform's own default (a plain local `terraform.tfstate` file on disk) - it's an explicit opt-in, resolved the same way as `user_root` (live SDK username lookup, requires a `DatabricksProvider`), computing:

```
/Users/{you}/.laktory/{stack_name}/{env_name}/state/terraform.tfstate
```

Setting one does not enable the other - they're independent opt-ins - but their computed roots are deliberately built from the same template, so using both together nests state *inside* the same root your objects deploy to, with zero extra configuration:

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
terraform:
  backend:
    databricks_workspace: true
```

```
/Users/{you}/.laktory/{stack_name}/{env_name}/                          <- settings.workspace_root
/Users/{you}/.laktory/{stack_name}/{env_name}/notebooks/my_notebook.py  <- a deployed Notebook
/Users/{you}/.laktory/{stack_name}/{env_name}/state/terraform.tfstate   <- Terraform state
```

When both are enabled together, Laktory resolves your username only once and reuses it for both, instead of making two separate lookups.

## How the username is resolved

`user_root`, `backend.databricks_workspace: true`, and the [`${current_user.X}` variable namespace](variables.md#current-user) all resolve your identity the same way: they find a `DatabricksProvider` in the stack, build a Databricks SDK `WorkspaceClient` from whatever credentials are already configured on it (token, profile, service principal, ...), and call the SDK's current-user endpoint. If that provider is authenticated as a human, this is your email; if it's authenticated as a service principal (e.g. in CI/CD), it's the service principal's identity instead - "your own root" then really means "whoever/whatever this provider is authenticated as." Either way, no extra credentials or scopes are required beyond what the `DatabricksProvider` already needs.

Because this is a live API call, commands that use it (`build`, `preview`, `deploy`, `destroy`, `validate`) need a real Databricks connection - unlike a stack that touches none of these, which can run those commands with no live credentials at all. When more than one of the three is used in the same stack, Laktory resolves your username only once and reuses it for all of them.

## Customizing the root

`user_root` is a convenience default, not a requirement. Set `settings.workspace_root` to any explicit string instead - including one built with [variables](variables.md), e.g. to scope by team rather than by individual user:

```yaml title="stack.yaml"
settings:
  workspace_root: /Users/${vars.team_service_principal}/.laktory/${vars.env}/
```

Once defined, reuse that same value anywhere else in the stack via `${settings.workspace_root}` (see [Variables - Settings](variables.md#settings)) instead of duplicating the literal - useful for a resource that sets an explicit `path:`, like `WorkspaceTree`:

```yaml title="stack.yaml"
resources:
  databricks_workspacetrees:
    app:
      source: ./app/
      path: ${settings.workspace_root}app
```

`terraform.backend` can similarly be set to any standard Terraform backend block (`azurerm`, `s3`, `gcs`, `http`, ...) instead of `databricks_workspace: true`, fully independently of how `workspace_root` is configured.
