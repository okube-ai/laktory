When building a data platform, it is essential to consider data governance from the get go.
Two distinct mechanisms control who can do what: **access controls** for operations on
workspace objects, and **grants** for data access through Unity Catalog.

| Concern | Field | Objects | Examples |
|---------|-------|---------|---------|
| Workspace access control — who can run, manage, or view a workspace object | `access_controls` | Cluster, Job, Warehouse, Pipeline, Notebook, Dashboard, … | `CAN_RUN`, `CAN_MANAGE`, `CAN_VIEW` |
| Data governance — who can access data | `grant` / `grants` | Catalog, Schema, Table, Volume, ExternalLocation, … | `SELECT`, `USE_CATALOG`, `WRITE_VOLUME` |

These two concerns are completely independent: a user can have permission to run a job without
having any access to the data it reads, and vice versa.

## Unity Catalog

Unity Catalog is a fine-grained governance solution for data and AI on the Databricks platform.
It provides a central location to administer and audit data access.

More information available [here](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices)

## Grants — Data Governance

??? "API Documentation"
    [`laktory.models.resources.databricks.Grant`][laktory.models.resources.databricks.Grant]<br>
    [`laktory.models.resources.databricks.Grants`][laktory.models.resources.databricks.Grants]<br>

Grants define **who has access to what data**. A privilege (like `SELECT` or `WRITE_VOLUME`) is
granted to a principal (user, group, or service principal) on a Unity Catalog securable object
(catalog, schema, table, volume, external location, etc.). Grants are entirely decoupled from
workspace operations — granting `SELECT` on a table does not allow a user to run the pipeline
that produces it.

The full list of privileges and securable objects is described
[here](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html).

Resources that Laktory creates (Catalog, Schema, Table, Volume, etc.) expose two fields for
defining grants inline:

- `grant` — non-destructive: adds or updates privileges for the listed principal(s) without
  removing grants for other principals. Use when access is also managed from other sources
  (Databricks UI, another tool).
- `grants` — full replacement: replaces **all** existing grants on the resource, including
  those set outside Laktory. Use only when Laktory is the sole source of truth for this
  resource's access.

```yaml title="catalog.yaml"
- name: prod
  grants:
    - principal: account users
      privileges:
        - USE_CATALOG
        - USE_SCHEMA
  schemas:
    - name: finance
      grants:
        - principal: domain-finance
          privileges:
            - SELECT
    - name: engineering
      grants:
        - principal: domain-engineering
          privileges:
            - SELECT
```

## Access Controls — Workspace Operations

??? "API Documentation"
    [`laktory.models.resources.databricks.Permissions`][laktory.models.resources.databricks.Permissions]<br>
    [`laktory.models.resources.databricks.AccessControl`][laktory.models.resources.databricks.AccessControl]<br>

`access_controls` defines **who can perform operations on workspace objects** — execute a job,
restart a cluster, view a notebook, manage a dashboard. This is fully independent of data access:
a user with `CAN_RUN` on a job can trigger it without having any `SELECT` privilege on the tables
it reads.

Supported resources include: Cluster, ClusterPolicy, Job, Warehouse, Pipeline, Notebook, App,
Dashboard, Alert, Query, Repo, MlflowModel, MlflowExperiment, WorkspaceFile, WorkspaceTree, and
others.

When `access_controls` is set, Laktory automatically generates a `Permissions` resource as a
child of the parent resource.

Common permission levels:

| Level | Meaning |
|-------|---------|
| `CAN_VIEW` | Read-only access to the object |
| `CAN_RUN` | Execute / trigger the object (job, pipeline) |
| `CAN_MANAGE_RUN` | Manage runs without full ownership |
| `CAN_MANAGE` | Full control including editing and deleting |

```yaml title="job.yaml"
name: pl-stock-prices-job

access_controls:
  - group_name: account users
    permission_level: CAN_VIEW
  - group_name: role-engineers
    permission_level: CAN_MANAGE_RUN
```

## Users and Groups

??? "API Documentation"
    [`laktory.models.resources.databricks.User`][laktory.models.resources.databricks.User]<br>
    [`laktory.models.resources.databricks.Group`][laktory.models.resources.databricks.Group]<br>

The first step in administering access is to create users and assign them to groups.
It is generally recommended to maintain separate groups for workspace access controls and for
data grants:

* `role-workspace-admin`
* `role-metastore-admin`
* `role-engineer`
* `role-analyst`
* `role-scientist`
* `domain-finance`
* `domain-hr`
* `domain-engineering`

Groups prefixed with `role-` govern workspace operations: who can run a pipeline, manage a
cluster, or modify a dashboard — but they grant no access to data itself. Groups prefixed with
`domain-` govern data access: who can read or write data in a given business domain.

For example, John Doe — a data engineer working with the finance department — would be assigned
to both `role-engineer` and `domain-finance`, while Jane Doe — an HR data analyst — would be
assigned to `role-analyst` and `domain-hr`.

## Implementation

Here is how to use Laktory for creating users and groups:

```py
from laktory import models

# Set groups
groups = [
    models.Group(display_name="role-engineer"),
    models.Group(display_name="role-analyst"),
    models.Group(display_name="domain-finance"),
    models.Group(display_name="domain-hr"),
]

# Set users
users = [
    models.User(
        user_name="john.doe@okube.ai", groups=["role-engineer", "domain-finance"]
    ),
    models.User(user_name="jane.doe@okube.ai", groups=["role-analyst", "domain-hr"]),
]
```

A comprehensive example of how to set up users, groups, and Unity Catalog objects is available
[here](https://github.com/okube-ai/lakehouse-as-code/blob/main/unity-catalog/).
