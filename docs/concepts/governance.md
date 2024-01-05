When building a data platform, it is essential to consider data governance from the get go.
One key aspect of data governance is managing who has access to what data.

## Unity Catalog
Unity Catalog is a fine-grained governance solution for data and AI on the Databricks platform. 
It provides a central location to administer and audit data access.

More information available [here](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices)

## Grants - Data Access Control
In a governed data solution, grants define data access.
A privilege (like read or write) is granted to a user (or principal) to operate of a given object. 
In the context of unity catalog, they are called securable objects and includes schema, table, volume and others of similar nature.
The full list of privileges and securable objects is described [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html)

Essentially, setting up a grant define who has access to what type of data.

## Permissions - Workspace Operations
Permissions on the other hand are assigned on Databricks objects like clusters, notebooks, pipelines and jobs. 
They don't define what data a user may be able to access, but rather what kind of operation he might be allowed to execute within a workspace.

## Users and Groups
??? "API Documentation"
    [`laktory.models.User`][laktory.models.User]<br>
    [`laktory.models.Group`][laktory.models.Group]<br>
The first step in administering data access is to set some users and assign them to groups.
It is generally recommended to have a set of groups for managing the data grants and another set of groups for managing the workspace permissions.

* `role-workspace-admin`
* `role-metastore-admin`
* `role-engineer`
* `role-analyst`
* `role-scientist`
* `domain-finance`
* `domain-hr`
* `domain-engineering`

Here, groups starting with `role-` are about permissions. They define who can run a pipeline, assign a workspace to a catalog, modify a dashboard or use a specific cluster, but they don't provide any access to the data itself.
The groups starting with `domain-` will assume that role of giving data access to its members.

For example, John Doe, a data engineer working with the finance department would therefore be assigned to both `role-engineer` and `domain-finance`, while Jane Doe, an HR data analyst would be assigned to `role-analyst` and `domain-hr`.

## Implementation

Here is how to use Laktory for creating the users and groups

```py
from laktory import models

# Set groups
groups = [
    models.Group(display_name="role-engineer"),
    models.Group(display_name="role-analyst"),
    models.Group(display_name="domain-finance"),
    models.Group(display_name="domain-hr"),
]
group_ids = {}
for g in groups:
    g.to_pulumi()
    group_ids[g.display_name] = g.id

# Set users
users = [
    models.User(
        user_name="john.doe@okube.ai", groups=["role-engineer", "domain-finance"]
    ),
    models.User(user_name="jane.doe@okube.ai", groups=["role-analyst", "domain-hr"]),
]
```

Laktory also allows to set the privileges and permissions directly at the declaration of the models (when applicable).

```yaml title="pipeline.yaml"
name: pl-stock-prices

catalog: dev
target: finance

permissions:
  - group_name: account users
    permission_level: CAN_VIEW
  - group_name: role-engineers
    permission_level: CAN_RUN
...
```

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


A comprehensive example on how to setup users, groups and Unity Catalog objects is available [here](https://github.com/okube-ai/lakehouse-as-code/blob/main/unity-catalog/).  