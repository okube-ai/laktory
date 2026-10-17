from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.databricks._postgrespath import postgres_path_key
from laktory.models.resources.databricks.postgresbranch_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresbranch_base import PostgresBranchBase
from laktory.models.resources.databricks.postgrescatalog import PostgresCatalog
from laktory.models.resources.databricks.postgresdatabase import PostgresDatabase
from laktory.models.resources.databricks.postgresendpoint import PostgresEndpoint
from laktory.models.resources.databricks.postgresrole import PostgresRole
from laktory.models.resources.databricks.postgressnapshotschedule import (
    PostgresSnapshotSchedule,
)
from laktory.models.resources.databricks.postgressyncedtable import PostgresSyncedTable


class PostgresBranch(PostgresBranchBase):
    """
    Lakebase Autoscaling (Postgres) Branch

    Isolated, copy-on-write version of the data of a project (e.g. production,
    development, a feature branch), with its own endpoints, databases and
    roles. A `production` branch is created implicitly with each project: set
    `replace_existing: true` to manage it. Can also be declared in the
    `branches` of a `PostgresProject`.

    Lakebase Autoscaling resources don't detect changes made outside
    Terraform, and a value removed from `spec` is kept by the server.

    Examples
    --------
    ```py
    import io

    from laktory import models

    branch_yaml = '''
    parent: projects/app
    branch_id: production
    replace_existing: true
    spec:
      no_expiry: true
    endpoints:
    - endpoint_id: primary
      replace_existing: true
      spec:
        endpoint_type: ENDPOINT_TYPE_READ_WRITE
        autoscaling_limit_min_cu: 0.5
        autoscaling_limit_max_cu: 4.0
    catalogs:
    - catalog_id: app_pg
      spec:
        postgres_database: app_db
        create_database_if_missing: true
    synced_tables:
    - synced_table_id: app_pg.public.dim_customers
      spec:
        postgres_database: app_db
        source_table_full_name: prod.gold.dim_customers
        primary_key_columns:
        - customer_id
        scheduling_policy: SNAPSHOT
        new_pipeline_spec:
          storage_catalog: prod
          storage_schema: lakebase
    snapshot_schedule:
      schedule:
      - daily_schedule:
          hour: 3
        retention: 168h0m0s
    '''
    branch = models.resources.databricks.PostgresBranch.model_validate_yaml(
        io.StringIO(branch_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_branch](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_branch)
    """

    # Relax field the base marks required but Laktory fills from the parent project
    parent: str = Field(
        None,
        description="Project of the branch (`projects/{project_id}`). Set automatically when declared in the branches of a project.",
    )

    # Laktory-specific
    catalogs: list[PostgresCatalog] = Field(
        None,
        description="Unity Catalog catalogs registering databases of the branch. Their `spec.branch` is set automatically.",
    )
    databases: list[PostgresDatabase] = Field(
        None,
        description="Databases of the branch. Their `parent` is set automatically.",
    )
    endpoints: list[PostgresEndpoint] = Field(
        None,
        description="Endpoints (compute) of the branch. Their `parent` is set automatically.",
    )
    roles: list[PostgresRole] = Field(
        None,
        description="Roles of the branch. Their `parent` is set automatically.",
    )
    snapshot_schedule: PostgresSnapshotSchedule = Field(
        None,
        description="Snapshot schedule of the branch. Its `parent` is set automatically.",
    )
    synced_tables: list[PostgresSyncedTable] = Field(
        None,
        description="Unity Catalog tables synced into databases of the branch. Their `spec.branch` is set automatically.",
    )

    @model_validator(mode="after")
    def assign_parents(self):
        """Set the branch path on nested resources"""
        if self.parent is None:
            return self
        name = f"{self.parent}/branches/{self.branch_id}"
        children = (
            (self.endpoints or [])
            + (self.roles or [])
            + (self.databases or [])
            + ([self.snapshot_schedule] if self.snapshot_schedule else [])
        )
        for r in children:
            if r.parent is None:
                r.parent = name
        for db in self.databases or []:
            db.assign_parents()
        for r in (self.catalogs or []) + (self.synced_tables or []):
            if r.spec is not None and r.spec.branch is None:
                r.spec.branch = name
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return postgres_path_key(self.parent, self.branch_id)

    @property
    def additional_core_resources(self) -> list:
        """
        - endpoints
        - roles
        - databases
        - snapshot schedule
        - catalogs
        - synced tables
        """
        return (
            (self.endpoints or [])
            + (self.roles or [])
            + (self.databases or [])
            + ([self.snapshot_schedule] if self.snapshot_schedule else [])
            + (self.catalogs or [])
            + (self.synced_tables or [])
        )

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return [
            "catalogs",
            "databases",
            "endpoints",
            "roles",
            "snapshot_schedule",
            "synced_tables",
        ]
