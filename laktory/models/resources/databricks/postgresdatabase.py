from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.databricks.postgrescdfconfig import PostgresCdfConfig
from laktory.models.resources.databricks.postgresdataapi import PostgresDataApi
from laktory.models.resources.databricks.postgresdatabase_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresdatabase_base import (
    PostgresDatabaseBase,
)


class PostgresDatabase(PostgresDatabaseBase):
    """
    Lakebase Autoscaling (Postgres) Database

    Postgres database of a branch, owned by a role of the same branch. A
    `databricks-postgres` database is created implicitly with each branch:
    set `replace_existing: true` to manage it. Removing a managed database
    from the configuration deletes it. Can also be declared in the
    `databases` of a `PostgresBranch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    database_yaml = '''
    parent: projects/app/branches/production
    database_id: app-db
    spec:
      postgres_database: app_db
      role: projects/app/branches/production/roles/app-owner
    data_api:
      spec:
        db_schemas:
        - public
    cdf_configs:
    - postgres_schema: public
      catalog: dev
      schema: app_replicated
    '''
    database = models.resources.databricks.PostgresDatabase.model_validate_yaml(
        io.StringIO(database_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_database](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_database)
    """

    # Relax field the base marks required but Laktory fills from the parent branch
    parent: str = Field(
        None,
        description="Branch of the database (`projects/{project_id}/branches/{branch_id}`). Set automatically when declared in the databases of a branch.",
    )

    # Laktory-specific
    cdf_configs: list[PostgresCdfConfig] = Field(
        None,
        description="Change data feed configurations replicating schemas of the database into Unity Catalog. Their `parent` is set automatically.",
    )
    data_api: PostgresDataApi = Field(
        None,
        description="Data API exposing the database. Its `parent` is set automatically.",
    )

    @model_validator(mode="after")
    def assign_parents(self):
        """Set the database path on nested resources"""
        if self.parent is None:
            return self
        for r in ([self.data_api] if self.data_api else []) + (self.cdf_configs or []):
            if r.parent is None:
                r.parent = self.path
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def path(self) -> str:
        """Lakebase resource path `{branch path}/databases/{database_id}`"""
        return f"{self.parent}/databases/{self.database_id}"

    @property
    def resource_key(self) -> str:
        return self.path

    @property
    def additional_core_resources(self) -> list:
        """
        - data API
        - change data feed configurations
        """
        return ([self.data_api] if self.data_api else []) + (self.cdf_configs or [])

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["cdf_configs", "data_api"]
