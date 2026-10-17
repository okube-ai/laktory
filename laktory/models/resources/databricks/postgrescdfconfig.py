from pydantic import Field

from laktory.models.resources.databricks.postgrescdfconfig_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgrescdfconfig_base import (
    PostgresCdfConfigBase,
)


class PostgresCdfConfig(PostgresCdfConfigBase):
    """
    Lakebase Autoscaling (Postgres) change data feed configuration

    Replicates the tables of a Postgres schema of a database into a Unity
    Catalog schema. Can also be declared in the `cdf_configs` of a
    `PostgresDatabase`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    cdf_yaml = '''
    parent: projects/app/branches/production/databases/app-db
    postgres_schema: public
    catalog: dev
    schema: app_replicated
    '''
    cdf = models.resources.databricks.PostgresCdfConfig.model_validate_yaml(
        io.StringIO(cdf_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_cdf_config](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_cdf_config)
    """

    # Relax field the base marks required but Laktory fills from the parent database
    parent: str = Field(
        None,
        description="Database replicated from (`projects/{project_id}/branches/{branch_id}/databases/{database_id}`). Set automatically when declared in the cdf_configs of a database.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}/{self.cdf_config_id or self.postgres_schema}"
