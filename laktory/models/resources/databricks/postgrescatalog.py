from pydantic import Field

from laktory.models.resources.databricks.postgrescatalog_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgrescatalog_base import PostgresCatalogBase
from laktory.models.resources.databricks.postgrescatalog_base import PostgresCatalogSpec


class PostgresCatalog(PostgresCatalogBase):
    """
    Lakebase Autoscaling (Postgres) Unity Catalog catalog

    Registers a Postgres database of a branch as a Unity Catalog catalog, so
    its tables can be queried from Databricks and used as the destination of
    synced tables. Can also be declared in the `catalogs` of a
    `PostgresBranch`, which sets `spec.branch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    catalog_yaml = '''
    catalog_id: app_pg
    spec:
      branch: projects/app/branches/production
      postgres_database: app_db
      create_database_if_missing: true
    '''
    catalog = models.resources.databricks.PostgresCatalog.model_validate_yaml(
        io.StringIO(catalog_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_catalog](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_catalog)
    """

    # Required (base marks it optional)
    spec: PostgresCatalogSpec = Field(
        ...,
        description="Catalog configuration: Postgres database and branch (set automatically when declared in the catalogs of a branch)",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.catalog_id
