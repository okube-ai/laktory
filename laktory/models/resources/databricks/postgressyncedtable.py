from laktory.models.resources.databricks.postgressyncedtable_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgressyncedtable_base import (
    PostgresSyncedTableBase,
)


class PostgresSyncedTable(PostgresSyncedTableBase):
    """
    Lakebase Autoscaling (Postgres) synced table

    Copy of a Unity Catalog Delta table into a Postgres table for low-latency
    serving, maintained by a managed Lakeflow pipeline. `SNAPSHOT` scheduling
    copies the full table on each sync; `TRIGGERED` and `CONTINUOUS` are
    incremental and require Change Data Feed on the source table. The source
    table must exist when the synced table is created. Can also be declared in
    the `synced_tables` of a `PostgresBranch`, which sets `spec.branch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    synced_table_yaml = '''
    synced_table_id: app_pg.public.dim_customers
    spec:
      branch: projects/app/branches/production
      postgres_database: app_db
      source_table_full_name: prod.gold.dim_customers
      primary_key_columns:
      - customer_id
      scheduling_policy: TRIGGERED
      create_database_objects_if_missing: true
      new_pipeline_spec:
        storage_catalog: prod
        storage_schema: lakebase
    '''
    synced_table = models.resources.databricks.PostgresSyncedTable.model_validate_yaml(
        io.StringIO(synced_table_yaml)
    )
    ```

    References
    ----------

    * [Lakebase synced tables](https://docs.databricks.com/aws/en/oltp/sync-data/sync-table)
    * [Terraform databricks_postgres_synced_table](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_synced_table)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.synced_table_id
