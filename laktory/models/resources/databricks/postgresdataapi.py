from pydantic import Field

from laktory.models.resources.databricks._postgrespath import postgres_path_key
from laktory.models.resources.databricks.postgresdataapi_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresdataapi_base import PostgresDataApiBase


class PostgresDataApi(PostgresDataApiBase):
    """
    Lakebase Autoscaling (Postgres) Data API

    REST API over the schemas of a database. `spec` must set at least one
    field on create, typically `db_schemas` (the schemas must exist). Can also
    be declared as the `data_api` of a `PostgresDatabase`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    api_yaml = '''
    parent: projects/app/branches/production/databases/app-db
    spec:
      db_schemas:
      - public
      db_max_rows: 1000
    '''
    api = models.resources.databricks.PostgresDataApi.model_validate_yaml(
        io.StringIO(api_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_data_api](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_data_api)
    """

    # Relax field the base marks required but Laktory fills from the parent database
    parent: str = Field(
        None,
        description="Database exposed by the API (`projects/{project_id}/branches/{branch_id}/databases/{database_id}`). Set automatically when declared as the data_api of a database.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return postgres_path_key(self.parent)
