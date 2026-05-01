from typing import Union

from pydantic import Field

from laktory.models.grants.connectiongrant import ConnectionGrant
from laktory.models.resources.databricks.connection_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.connection_base import ConnectionBase


class Connection(ConnectionBase):
    """
    Databricks Connection

    Examples
    --------
    ```py
    import io

    from laktory import models

    connection_yaml = '''
    name: my-mysql-connection
    connection_type: MYSQL
    comment: Connection to MySQL database
    options:
      host: mysql.example.com
      port: "3306"
      user: admin
    grants:
    - principal: account users
      privileges:
      - USE_CONNECTION
    '''
    connection = models.resources.databricks.Connection.model_validate_yaml(
        io.StringIO(connection_yaml)
    )
    ```

    References
    ----------

    * [Databricks Connection](https://docs.databricks.com/en/query-federation/index.html)
    """

    grant: Union[ConnectionGrant, list[ConnectionGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Connection and authoritative for a specific principal.
    Other principals within the grants are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[ConnectionGrant] = Field(
        None,
        description="""
    Grants operating on the Connection and authoritative for all principals. Replaces any existing grants
    defined inside or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - connection grants
        """
        resources = []

        resources += self.get_grants_additional_resources(
            object={"foreign_connection": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]
