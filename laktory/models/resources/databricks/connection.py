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

    grant: ConnectionGrant | list[ConnectionGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[ConnectionGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Connection — including those
    set outside Laktory — with only the entries listed here. Use only when Laktory owns all access management for
    this resource. Mutually exclusive with `grant`.
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
            object={
                "foreign_connection": f"${{resources.{self.resource_name}.full_name}}"
            }
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants"]
