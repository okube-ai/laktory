from pydantic import Field

from laktory.models.resources.databricks.postgresendpoint_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresendpoint_base import (
    PostgresEndpointBase,
)


class PostgresEndpoint(PostgresEndpointBase):
    """
    Lakebase Autoscaling (Postgres) Endpoint

    Compute (read-write or read-only) serving a branch, with autoscaling
    limits and scale to zero. A read-write endpoint named `primary` is created
    implicitly with each branch: set `replace_existing: true` to manage it.
    Can also be declared in the `endpoints` of a `PostgresBranch`.

    Lakebase Autoscaling resources don't detect changes made outside
    Terraform, and a value removed from `spec` is kept by the server.

    Examples
    --------
    ```py
    import io

    from laktory import models

    endpoint_yaml = '''
    parent: projects/app/branches/production
    endpoint_id: primary
    replace_existing: true
    spec:
      endpoint_type: ENDPOINT_TYPE_READ_WRITE
      autoscaling_limit_min_cu: 0.5
      autoscaling_limit_max_cu: 4.0
      suspend_timeout_duration: 600s
    '''
    endpoint = models.resources.databricks.PostgresEndpoint.model_validate_yaml(
        io.StringIO(endpoint_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_endpoint](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_endpoint)
    """

    # Relax field the base marks required but Laktory fills from the parent branch
    parent: str = Field(
        None,
        description="Branch of the endpoint (`projects/{project_id}/branches/{branch_id}`). Set automatically when declared in the endpoints of a branch.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}/endpoints/{self.endpoint_id}"
