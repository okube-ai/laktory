from pydantic import Field

from laktory.models.resources.databricks.postgresrole_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresrole_base import PostgresRoleBase


class PostgresRole(PostgresRoleBase):
    """
    Lakebase Autoscaling (Postgres) Role

    Postgres role of a branch, mapped to a Databricks user, service principal
    or group, or authenticated with a password. An owner role is created
    implicitly with each branch for the identity that created it: set
    `replace_existing: true` to manage it. Removing a managed role from the
    configuration deletes it. `spec.membership_roles` replaces the role
    memberships on every apply. Can also be declared in the `roles` of a
    `PostgresBranch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    role_yaml = '''
    parent: projects/app/branches/production
    role_id: role-engineers
    spec:
      postgres_role: role-engineers
      identity_type: GROUP
      auth_method: LAKEBASE_OAUTH_V1
    '''
    role = models.resources.databricks.PostgresRole.model_validate_yaml(
        io.StringIO(role_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_role](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_role)
    """

    # Relax field the base marks required but Laktory fills from the parent branch
    parent: str = Field(
        None,
        description="Branch of the role (`projects/{project_id}/branches/{branch_id}`). Set automatically when declared in the roles of a branch.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.parent}/roles/{self.role_id}"
