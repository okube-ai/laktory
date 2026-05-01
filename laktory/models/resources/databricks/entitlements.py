from laktory.models.resources.databricks.entitlements_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.entitlements_base import EntitlementsBase


class Entitlements(EntitlementsBase):
    """
    Databricks Entitlements

    This resource allows you to set entitlements to existing users, groups or
    service principals. You must define entitlements of a principal using either
    ``Entitlements`` or directly within one of ``User``, ``Group`` or
    ``ServicePrincipal``. Having entitlements defined in both resources will
    result in non-deterministic behaviour.

    Examples
    --------
    ```py
    import io

    from laktory import models

    entitlements_yaml = '''
    user_id: ${resources.user-john.id}
    allow_cluster_create: true
    allow_instance_pool_create: true
    databricks_sql_access: true
    workspace_access: true
    '''
    entitlements = models.resources.databricks.Entitlements.model_validate_yaml(
        io.StringIO(entitlements_yaml)
    )
    ```

    References
    ----------

    * [Databricks Entitlements](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/entitlements)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        if self.user_id:
            return f"user-{self.user_id}"
        if self.group_id:
            return f"group-{self.group_id}"
        if self.service_principal_id:
            return f"spn-{self.service_principal_id}"
        return "entitlements"

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #
