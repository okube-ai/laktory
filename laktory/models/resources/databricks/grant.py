from laktory.models.resources.databricks.grant_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.grant_base import GrantBase


class Grant(GrantBase):
    """
    Databricks Grant

    Non-destructive per-principal grant. Updates privileges for a single principal without
    affecting any other principals' grants on the same securable.

    Use this standalone resource when you need to manage grants on a securable that Laktory
    does not create (e.g., a pre-existing catalog or an externally managed table). For resources
    that Laktory creates, prefer the embedded `grant` field on the resource itself (e.g.,
    `Catalog.grant`, `Schema.grant`) - it generates the same Terraform resource but keeps the
    grant definition co-located with the resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    grant_yaml = '''
    catalog: dev
    principal: metastore-admins
    privileges:
    - CREATE_SCHEMA
    '''
    grant = models.resources.databricks.Grant.model_validate_yaml(
        io.StringIO(grant_yaml)
    )
    ```

    References
    ----------

    * [Databricks Privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html)
    """

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return []
