from laktory.models.resources.databricks.grants_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.grants_base import GrantsBase


class Grants(GrantsBase):
    """
    Databricks Grants

    Full grant replacement. Sets the complete list of grants for a securable and removes every
    existing grant not listed here - including those set outside Laktory.

    Use this standalone resource when you need authoritative grant management on a securable that
    Laktory does not create. For resources that Laktory creates, prefer the embedded `grants` field
    on the resource itself (e.g., `Catalog.grants`, `Schema.grants`) - it generates the same
    Terraform resource but keeps the grant definition co-located with the resource.

    Warning: because this resource replaces all grants, any access granted through the Databricks
    UI or another tool will be removed on the next Terraform apply.

    Examples
    --------
    ```py
    import io

    from laktory import models

    grants_yaml = '''
    catalog: dev
    grants:
    - principal: metastore-admins
      privileges:
      - CREATE_SCHEMA
    - principal: account users
      privileges:
      - USE_CATALOG
      - USE_SCHEMA
    '''
    grants = models.resources.databricks.Grants.model_validate_yaml(
        io.StringIO(grants_yaml)
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
