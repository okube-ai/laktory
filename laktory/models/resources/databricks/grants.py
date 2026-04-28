from typing import Union

from laktory.models.resources.databricks.grants_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.grants_base import GrantsBase


class Grants(GrantsBase):
    """
    Databricks Grants

    Authoritative for all principals. Sets the grants of a securable and replaces any
    existing grants defined inside or outside of Laktory.

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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return []
