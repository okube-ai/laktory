from typing import Union

from laktory.models.resources.databricks.grant_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.grant_base import GrantBase


class Grant(GrantBase):
    """
    Databricks Grant

    Authoritative for a specific principal. Updates the grants of a securable to a
    single principal. Other principals within the grants for the securables are preserved.

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
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return []
