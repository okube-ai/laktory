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
    from laktory import models

    grants = models.resources.databricks.Grant(
        catalog="dev",
        principal="metastore-admins",
        privileges=["CREATE_SCHEMA"],
    )
    ```
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
