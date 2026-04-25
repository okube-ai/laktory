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
    from laktory import models

    grants = models.resources.databricks.Grants(
        catalog="dev",
        grants=[{"principal": "metastore-admins", "privileges": ["CREATE_SCHEMA"]}],
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
