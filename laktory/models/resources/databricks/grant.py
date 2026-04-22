from typing import Union

from laktory.models.resources.databricks.grant_base import GrantBase
from laktory.models.resources.pulumiresource import PulumiResource


class Grant(GrantBase, PulumiResource):
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
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Grant"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return []

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
