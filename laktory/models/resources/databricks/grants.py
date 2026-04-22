from typing import Union

from laktory.models.resources.databricks.grants_base import GrantsBase
from laktory.models.resources.pulumiresource import PulumiResource


class Grants(GrantsBase, PulumiResource):
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
    # Pulumi Methods                                                          #
    # ----------------------------------------------------------------------- #
    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Grants"

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
