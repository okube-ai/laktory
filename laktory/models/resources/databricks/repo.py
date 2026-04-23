from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.repo_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.repo_base import RepoBase
from laktory.models.resources.pulumiresource import PulumiResource


class RepoSparseCheckout(BaseModel):
    patterns: list[str] = Field(
        ...,
        description="""
    array of paths (directories) that will be used for sparse checkout. List of patterns could be updated in-place. 
    Addition or removal of the `sparse_checkout` configuration block will lead to recreation of the Git folder.
    """,
    )


class Repo(RepoBase, PulumiResource):
    """
    Databricks Repo

    Examples
    --------
    ```py
    from laktory import models

    repo = models.resources.databricks.Repo(
        url="https://github.com/okube-ai/laktory",
        path="/Users/olivier.soucy@okube.ai/laktory-repo",
        branch="main",
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )
    ```

    References
    ----------

    * [Databricks Repo](https://www.databricks.com/product/repos)
    * [Pulumi Databricks Repo](https://www.pulumi.com/registry/packages/databricks/api-docs/repo)

    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    repo_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Repo"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
