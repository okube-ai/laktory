from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class RepoSparseCheckout(BaseModel):
    patterns: list[str] = Field(
        ...,
        description="""
    array of paths (directories) that will be used for sparse checkout. List of patterns could be updated in-place. 
    Addition or removal of the `sparse_checkout` configuration block will lead to recreation of the Git folder.
    """,
    )


class Repo(BaseModel, PulumiResource, TerraformResource):
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
    branch: str = Field(
        None,
        description="""
    Name of the branch for initial checkout. If not specified, the default branch of the repository will be used. 
    Conflicts with `tag`. If `branch` is removed, and `tag` isn't specified, then the repository will stay at the
    previously checked out state.
    """,
    )
    commit_hash: str = Field(
        None,
        description="""
    Hash of the HEAD commit at time of the last executed operation. It won't change if you manually perform pull 
    operation via UI or API
    """,
    )
    git_provider: str = Field(
        None,
        description="""
    Case insensitive name of the Git provider. Following values are supported right now (could be a subject for a 
    change, consult Repos API documentation): `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, 
    `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`.
    """,
    )
    path: str = Field(
        None,
        description="""
    Path to put the checked out Git folder. If not specified, then the Git folder will be created in the default 
    location. If the value changes, Git folder is re-created.
    """,
    )
    sparse_checkout: list[RepoSparseCheckout] = Field(
        None, description="Sparse checkout feature in Databricks Git folders"
    )
    tag: str = Field(
        None, description="Name of the tag for initial checkout. Conflicts with branch."
    )
    url: str = Field(
        ...,
        description="The URL of the Git Repository to clone from. If the value changes, Git folder is re-created.",
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
    def singularizations(self) -> dict[str, str]:
        return {
            "libraries": "libraries",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_repo"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
