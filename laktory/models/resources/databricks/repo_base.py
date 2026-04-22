# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_repo
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class RepoSparseCheckout(BaseModel):
    patterns: list[str] = Field(
        ...,
        description="array of paths (directories) that will be used for sparse checkout.  List of patterns could be updated in-place",
    )


class RepoBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_repo`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    url: str = Field(
        ...,
        description="The URL of the Git Repository to clone from. If the value changes, Git folder is re-created",
    )
    branch: str | None = Field(
        None,
        description="name of the branch for initial checkout. If not specified, the default branch of the repository will be used.  Conflicts with `tag`.  If `branch` is removed, and `tag` isn't specified, then the repository will stay at the previously checked out state",
    )
    commit_hash: str | None = Field(
        None,
        description="Hash of the HEAD commit at time of the last executed operation. It won't change if you manually perform pull operation via UI or API",
    )
    git_provider: str | None = Field(
        None,
        description="case insensitive name of the Git provider.  Following values are supported right now (could be a subject for a change, consult [Repos API documentation](https://docs.databricks.com/dev-tools/api/latest/repos.html)): `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`",
    )
    path: str | None = Field(
        None,
        description="path to put the checked out Git folder. If not specified, , then the Git folder will be created in the default location.  If the value changes, Git folder is re-created",
    )
    tag: str | None = Field(
        None,
        description="name of the tag for initial checkout.  Conflicts with `branch`",
    )
    sparse_checkout: RepoSparseCheckout | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_repo"
