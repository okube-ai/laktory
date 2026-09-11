# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_git_credential
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class GitCredentialBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_git_credential`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    git_provider: str = Field(
        ...,
        description="case insensitive name of the Git provider.  Following values are supported right now (could be a subject for a change, consult [Git Credentials API documentation](https://docs.databricks.com/dev-tools/api/latest/gitcredentials.html)): `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`, `awsCodeCommit`, `azureDevOpsServicesAad`",
    )
    force: bool | None = Field(
        None,
        description="specify if settings need to be enforced (i.e., to overwrite previously set credential for service principals)",
    )
    git_email: str | None = Field(
        None,
        description="The email associated with your Git provider user account. Used for authentication with the remote repository and also sets the author & committer identity for commits",
    )
    git_username: str | None = Field(
        None,
        description="user name at Git provider.  For most Git providers it is only used to set the Git committer & author names for commits, however it may be required for authentication depending on your Git provider / token requirements",
    )
    is_default_for_provider: bool | None = Field(
        None,
        description="boolean flag specifying if the credential is the default for the given provider type",
    )
    name: str | None = Field(
        None,
        description="the name of the git credential, used for identification and ease of lookup",
    )
    personal_access_token: str | None = Field(
        None,
        description="The personal access token used to authenticate to the corresponding Git provider. If value is not provided, it's sourced from the first environment variable of [`GITHUB_TOKEN`](https://registry.terraform.io/providers/integrations/github/latest/docs#oauth--personal-access-token), [`GITLAB_TOKEN`](https://registry.terraform.io/providers/gitlabhq/gitlab/latest/docs#required), or [`AZDO_PERSONAL_ACCESS_TOKEN`](https://registry.terraform.io/providers/microsoft/azuredevops/latest/docs#argument-reference), that has a non-empty value",
    )
    principal_id: int | None = Field(
        None,
        description="The ID of the service principal whose credentials will be managed. Only service principal managers can use this field. When specified, the git credential is created or updated for the given service principal instead of the calling user",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_git_credential"


__all__ = ["GitCredentialBase"]
