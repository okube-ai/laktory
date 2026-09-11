from laktory.models.resources.databricks.gitcredential_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.gitcredential_base import GitCredentialBase


class GitCredential(GitCredentialBase):
    """
    Databricks Git Credential

    Examples
    --------
    ```py
    import io

    from laktory import models

    credential_yaml = '''
    git_provider: gitHub
    git_username: my-user
    personal_access_token: ${vars.git_pat}
    '''
    credential = models.resources.databricks.GitCredential.model_validate_yaml(
        io.StringIO(credential_yaml)
    )
    ```

    References
    ----------

    * [Databricks Git Credential](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/git_credential)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.name or self.git_provider
