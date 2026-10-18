from laktory.models.resources.databricks.defaultworkspacebaseenvironment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.defaultworkspacebaseenvironment_base import (
    DefaultWorkspaceBaseEnvironmentBase,
)


class DefaultWorkspaceBaseEnvironment(DefaultWorkspaceBaseEnvironmentBase):
    """
    Databricks default workspace base environment

    Workspace base environments (CPU and/or GPU) applied by default to new
    serverless notebooks. Workspace singleton. Maps to the Terraform
    `databricks_environments_default_workspace_base_environment` resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    environment_yaml = '''
    cpu_workspace_base_environment: ${resources.workspace-base-environment-laktory.name}
    '''
    environment = (
        models.resources.databricks.DefaultWorkspaceBaseEnvironment.model_validate_yaml(
            io.StringIO(environment_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks serverless environments](https://docs.databricks.com/aws/en/compute/serverless/dependencies)
    * [Terraform databricks_environments_default_workspace_base_environment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/environments_default_workspace_base_environment)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Singleton: named after the resource type"""
        return ""
