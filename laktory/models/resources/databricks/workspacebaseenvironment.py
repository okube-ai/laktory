from laktory.models.resources.databricks.workspacebaseenvironment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.workspacebaseenvironment_base import (
    WorkspaceBaseEnvironmentBase,
)


class WorkspaceBaseEnvironment(WorkspaceBaseEnvironmentBase):
    """
    Databricks workspace base environment

    Shareable serverless environment (environment version and Python
    dependencies) defined by an environment YAML file stored in the workspace
    or a Unity Catalog volume, e.g. to share the Laktory wheel and its
    dependencies across serverless notebooks and jobs. Maps to the Terraform
    `databricks_environments_workspace_base_environment` resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    environment_yaml = '''
    display_name: laktory
    filepath: /Volumes/dev/sources/environments/laktory.yaml
    base_environment_type: CPU
    '''
    environment = (
        models.resources.databricks.WorkspaceBaseEnvironment.model_validate_yaml(
            io.StringIO(environment_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks serverless environments](https://docs.databricks.com/aws/en/compute/serverless/dependencies)
    * [Terraform databricks_environments_workspace_base_environment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/environments_workspace_base_environment)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name
