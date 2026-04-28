from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.terraformresource import TerraformResource


class CurrentUserLookup(ResourceLookup):
    pass


class CurrentUser(BaseModel, TerraformResource):
    """
    Databricks Current User data source. Returns the identity of the
    authenticated user or service principal making API calls.

    Examples
    --------
    ```py
    import io

    from laktory import models

    current_user = models.resources.databricks.CurrentUser.model_validate_yaml(
        io.StringIO("{}")
    )
    ```

    References
    ----------

    * [Databricks Current User](https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/current_user)
    """

    pass

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_current_user"
