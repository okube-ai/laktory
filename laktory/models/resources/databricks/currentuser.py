from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.terraformresource import TerraformResource


class CurrentUserLookup(ResourceLookup):
    pass


class CurrentUser(BaseModel, TerraformResource):
    """
    Databricks Current User data source. Returns the identity of the
    authenticated user or service principal making API calls.

    `databricks_current_user` only exists as a Terraform data source (there
    is no corresponding `resource` type to "create" the current user), so
    unlike other resources, `lookup_existing` defaults to always-on and
    doesn't need to be set explicitly.

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

    lookup_existing: CurrentUserLookup = Field(
        default_factory=CurrentUserLookup,
        exclude=True,
        description="Always populated - `databricks_current_user` is a Terraform data source only, so this resource is always looked up rather than created.",
    )

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
