from pydantic import Field

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.modelserving_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.modelserving_base import ModelServingBase
from laktory.models.resources.databricks.permissions import Permissions


class ModelServing(ModelServingBase):
    """
    Databricks model serving endpoint

    Examples
    --------
    ```py
    import io

    from laktory import models

    model_serving_yaml = '''
    name: my-endpoint
    config:
      served_entities:
        - entity_name: my-catalog.my_schema.my_model
          entity_version: "1"
          workload_size: Small
          scale_to_zero_enabled: true
    access_controls:
    - group_name: role-engineers
      permission_level: CAN_QUERY
    '''
    model_serving = models.resources.databricks.ModelServing.model_validate_yaml(
        io.StringIO(model_serving_yaml)
    )
    ```

    References
    ----------

    * [Databricks Model Serving](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/model_serving)
    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_options={"name": f"permissions-{self.resource_name}"},
                    access_controls=self.access_controls,
                    serving_endpoint_id=f"${{resources.{self.resource_name}.serving_endpoint_id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["access_controls"]
