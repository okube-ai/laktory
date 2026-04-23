from typing import Any

from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.pipeline_base import PipelineBase
from laktory.models.resources.pulumiresource import PulumiResource


class Pipeline(PipelineBase, PulumiResource):
    """
    Databricks Lakeflow Declarative Pipeline (formerly Delta Live Tables)

    Examples
    --------
    Assuming the configuration yaml file
    ```py
    import io

    from laktory import models

    # Define pipeline
    pipeline_yaml = '''
    name: pl-stock-prices

    catalog: dev
    target: finance

    clusters:
      - name : default
        node_type_id: Standard_DS3_v2
        autoscale:
          min_workers: 1
          max_workers: 2

    libraries:
      - notebook:
          path: /pipelines/dlt_brz_template.py
      - notebook:
          path: /pipelines/dlt_slv_template.py
      - notebook:
          path: /pipelines/dlt_gld_stock_performances.py

    access_controls:
      - group_name: account users
        permission_level: CAN_VIEW
      - group_name: role-engineers
        permission_level: CAN_RUN

    '''
    pipeline = models.resources.databricks.Pipeline.model_validate_yaml(
        io.StringIO(pipeline_yaml)
    )
    ```

    References
    ----------

    * [Databricks Pipeline](https://docs.databricks.com/api/workspace/pipelines/create)
    * [Pulumi Databricks Pipeline](https://www.pulumi.com/registry/packages/databricks/api-docs/pipeline/)
    """

    access_controls: list[AccessControl] = Field(
        [], description="Pipeline access controls"
    )
    name_prefix: str = Field(None, description="Prefix added to the DLT pipeline name")
    name_suffix: str = Field(None, description="Suffix added to the DLT pipeline name")

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.name = self.name_prefix + self.name
                self.name_prefix = ""
            if self.name_suffix:
                self.name = self.name + self.name_suffix
                self.name_suffix = ""
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        dlt
        """
        return "dlt-pipeline"

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
                    pipeline_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Pipeline"

    @property
    def pulumi_excludes(self) -> list[str] | dict[str, bool]:
        return {
            "access_controls": True,
            "name_prefix": True,
            "name_suffix": True,
        }

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return self.pulumi_excludes
