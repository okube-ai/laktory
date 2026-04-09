from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class AppGitRepository(BaseModel):
    """App Git Repository settings"""

    provider: str = Field(..., description="")
    url: str = Field(..., description="")


# class AppProviderConfig(BaseModel):
#     """App Provider Configuration"""
#     workspace_id: str = Field(..., description="")


class AppResourceDatabase(BaseModel):
    """App Resource Database Configuration"""

    database_name: str = Field(..., description="The name of database.")
    instance_name: str = Field(..., description="The name of database instance.")
    permission: str = Field(
        ...,
        description="Permission to grant on database. Supported permissions are: CAN_CONNECT_AND_CREATE.",
    )


class AppResourceExperiment(BaseModel):
    """App Resource Experiment Configuration"""

    experiment_id: str = Field(..., description="")
    permission: str = Field(..., description="")


class AppResourceGenieSpace(BaseModel):
    """App Resource Genie Space Configuration"""

    name: str = Field(..., description="The name of Genie Space.")
    permission: str = Field(..., description="")
    space_id: str = Field(..., description="The unique ID of Genie Space.")


class AppResourceJob(BaseModel):
    """App Resource Job Configuration"""

    id: str = Field(..., description="Id of the job to grant permission on.")
    permission: str = Field(
        ...,
        description="Permissions to grant on the Job. Supported permissions are: `CAN_MANAGE`, `IS_OWNER`, `CAN_MANAGE_RUN`, `CAN_VIEW`.",
    )


class AppResourceSecret(BaseModel):
    """App Resource Secret Configuration"""

    key: str = Field(..., description="Key of the secret to grant permission on.")
    permission: str = Field(
        ...,
        description="Permission to grant on the secret scope. For secrets, only one permission is allowed. Permission must be one of: `READ`, `WRITE`, `MANAGE`.",
    )
    scope: str = Field(..., description="Scope of the secret to grant permission on.")


class AppResourceServingEndpoint(BaseModel):
    """App Resource Serving Endpoint Configuration"""

    name: str = Field(
        ..., description="Name of the serving endpoint to grant permission on."
    )
    permission: str = Field(
        ...,
        description="Permission to grant on the serving endpoint. Supported permissions are: `CAN_MANAGE`, `CAN_QUERY`, `CAN_VIEW`.",
    )


class AppResourceSqlWarehouse(BaseModel):
    """App Resource SQL Warehouse Configuration"""

    id: str = Field(..., description="Id of the SQL warehouse to grant permission on.")
    permission: str = Field(
        ...,
        description="Permission to grant on the SQL warehouse. Supported permissions are: `CAN_MANAGE`, `CAN_USE`, `IS_OWNER`.",
    )


class AppResourceUcSecurable(BaseModel):
    """App Resource Unity Catalog Securable Configuration"""

    permission: str = Field(
        ...,
        description="Permissions to grant on UC securable, i.e. `READ_VOLUME`, `WRITE_VOLUME`.",
    )
    securable_full_name: str = Field(
        ...,
        description="the full name of UC securable, i.e. `my-catalog.my-schema.my-volume`.",
    )
    securable_type: str = Field(
        ..., description="the type of UC securable, i.e. `VOLUME`."
    )
    securable_kind: str = Field(None, description="")


class AppResource(BaseModel):
    """App Resource Configuration"""

    name: str = Field(..., description="The name of the resource.")
    # app: AppResourceApp = Field(None, description="")
    database: AppResourceDatabase = Field(None, description="")
    description: str = Field(
        None,
        description="The description of the resource. Exactly one of the following attributes must be provided.",
    )
    experiment: AppResourceExperiment = Field(None, description="")
    genie_space: AppResourceGenieSpace = Field(None, description="attribute")
    job: AppResourceJob = Field(None, description="attribute")
    secret: AppResourceSecret = Field(None, description="attribute")
    serving_endpoint: AppResourceServingEndpoint = Field(None, description="attribute")
    sql_warehouse: AppResourceSqlWarehouse = Field(None, description="attribute")
    uc_securable: AppResourceUcSecurable = Field(
        None,
        description="attribute (see the API docs for full list of supported UC objects)",
    )


class App(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks App

    Examples
    --------
    ```py
    import io

    from laktory import models

    # Define App
    job_yaml = '''
    name: stocks-dash
    description: A dashboard app for visualizing stock prices.
    resources:
    - name: sql-warehouse
      sql_warehouse:
        id: warehouse_id
        permission: CAN_USE
    access_controls:
        - group_name: account users
          permission_level: CAN_USE
    '''
    app = models.resources.databricks.App.model_validate_yaml(io.StringIO(job_yaml))
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    budget_policy_id: str = Field(
        None, description="The Budget Policy ID set for this resource."
    )
    compute_size: str = Field(
        None,
        description="A string specifying compute size for the App. Possible values are `MEDIUM`, `LARGE`.",
    )

    description: str = Field(None, description="The description of the app.")

    git_repository: AppGitRepository = Field(None, description="")
    name: str = Field(
        None,
        description="The name of the app. The name must contain only lowercase alphanumeric characters and hyphens. It must be unique within the workspace.",
    )
    name_prefix: str = Field(None, description="Prefix added to the app name")
    name_suffix: str = Field(None, description="Suffix added to the app name")

    no_compute: bool = Field(None, description="")
    # provider_config: AppProviderConfig = Field(None, description="")
    resources: list[AppResource] = Field(
        None, description="A list of resources that the app have access to."
    )

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
                    app_name=f"${{resources.{self.resource_name}.name}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:App"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "name_prefix",
            "name_suffix",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def singularizations(self) -> dict[str, str]:
        return {
            "resources": "resources",
        }

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_app"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
