from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.app_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.app_base import AppBase
from laktory.models.resources.databricks.permissions import Permissions


class AppGitRepository(BaseModel):
    provider: str = Field(..., description="Git provider (e.g. gitHub).")
    url: str = Field(..., description="URL of the Git repository.")


class AppResourceApp(BaseModel):
    name: str | None = Field(None, description="Name of the app resource.")
    permission: str | None = Field(None, description="Permission to grant.")


class AppResourceDatabase(BaseModel):
    database_name: str = Field(..., description="Name of the database.")
    instance_name: str = Field(..., description="Name of the database instance.")
    permission: str = Field(..., description="Permission to grant.")


class AppResourceExperiment(BaseModel):
    experiment_id: str = Field(..., description="ID of the MLflow experiment.")
    permission: str = Field(..., description="Permission to grant.")


class AppResourceGenieSpace(BaseModel):
    name: str = Field(..., description="Name of the Genie space.")
    permission: str = Field(..., description="Permission to grant.")
    space_id: str = Field(..., description="ID of the Genie space.")


class AppResourceJob(BaseModel):
    id: str = Field(..., description="ID of the Databricks job.")
    permission: str = Field(..., description="Permission to grant.")


class AppResourcePostgres(BaseModel):
    branch: str | None = Field(None, description="Branch name.")
    database: str | None = Field(None, description="Database name.")
    permission: str | None = Field(None, description="Permission to grant.")


class AppResourceSecret(BaseModel):
    key: str = Field(..., description="Secret key.")
    permission: str = Field(..., description="Permission to grant.")
    scope: str = Field(..., description="Secret scope.")


class AppResourceServingEndpoint(BaseModel):
    name: str = Field(..., description="Name of the serving endpoint.")
    permission: str = Field(..., description="Permission to grant.")


class AppResourceSqlWarehouse(BaseModel):
    id: str = Field(..., description="ID of the SQL warehouse.")
    permission: str = Field(..., description="Permission to grant.")


class AppResourceUcSecurable(BaseModel):
    permission: str = Field(..., description="Permission to grant.")
    securable_full_name: str = Field(..., description="Full name of the UC securable.")
    securable_type: str = Field(..., description="Type of the UC securable.")


class AppResource(BaseModel):
    name: str = Field(
        ..., description="Name used to refer to this resource inside the app."
    )
    app: AppResourceApp | None = Field(None, description="App resource reference.")
    database: AppResourceDatabase | None = Field(
        None, description="Database resource reference."
    )
    description: str | None = Field(None, description="Description of the resource.")
    experiment: AppResourceExperiment | None = Field(
        None, description="MLflow experiment resource reference."
    )
    genie_space: AppResourceGenieSpace | None = Field(
        None, description="Genie space resource reference."
    )
    job: AppResourceJob | None = Field(
        None, description="Databricks job resource reference."
    )
    postgres: AppResourcePostgres | None = Field(
        None, description="Postgres resource reference."
    )
    secret: AppResourceSecret | None = Field(
        None, description="Secret resource reference."
    )
    serving_endpoint: AppResourceServingEndpoint | None = Field(
        None, description="Serving endpoint resource reference."
    )
    sql_warehouse: AppResourceSqlWarehouse | None = Field(
        None, description="SQL warehouse resource reference."
    )
    uc_securable: AppResourceUcSecurable | None = Field(
        None, description="Unity Catalog securable resource reference."
    )


class AppTelemetryExportDestinationUnityCatalog(BaseModel):
    logs_table: str = Field(
        ..., description="Full name of the Unity Catalog table for logs."
    )
    metrics_table: str = Field(
        ..., description="Full name of the Unity Catalog table for metrics."
    )
    traces_table: str = Field(
        ..., description="Full name of the Unity Catalog table for traces."
    )


class AppTelemetryExportDestination(BaseModel):
    unity_catalog: AppTelemetryExportDestinationUnityCatalog | None = Field(
        None, description="Unity Catalog telemetry destination."
    )


class App(AppBase):
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

    git_repository: AppGitRepository | None = Field(
        None, description="Git repository configuration."
    )
    name_prefix: str = Field(None, description="Prefix added to the app name")
    name_suffix: str = Field(None, description="Suffix added to the app name")
    resources: list[AppResource] | None = Field(
        None, description="A list of resources that the app has access to."
    )
    telemetry_export_destinations: list[AppTelemetryExportDestination] | None = Field(
        None, description="Telemetry export destinations."
    )

    # provider_config: AppProviderConfig = Field(None, description="")

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
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "name_prefix",
            "name_suffix",
        ]
