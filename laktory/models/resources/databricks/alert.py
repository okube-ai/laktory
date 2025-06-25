from pathlib import Path
from typing import Any
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class AlertConditionThresholdValue(BaseModel):
    bool_value: bool = Field(
        None,
        description="boolean value (`true` or `false`) to compare against boolean results.",
    )
    double_value: float = Field(
        None, description="double value to compare against integer and double results."
    )
    string_value: str = Field(
        None, description="string value to compare against string results."
    )


class AlertConditionThreshold(BaseModel):
    value: AlertConditionThresholdValue = Field(
        ...,
        description="Actual value used in comparison (one of the attributes is required)",
    )


class AlertConditionOperandColumn(BaseModel):
    name: str = Field(..., description="Name of the column")


class AlertConditionOperand(BaseModel):
    column: AlertConditionOperandColumn = Field(
        ...,
        description="""
    Block describing the column from the query result to use for comparison in alert evaluation
    """,
    )


class AlertCondition(BaseModel):
    op: str = Field(
        ...,
        description="""
    Operator used to compare in alert evaluation. (Enum: `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, 
    `LESS_THAN_OR_EQUAL`, `EQUAL`, `NOT_EQUAL`, `IS_NULL`)
    """,
    )
    operand: AlertConditionOperand = Field(
        ...,
        description="Name of the column from the query result to use for comparison in alert evaluation.",
    )
    empty_result_state: str = Field(
        None,
        description="Alert state if the result is empty (`UNKNOWN`, `OK`, `TRIGGERED`)",
    )
    threshold: AlertConditionThreshold = Field(
        ..., description="Threshold value used for comparison in alert evaluation:"
    )


class Alert(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks SQL Query

    Examples
    --------
    ```py
    from laktory import models

    alert = models.resources.databricks.Alert(
        display_name="My Alert",
        parent_path="/alerts",
        query_id="1",
        condition={
            "op": "GREATER_THAN",
            "operand": {"column": {"name": "value"}},
            "threshold": {"value": {"double_value": 42.0}},
        },
    )
    ```
    """

    access_controls: list[AccessControl] = Field(
        [], description="SQL Alert access controls"
    )
    condition: AlertCondition = Field(
        ..., description="Trigger conditions of the alert."
    )
    custom_body: str = Field(
        None,
        description="""
    Custom body of alert notification, if it exists. See [Alerts API reference](https://docs.databricks.com/sql/user/alerts/index.html)
        for custom templating instructions.
    """,
    )
    custom_subject: str = Field(
        None,
        description="""
    Custom subject of alert notification, if it exists. This includes email subject, Slack notification header, etc.
    See [Alerts API reference](https://docs.databricks.com/sql/user/alerts/index.html) for custom templating
    instructions.
    """,
    )
    dirpath: str = Field(
        None,
        description="Workspace directory inside rootpath in which the alert is deployed. Used only if `parent_path` is not specified.",
    )
    display_name: str = Field(..., description="Name of the alert.")
    name_prefix: str = Field(None, description="Prefix added to the alert display name")
    name_suffix: str = Field(None, description="Suffix added to the alert display name")
    notify_on_ok: bool = Field(
        None,
        description="Whether to notify alert subscribers when alert returns back to normal.",
    )
    owner_user_name: str = Field(None, description="Alert owner's username.")
    parent_path: str = Field(
        None,
        description="""
    The path to a workspace folder containing the alert. Set to `None` to use user's home folder. Overwrite `rootpath` 
    and `dirpath`. If changed, the alert will be recreated.
    """,
    )
    query_id: str = Field(
        None,
        description="ID of the query evaluated by the alert. Mutually exclusive with `query`.",
    )
    rootpath: str = Field(
        None,
        description="""
    Root directory to which all alerts are deployed to. Can also be configured by settings 
    LAKTORY_WORKSPACE_LAKTORY_ROOT environment variable. Default is `/.laktory/`. Used only if `parent_path` is not
    specified.
    """,
    )
    seconds_to_retrigger: int = Field(
        None,
        description="""
    Number of seconds an alert must wait after being triggered to rearm itself. After rearming, it can be triggered 
    again. If 0 or not specified, the alert will not be triggered again.
    """,
    )

    @model_validator(mode="after")
    def set_paths(self) -> Any:
        # Parent Path explicitly set
        if "parent_path" in self.model_fields_set:
            return self

        # root
        if self.rootpath is None:
            self.rootpath = settings.workspace_laktory_root

        # dir
        if self.dirpath is None:
            self.dirpath = ""
        if self.dirpath.startswith("/"):
            self.dirpath = self.dirpath[1:]

        # parent_path
        _path = Path(self.rootpath) / self.dirpath
        self.parent_path = _path.as_posix()

        return self

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.display_name = self.name_prefix + self.display_name
                self.name_prefix = ""
            if self.name_suffix:
                self.display_name = self.display_name + self.name_suffix
                self.name_suffix = ""
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.display_name.replace(" ", "_")

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
                    sql_alert_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Alert"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "dirpath", "rootpath", "name_prefix", "name_suffix"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_alert"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
