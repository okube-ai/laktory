from pathlib import Path
from typing import Any
from typing import Literal
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import model_validator

from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.alert import Alert
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class QueryParameterTextValue(BaseModel):
    value: str = Field(..., description="Actual text value.")


class QueryParameterQueryBackedValueMultiValuesOptions(BaseModel):
    prefix: str = Field(
        ..., description="Character that prefixes each selected parameter value."
    )
    separator: str = Field(
        ...,
        description="Character that separates each selected parameter value. Defaults to a comma.",
    )
    suffix: str = Field(
        ..., description="Character that suffixes each selected parameter value."
    )


class QueryParameterQueryBackedValue(BaseModel):
    query_id: str = Field(
        None, description="ID of the query that provides the parameter values."
    )
    multi_values_options: QueryParameterQueryBackedValueMultiValuesOptions = Field(
        None,
        description="If specified, allows multiple values to be selected for this parameter.",
    )
    values: list[str] = Field(
        None, description="List of selected query parameter values."
    )


class QueryParameterNumericValue(BaseModel):
    value: float = Field(None, description="Actual numeric value")


class QueryParameterEnumValueMultiValuesOptions(BaseModel):
    prefix: str = Field(
        None, description="Character that prefixes each selected parameter value."
    )
    separator: str = Field(
        None,
        description="Character that separates each selected parameter value. Defaults to a comma.",
    )
    suffix: str = Field(
        None, description="Character that suffixes each selected parameter value."
    )


class QueryParameterEnumValue(BaseModel):
    enum_options: str = Field(
        None, description="List of valid query parameter values, newline delimited."
    )
    multi_values_options: QueryParameterEnumValueMultiValuesOptions = Field(
        None,
        description="If specified, allows multiple values to be selected for this parameter.",
    )
    values: list[str] = Field(
        None, description="List of selected query parameter values."
    )


class QueryParameterDateValue(BaseModel):
    date_value: str = Field(None, description="Manually specified date-time value")
    dynamic_date_value: Literal["NOW", "YESTERDAY"] = Field(
        None, description="Dynamic date-time value based on current date-time"
    )
    precision: Literal["DAY_PRECISION", "MINUTE_PRECISION", "SECOND_PRECISION"] = Field(
        None,
        description="Date-time precision to format the value into when the query is run.",
    )


class QueryParameterDateRangeValueDateRangeValue(BaseModel):
    end: str = Field(..., description="end of the date range")
    start: str = Field(..., description="start of the date range")


class QueryParameterDateRangeValue(BaseModel):
    date_range_value: QueryParameterDateRangeValueDateRangeValue = Field(
        None, description="Manually specified date-time range value"
    )
    dynamic_date_range_value: Literal[
        "TODAY",
        "YESTERDAY",
        "THIS_WEEK",
        "THIS_MONTH",
        "THIS_YEAR",
        "LAST_WEEK",
        "LAST_MONTH",
        "LAST_YEAR",
        "LAST_HOUR",
        "LAST_8_HOURS",
        "LAST_24_HOURS",
        "LAST_7_DAYS",
        "LAST_14_DAYS",
        "LAST_30_DAYS",
        "LAST_60_DAYS",
        "LAST_90_DAYS",
        "LAST_12_MONTHS",
    ] = Field(
        None, description="Dynamic date-time range value based on current date-time."
    )
    precision: str = Field(None, description="")
    start_day_of_week: str = Field(None, description="")


class QueryParameter(BaseModel):
    name: str = Field(
        ...,
        description="Literal parameter marker that appears between double curly braces in the query text.",
    )
    date_range_value: QueryParameterDateRangeValue = Field(
        None,
        description="""
    Date-range query parameter value. Consists of following attributes (Can only specify one of 
    `dynamic_date_range_value` or`date_range_value`)
    """,
    )
    date_value: QueryParameterDateValue = Field(
        None,
        description="""
        Date query parameter value. Consists of following attributes (Can only specify one of 
        `dynamic_date_value` or `date_value`)
        """,
    )
    enum_value: QueryParameterEnumValue = Field(
        None, description="Dropdown parameter value"
    )
    numeric_value: QueryParameterNumericValue = Field(
        None, description="Numeric parameter value"
    )
    query_backed_value: QueryParameterQueryBackedValue = Field(
        None, description="Query-based dropdown parameter value"
    )
    text_value: QueryParameterTextValue = Field(
        None, description="Text parameter value"
    )
    title: str = Field(
        None,
        description="Text displayed in the user-facing parameter widget in the UI.",
    )


class Query(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Query

    Examples
    --------
    ```py
    from laktory import models

    query = models.resources.databricks.Query(
        display_name="google-prices",
        parent_path="/queries",
        query_text="SELECT * FROM dev.finance.slv_stock_prices",
        warehouse_id="12345",
    )
    ```
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    alert: Alert = Field(None, description="")
    apply_auto_limit: bool = Field(
        None, description="Whether to apply a 1000 row limit to the query result."
    )
    catalog: str = Field(
        None, description="Name of the catalog where this query will be executed."
    )
    description: str = Field(
        None,
        description="General description that conveys additional information about this query such as usage notes.",
    )
    dirpath: str = Field(
        None,
        description="""
        Workspace directory inside rootpath in which the query is deployed. Used only if `parent_path` 
        is not specified.
        """,
    )
    display_name: str = Field(..., description="Name of the query.")
    name_prefix: str = Field(None, description="")
    name_suffix: str = Field(None, description="")
    owner_user_name: str = Field(None, description="Query owner's username.")
    parameters: list[QueryParameter] = Field(
        None,
        description="Query parameter definition. Consists of following attributes (one of `*_value` is required):",
    )
    parent_path: Union[str, None] = Field(
        None,
        description="""
    The path to a workspace folder containing the query. Set to `None` to use user's home folder. Overwrite `rootpath` 
    and `dirpath`. If changed, the query will be recreated.""",
    )
    query_text: str = Field(..., description="Text of SQL query.")
    rootpath: str = Field(
        None,
        description="""
    Root directory to which all queries are deployed to. Can also be configured by settings 
    LAKTORY_WORKSPACE_LAKTORY_ROOT environment variable. Default is `/.laktory/`. Used only if `parent_path` is not
    specified.
    """,
    )
    run_as_mode: str = Field(None, description="Sets the 'Run as' role for the object.")
    schema_: str = Field(
        None,
        validation_alias=AliasChoices("schema", "schema_"),
        description="Name of the schema where this query will be executed.",
    )  # required not to overwrite BaseModel attribute
    tags: list[str] = Field(None, description="Tags that will be added to the query.")
    warehouse_id: str = Field(
        ...,
        description="ID of a SQL warehouse which will be used to execute this query.",
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
        return self.display_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        - alert
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    sql_query_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        if self.alert:
            self.alert.query_id = f"${{resources.{self.resource_name}.id}}"
            resources += [self.alert]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_renames(self) -> dict[str, str]:
        return {"schema_": "schema"}

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Query"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "access_controls",
            "alert",
            "dirpath",
            "rootpath",
            "name_prefix",
            "name_suffix",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_query"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
