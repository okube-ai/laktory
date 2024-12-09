from pathlib import Path
from typing import Literal
from typing import Union
from typing import Any
from pydantic import Field
from pydantic import AliasChoices
from pydantic import model_validator
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.alert import Alert


class QueryParameterTextValue(BaseModel):
    """
    Attributes
    ----------
    value:
        Actual text value.
    """

    value: str


class QueryParameterQueryBackedValueMultiValuesOptions(BaseModel):
    """
    Attributes
    ----------
    prefix:
        Character that prefixes each selected parameter value.
    separator:
        Character that separates each selected parameter value. Defaults to a
        comma.
    suffix:
        Character that suffixes each selected parameter value.
    """

    prefix: str
    separator: str
    suffix: str


class QueryParameterQueryBackedValue(BaseModel):
    """
    Attributes
    ----------
    query_id:
        ID of the query that provides the parameter values.
    multi_values_options:
        If specified, allows multiple values to be selected for this parameter.
    values:
        List of selected query parameter values.
    """

    query_id: str = None
    multi_values_options: QueryParameterQueryBackedValueMultiValuesOptions = None
    values: list[str] = None


class QueryParameterNumericValue(BaseModel):
    """
    Attributes
    ----------
    value:
        Actual numeric value
    """

    value: float = None


class QueryParameterEnumValueMultiValuesOptions(BaseModel):
    """
    Attributes
    ----------
    prefix:
        Character that prefixes each selected parameter value.
    separator:
        Character that separates each selected parameter value. Defaults to a comma.
    suffix:
        Character that suffixes each selected parameter value.
    """

    prefix: str = None
    separator: str = None
    suffix: str = None


class QueryParameterEnumValue(BaseModel):
    """
    Attributes
    ----------
    enum_options:
        List of valid query parameter values, newline delimited.
    multi_values_options:
        If specified, allows multiple values to be selected for this parameter.
    values:
        List of selected query parameter values.
    """

    enum_options: str = None
    multi_values_options: QueryParameterEnumValueMultiValuesOptions = None
    values: list[str] = None


class QueryParameterDateValue(BaseModel):
    """
    Attributes
    ----------
    date_value:
        Manually specified date-time value
    dynamic_date_value:
        Dynamic date-time value based on current date-time
    precision:
        Date-time precision to format the value into when the query is run.
    """

    date_value: str = None
    dynamic_date_value: Literal["NOW", "YESTERDAY"] = None
    precision: Literal["DAY_PRECISION", "MINUTE_PRECISION", "SECOND_PRECISION"] = None


class QueryParameterDateRangeValueDateRangeValue(BaseModel):
    """
    Attributes
    ----------
    end:
        end of the date range
    start:
        start of the date range
    """

    end: str
    start: str


class QueryParameterDateRangeValue(BaseModel):
    """
    Attributes
    ----------
    date_range_value:
        Manually specified date-time range value
    dynamic_date_range_value:
        Dynamic date-time range value based on current date-time.
    """

    date_range_value: QueryParameterDateRangeValueDateRangeValue = None
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
    ] = None
    precision: str = None
    start_day_of_week: str = None


class QueryParameter(BaseModel):
    """
    Query Parameters Specifications

    Attributes
    ----------

    name:
        Literal parameter marker that appears between double curly braces in
        the query text.
    date_range_value:
        Date-range query parameter value. Consists of following attributes
        (Can only specify one of `dynamic_date_range_value` or
        `date_range_value`)
    date_value:
        Date query parameter value. Consists of following attributes (Can only
        specify one of `dynamic_date_value` or `date_value`)
    enum_value:
        Dropdown parameter value
    numeric_value:
        Numeric parameter value
    query_backed_value:
        Query-based dropdown parameter value
    text_value:
        Text parameter value
    title:
        Text displayed in the user-facing parameter widget in the UI.
    """

    name: str
    date_range_value: QueryParameterDateRangeValue = None
    date_value: QueryParameterDateValue = None
    enum_value: QueryParameterEnumValue = None
    numeric_value: QueryParameterNumericValue = None
    query_backed_value: QueryParameterQueryBackedValue = None
    text_value: QueryParameterTextValue = None
    title: str = None


class Query(BaseModel, PulumiResource, TerraformResource):
    """
    Databricks Query

    Attributes
    ----------
    access_controls:
        Query access controls
    dirpath:
        Workspace directory inside rootpath in which the query is deployed.
        Used only if `parent_path` is not specified.
    display_name:
        Name of the query.
    query_text:
        Text of SQL query.
    warehouse_id:
        ID of a SQL warehouse which will be used to execute this query.
    apply_auto_limit:
        Whether to apply a 1000 row limit to the query result.
    catalog:
        Name of the catalog where this query will be executed.
    description:
        General description that conveys additional information about this
        query such as usage notes.
    owner_user_name:
        Query owner's username.
    parameters:
        Query parameter definition. Consists of following attributes
        (one of `*_value` is required):
    parent_path:
        The path to a workspace folder containing the query. Set to `None`
        to use user's home folder. Overwrite `rootpath` and `dirpath`. If
        changed, the query will be recreated.
    rootpath:
        Root directory to which all queries are deployed to. Can also be
        configured by settings LAKTORY_WORKSPACE_LAKTORY_ROOT environment
        variable. Default is `/.laktory/`. Used only if `parent_path` is not
        specified.
    run_as_mode:
        Sets the "Run as" role for the object.
    schema:
        Name of the schema where this query will be executed.
    tags:
        Tags that will be added to the query.

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

    access_controls: list[AccessControl] = []
    alert: Alert = None
    apply_auto_limit: bool = None
    catalog: str = None
    description: str = None
    dirpath: str = None
    display_name: str
    name_prefix: str = None
    name_suffix: str = None
    owner_user_name: str = None
    parameters: list[QueryParameter] = None
    parent_path: Union[str, None] = None
    query_text: str
    rootpath: str = None
    run_as_mode: str = None
    schema_: str = Field(
        None, validation_alias=AliasChoices("schema", "schema_")
    )  # required not to overwrite BaseModel attribute
    tags: list[str] = None
    warehouse_id: str

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
