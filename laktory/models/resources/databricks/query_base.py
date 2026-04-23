# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_query
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class QueryParameterDateRangeValueDateRangeValue(BaseModel):
    end: str = Field(...)
    start: str = Field(...)


class QueryParameterDateRangeValue(BaseModel):
    dynamic_date_range_value: str | None = Field(
        None,
        description="(String) Dynamic date-time range value based on current date-time.  Possible values are `TODAY`, `YESTERDAY`, `THIS_WEEK`, `THIS_MONTH`, `THIS_YEAR`, `LAST_WEEK`, `LAST_MONTH`, `LAST_YEAR`, `LAST_HOUR`, `LAST_8_HOURS`, `LAST_24_HOURS`, `LAST_7_DAYS`, `LAST_14_DAYS`, `LAST_30_DAYS`, `LAST_60_DAYS`, `LAST_90_DAYS`, `LAST_12_MONTHS`",
    )
    precision: str | None = Field(
        None,
        description="Date-time precision to format the value into when the query is run.  Possible values are `DAY_PRECISION`, `MINUTE_PRECISION`, `SECOND_PRECISION`.  Defaults to `DAY_PRECISION` (`YYYY-MM-DD`)",
    )
    start_day_of_week: float | None = Field(
        None, description="Specify what day that starts the week"
    )
    date_range_value: QueryParameterDateRangeValueDateRangeValue | None = Field(
        None,
        description="(Block) Manually specified date-time range value.  Consists of the following attributes: * `start` (Required, String) - begin of the date range. * `end` (Required, String) - end of the date range",
    )


class QueryParameterDateValue(BaseModel):
    date_value: str | None = Field(
        None, description="(String) Manually specified date-time value"
    )
    dynamic_date_value: str | None = Field(
        None,
        description="(String) Dynamic date-time value based on current date-time.  Possible values are `NOW`, `YESTERDAY`",
    )
    precision: str | None = Field(
        None,
        description="Date-time precision to format the value into when the query is run.  Possible values are `DAY_PRECISION`, `MINUTE_PRECISION`, `SECOND_PRECISION`.  Defaults to `DAY_PRECISION` (`YYYY-MM-DD`)",
    )


class QueryParameterEnumValueMultiValuesOptions(BaseModel):
    prefix: str | None = Field(
        None, description="Character that prefixes each selected parameter value"
    )
    separator: str | None = Field(
        None,
        description="Character that separates each selected parameter value. Defaults to a comma",
    )
    suffix: str | None = Field(
        None, description="Character that suffixes each selected parameter value"
    )


class QueryParameterEnumValue(BaseModel):
    enum_options: str | None = Field(
        None,
        description="(String) List of valid query parameter values, newline delimited",
    )
    values: list[str] | None = Field(
        None, description="(Array of strings) List of selected query parameter values"
    )
    multi_values_options: QueryParameterEnumValueMultiValuesOptions | None = Field(
        None,
        description="If specified, allows multiple values to be selected for this parameter. Consists of following attributes:",
    )


class QueryParameterNumericValue(BaseModel):
    value: float = Field(..., description="- actual numeric value")


class QueryParameterQueryBackedValueMultiValuesOptions(BaseModel):
    prefix: str | None = Field(
        None, description="Character that prefixes each selected parameter value"
    )
    separator: str | None = Field(
        None,
        description="Character that separates each selected parameter value. Defaults to a comma",
    )
    suffix: str | None = Field(
        None, description="Character that suffixes each selected parameter value"
    )


class QueryParameterQueryBackedValue(BaseModel):
    query_id: str = Field(
        ..., description="ID of the query that provides the parameter values"
    )
    values: list[str] | None = Field(
        None, description="(Array of strings) List of selected query parameter values"
    )
    multi_values_options: QueryParameterQueryBackedValueMultiValuesOptions | None = (
        Field(
            None,
            description="If specified, allows multiple values to be selected for this parameter. Consists of following attributes:",
        )
    )


class QueryParameterTextValue(BaseModel):
    value: str = Field(..., description="- actual numeric value")


class QueryParameter(BaseModel):
    name: str = Field(
        ...,
        description="Literal parameter marker that appears between double curly braces in the query text",
    )
    title: str | None = Field(
        None, description="Text displayed in the user-facing parameter widget in the UI"
    )
    date_range_value: QueryParameterDateRangeValue | None = Field(
        None,
        description="(Block) Manually specified date-time range value.  Consists of the following attributes: * `start` (Required, String) - begin of the date range. * `end` (Required, String) - end of the date range",
    )
    date_value: QueryParameterDateValue | None = Field(
        None, description="(String) Manually specified date-time value"
    )
    enum_value: QueryParameterEnumValue | None = Field(
        None,
        description="(Block) Dropdown parameter value. Consists of following attributes:",
    )
    numeric_value: QueryParameterNumericValue | None = Field(
        None,
        description="(Block) Numeric parameter value. Consists of following attributes:",
    )
    query_backed_value: QueryParameterQueryBackedValue | None = Field(
        None,
        description="(Block) Query-based dropdown parameter value. Consists of following attributes:",
    )
    text_value: QueryParameterTextValue | None = Field(
        None,
        description="(Block) Text parameter value. Consists of following attributes:",
    )


class QueryBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_query`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    display_name: str = Field(..., description="Name of the query")
    query_text: str = Field(..., description="Text of SQL query")
    warehouse_id: str = Field(
        ...,
        description="ID of a SQL warehouse which will be used to execute this query",
    )
    apply_auto_limit: bool | None = Field(
        None, description="Whether to apply a 1000 row limit to the query result"
    )
    catalog: str | None = Field(
        None, description="Name of the catalog where this query will be executed"
    )
    description: str | None = Field(
        None,
        description="General description that conveys additional information about this query such as usage notes",
    )
    owner_user_name: str | None = Field(None, description="Query owner's username")
    run_as_mode: str | None = Field(
        None,
        description="Sets the 'Run as' role for the object.  Should be one of `OWNER`, `VIEWER`",
    )
    schema_: str | None = Field(
        None,
        description="Name of the schema where this query will be executed",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    tags: list[str] | None = Field(
        None, description="Tags that will be added to the query"
    )
    parameter: list[QueryParameter] | None = PluralField(
        None,
        plural="parameters",
        description="Query parameter definition.  Consists of following attributes (one of `*_value` is required):",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_query"
