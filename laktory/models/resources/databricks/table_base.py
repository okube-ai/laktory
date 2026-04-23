# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_sql_table
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class TableColumn(BaseModel):
    comment: str | None = Field(None, description="User-supplied free-form text")
    identity: str | None = Field(
        None,
        description="Whether the field is an identity column. Can be `default`, `always`, or unset. It is unset by default",
    )
    name: str = Field(..., description="User-visible name of column")
    nullable: bool | None = Field(
        None, description="Whether field is nullable (Default: `true`)"
    )
    type: str | None = Field(
        None,
        description="Column type spec (with metadata) as SQL text. Not supported for `VIEW` table_type",
    )
    type_json: str | None = Field(None)


class TableBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_sql_table`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    catalog_name: str = Field(
        ...,
        description="Name of parent catalog. Change forces the creation of a new resource",
    )
    name: str = Field(..., description="User-visible name of column")
    schema_name: str = Field(
        ...,
        description="Name of parent Schema relative to parent Catalog. Change forces the creation of a new resource",
    )
    table_type: str = Field(
        ...,
        description="Distinguishes a view vs. managed/external Table. `MANAGED`, `EXTERNAL` or `VIEW`. Change forces the creation of a new resource",
    )
    cluster_id: str | None = Field(
        None,
        description="All table CRUD operations must be executed on a running cluster or SQL warehouse. If a cluster_id is specified, it will be used to execute SQL commands to manage this table. If empty, a cluster will be created automatically with the name `terraform-sql-table`. Conflicts with `warehouse_id`",
    )
    cluster_keys: list[str] | None = Field(
        None,
        description="a subset of columns to liquid cluster the table by. For automatic clustering, set `cluster_keys` to `['AUTO']`. To turn off clustering, set it to `['NONE']`. Conflicts with `partitions`",
    )
    comment: str | None = Field(None, description="User-supplied free-form text")
    data_source_format: str | None = Field(
        None,
        description="External tables are supported in multiple data source formats. The string constants identifying these formats are `DELTA`, `CSV`, `JSON`, `AVRO`, `PARQUET`, `ORC`, and `TEXT`. Change forces the creation of a new resource. Not supported for `MANAGED` tables or `VIEW`",
    )
    options_: dict[str, str] | None = Field(
        None,
        description="Map of user defined table options. Change forces creation of a new resource",
        serialization_alias="options",
        validation_alias=AliasChoices("options", "options_"),
    )
    owner: str | None = Field(
        None, description="User name/group name/sp application_id of the table owner"
    )
    partitions: list[str] | None = Field(
        None,
        description="a subset of columns to partition the table by. Change forces the creation of a new resource. Conflicts with `cluster_keys`",
    )
    properties: dict[str, str] | None = Field(
        None, description="A map of table properties"
    )
    storage_credential_name: str | None = Field(
        None,
        description="For EXTERNAL Tables only: the name of storage credential to use. Change forces the creation of a new resource",
    )
    storage_location: str | None = Field(
        None,
        description="URL of storage location for Table data (required for EXTERNAL Tables).  If the URL contains special characters, such as space, `&`, etc., they should be percent-encoded (space -> `%20`, etc.).  Not supported for `VIEW` or `MANAGED` table_type",
    )
    view_definition: str | None = Field(
        None,
        description="SQL text defining the view (for `table_type == 'VIEW'`). Not supported for `MANAGED` or `EXTERNAL` table_type",
    )
    warehouse_id: str | None = Field(
        None,
        description="All table CRUD operations must be executed on a running cluster or SQL warehouse. If a `warehouse_id` is specified, that SQL warehouse will be used to execute SQL commands to manage this table. Conflicts with `cluster_id`",
    )
    column: list[TableColumn] | None = PluralField(None, plural="columns")

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_sql_table"
