from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


# class Constraint(BaseModel, PipelineChild):
#     check: str = None
#     not_null: bool = None


class ColumnMetadata(BaseModel):
    comment: str | None = Field(None, description="Column description")
    # constraints: Constraint = None
    tags: dict[str, str | None] = Field({}, description="Column tags")

    def execute(self, column, table, spark, dtypes=None):
        from laktory.models.datasinks.unitycatalogdatasink import UnityCatalogDataSink

        table_id = table.full_name
        id = f"{table_id}.{column}"
        is_uc = isinstance(table, UnityCatalogDataSink)

        # Comment
        if "comment" in self.model_fields_set:
            logger.info(f"Setting column '{id}' comment to '{self.comment}'")
            if is_uc:
                if self.comment:
                    spark.sql(f"COMMENT ON COLUMN {id} IS '{self.comment}'")
                else:
                    spark.sql(f"COMMENT ON COLUMN {id} IS NULL")
            else:
                if self.comment:
                    spark.sql(
                        f"ALTER TABLE {table_id} CHANGE COLUMN {column} {column} {dtypes[column]} COMMENT '{self.comment}'"
                    )
                else:
                    spark.sql(
                        f"ALTER TABLE {table_id} CHANGE COLUMN {column} {column} {dtypes[column]}"
                    )

        # Tags
        if self.tags:
            if is_uc:
                for k, v in self.tags.items():
                    logger.info(f"Setting column '{id}' tag `{k}` to '{v}'")
                    if v is not None:
                        spark.sql(f"SET TAG ON COLUMN {id} `{k}` = `{v}`")
                    else:
                        spark.sql(f"UNSET TAG ON COLUMN {id} `{k}`")
            else:
                raise ValueError(f"Tags are not supported for {type(table)}")


class TableDataSinkMetadata(BaseModel, PipelineChild):
    columns: dict[str, ColumnMetadata] | None = Field(
        {}, description="Columns Metadata."
    )
    comment: str | None = Field(None, description="Table description")
    # options: dict[str, list[str]] | None = Field({}, description="Table options.")
    owner: str = None
    properties: dict[str, str | None] | None = Field(
        {}, description="Table properties."
    )
    tags: dict[str, str | None] | None = Field({}, description="Table tags")

    def execute(self):
        from laktory import get_spark_session
        from laktory.models.datasinks.unitycatalogdatasink import UnityCatalogDataSink

        spark = get_spark_session()

        table = self.parent
        is_uc = isinstance(table, UnityCatalogDataSink)
        id = table.full_name

        # Comment
        if "comment" in self.model_fields_set:
            logger.info(f"Setting table '{id}' comment to '{self.comment}'")
            if self.comment:
                spark.sql(f"COMMENT ON TABLE {id} IS '{self.comment}'")
            else:
                spark.sql(f"COMMENT ON TABLE {id} IS NULL")

        # Columns
        if self.columns:
            dtypes = {}
            if not is_uc:
                dtypes = (
                    spark.sql(f"DESCRIBE TABLE {id}")
                    .toPandas()
                    .set_index("col_name")["data_type"]
                    .to_dict()
                )

            for column, _meta in self.columns.items():
                _meta.execute(column=column, table=table, spark=spark, dtypes=dtypes)

        # Owner
        if self.owner:
            logger.info(f"Setting table '{id}' owner to '{self.comment}'")
            spark.sql("ALTER TABLE default.df SET OWNER TO `okube`")

        # Options
        # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties#options
        "A table option is a key-value pair which you can initialize when you perform a CREATE TABLE. You cannot SET or UNSET a table option."

        # Properties
        if self.properties:
            props = []
            for k, v in self.properties.items():
                if v is not None:
                    props += [f"{k} = {v}"]
            if props:
                props_string = ",".join(props)
                logger.info(f"Setting table '{id}' properties to ({props_string})")
                spark.sql(f"ALTER TABLE {id} SET TBLPROPERTIES({props_string});")

            props = []
            for k, v in self.properties.items():
                if v is None:
                    props += [k]
            if props:
                props_string = ",".join(props)
                logger.info(f"Unsetting table '{id}' properties ({props_string})")
                spark.sql(
                    f"ALTER TABLE {id} UNSET TBLPROPERTIES IF EXISTS ({props_string});"
                )

        # Tags
        if self.tags:
            if is_uc:
                for k, v in self.tags.items():
                    logger.info(f"Setting table '{id}' tag `{k}` to '{v}'")
                    if v is not None:
                        spark.sql(f"SET TAG ON TABLE {id} `{k}` = `{v}`")
                    else:
                        spark.sql(f"UNSET TAG ON TABLE {id} `{k}`")
            else:
                raise ValueError(f"Tags are not supported for {type(table)}")
