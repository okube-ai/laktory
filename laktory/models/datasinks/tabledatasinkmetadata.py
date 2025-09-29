from functools import cached_property

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


# class Constraint(BaseModel, PipelineChild):
#     check: str = None
#     not_null: bool = None


def set_tags(object, full_name, current, new, is_uc):
    from laktory import get_spark_session

    spark = get_spark_session()

    if not is_uc:
        logger.info("Tags are only supported on Unity Catalog. Skipping.")
        return

    # Apply new tags
    for k, v in new.items():
        v0 = current.get(k, None)
        if v != v0:
            logger.info(f"Setting {object} '{full_name}' tag `{k}` to '{v}'")
            if v is None or v0 is not None:
                # Tags can't be overwritten. They need to be unset first.
                spark.sql(f"UNSET TAG ON {object} {full_name} {k}")

            if v is not None:
                spark.sql(f"SET TAG ON {object} {full_name} `{k}` = `{v}`")

    # Remove old tags
    for k in current.keys():
        if k not in new:
            logger.info(f"Unsetting {object} '{full_name}' tag `{k}`")
            spark.sql(f"UNSET TAG ON {object} {full_name} {k}")


class ColumnMetadata(BaseModel):
    name: str = Field(..., description="Column name")
    comment: str | None = Field(None, description="Column description")
    # constraints: Constraint = None
    tags: dict[str, str | None] = Field({}, description="Column tags")
    _type: str | None = None

    def execute(self, current, table_meta):
        from laktory import get_spark_session

        spark = get_spark_session()

        table_full_name = table_meta.table_full_name
        column_full_name = f"{table_full_name}.{self.name}"
        is_uc = table_meta.is_uc
        object_type = "TABLE"
        if table_meta.table.table_type == "VIEW":
            object_type = "VIEW"

        # Comment
        if self.comment != current.comment:
            logger.info(
                f"Setting column '{column_full_name}' comment to '{self.comment}'"
            )
            if is_uc:
                if self.comment:
                    spark.sql(
                        f"COMMENT ON COLUMN {column_full_name} IS '{self.comment}'"
                    )
                else:
                    spark.sql(f"COMMENT ON COLUMN {column_full_name} IS NULL")
            else:
                if object_type == "VIEW":
                    raise ValueError(
                        f"Column comments are not supported for VIEW {type(table_meta.table)}"
                    )
                if self.comment:
                    spark.sql(
                        f"ALTER {object_type} {table_full_name} CHANGE COLUMN {self.name} {self.name} {current._type} COMMENT '{self.comment}'"
                    )
                else:
                    spark.sql(
                        f"ALTER {object_type} {table_full_name} CHANGE COLUMN {self.name} {self.name} {current._type}"
                    )

        # Tags
        set_tags(
            object="COLUMN",
            full_name=column_full_name,
            current=current.tags,
            new=self.tags,
            is_uc=is_uc,
        )


class TableDataSinkMetadata(BaseModel, PipelineChild):
    columns: list[ColumnMetadata] | None = Field([], description="Columns Metadata.")
    comment: str | None = Field(None, description="Table description")
    # options: dict[str, list[str]] | None = Field({}, description="Table options.")
    owner: str | None = None
    properties: dict[str, str] | None = Field({}, description="Table properties.")
    tags: dict[str, str | None] | None = Field({}, description="Table tags")

    @property
    def table(self):
        return self.parent

    @property
    def is_uc(self):
        from laktory.models.datasinks.unitycatalogdatasink import UnityCatalogDataSink

        return isinstance(self.table, UnityCatalogDataSink)

    @property
    def table_full_name(self):
        table = self.table
        if table:
            return self.table.full_name

    @cached_property
    def current(self):
        return self.get_current()

    def execute(self):
        from laktory import get_spark_session

        spark = get_spark_session()

        table = self.parent
        table_full_name = table.full_name
        object_type = "TABLE"
        if table.table_type == "VIEW":
            object_type = "VIEW"

        # Comment
        if self.comment != self.current.comment:
            logger.info(
                f"Setting table '{table_full_name}' comment to '{self.comment}'"
            )
            if self.is_uc:
                if self.comment:
                    spark.sql(f"COMMENT ON TABLE {table_full_name} IS '{self.comment}'")
                else:
                    spark.sql(f"COMMENT ON TABLE {table_full_name} IS NULL")
            else:
                self.properties["comment"] = self.comment

        # Columns
        for current in self.current.columns:
            new_found = False
            for new in self.columns:
                if new.name == current.name:
                    new_found = True
                    break
            if not new_found:
                new = ColumnMetadata(name=current.name)

            new.execute(current, table_meta=self)

        # Owner
        if self.owner and self.owner != self.current.owner:
            logger.info(f"Setting table '{table_full_name}' owner to '{self.owner}'")
            spark.sql(
                f"ALTER {object_type} {table_full_name} SET OWNER TO `{self.owner}`"
            )

        # Options
        # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties#options
        "A table option is a key-value pair which you can initialize when you perform a CREATE TABLE. You cannot SET or UNSET a table option."

        # Properties
        if self.properties:
            # Set new properties
            props = []
            for k, v in self.properties.items():
                props += [f"{k} = '{v}'"]
            if props:
                props_string = ",".join(props)
                logger.info(
                    f"Setting table '{table_full_name}' properties to ({props_string})"
                )
                spark.sql(
                    f"ALTER TABLE {table_full_name} SET TBLPROPERTIES({props_string});"
                )

            # Remove old properties
            props = []
            for k, v in self.current.properties.items():
                if k not in self.properties:
                    props += [k]
            if props:
                props_string = ",".join(props)
                logger.info(
                    f"Unsetting table '{table_full_name}' properties ({props_string})"
                )
                spark.sql(
                    f"ALTER TABLE {table_full_name} UNSET TBLPROPERTIES IF EXISTS ({props_string});"
                )

        # Tags
        set_tags(
            object="TABLE",
            full_name=table_full_name,
            current=self.current.tags,
            new=self.tags,
            is_uc=self.is_uc,
        )

    def get_current(self):
        from laktory import get_spark_session

        logger.info(
            f"Fetching current metadata for table table '{self.table_full_name}'"
        )

        spark = get_spark_session()

        df = (
            spark.sql(f"DESCRIBE EXTENDED {self.table_full_name}")
            .toPandas()
            .set_index("col_name")
        )

        # Column comments
        columns = []
        for col_name, row in df.iterrows():
            if col_name == "":
                break
            comment = row["comment"]
            if comment == "":
                comment = None
            columns += [ColumnMetadata(name=col_name, comment=comment)]
            columns[-1]._type = row["data_type"]

        df.index = df.index.str.lower()
        _meta = df["data_type"].to_dict()

        # Comment
        comment = _meta.get("comment", None)

        # Owner
        owner = _meta.get("owner", None)

        # Properties
        if "properties" in _meta:
            _properties = _meta["properties"]
        elif "table properties" in _meta:
            _properties = _meta["table properties"]
        else:
            _properties = "[]"
        _properties = _properties.replace("]", "").replace("[", "").split(",")
        properties = {}
        for p in _properties:
            if p == "":
                continue
            k, v = p.strip().split("=")
            properties[k] = v

        # Tags
        table_tags = {}
        if self.is_uc:
            catalog_name = self.table.catalog_name
            schema_name = self.table.schema_name
            table_name = self.table.table_name

            where = f"catalog_name = '{catalog_name}' AND schema_name = '{schema_name}' AND table_name = '{table_name}'"

            # Column tags
            df = spark.sql(
                f"SELECT * FROM system.information_schema.column_tags WHERE {where}"
            ).toPandas()
            for _, row in df.iterrows():
                col_name = row["column_name"]
                tag_name = row["tag_name"]
                tag_value = row["tag_value"]
                for col in columns:
                    if col.name == col_name:
                        col.tags[tag_name] = tag_value

            # Table tags
            df = spark.sql(
                f"SELECT * FROM system.information_schema.table_tags WHERE {where}"
            ).toPandas()
            for _, row in df.iterrows():
                tag_name = row["tag_name"]
                tag_value = row["tag_value"]
                table_tags[tag_name] = tag_value

        return TableDataSinkMetadata(
            columns=columns,
            owner=owner,
            comment=comment,
            tags=table_tags,
            properties=properties,
        )
