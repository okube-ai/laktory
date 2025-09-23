from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.pipelinechild import PipelineChild

logger = get_logger(__name__)


class Constraint(BaseModel, PipelineChild):
    check: str = None
    not_null: bool = None


class ColumnMetadata(BaseModel, PipelineChild):
    comment: str = None
    constraints: Constraint = None
    tags: list[str] = None


class TableDataSinkMetadata(BaseModel, PipelineChild):
    columns: dict[str, ColumnMetadata] | None = Field(
        {}, description="Columns Metadata."
    )
    comment: str | None = None
    # options: dict[str, list[str]] | None = Field({}, description="Table options.")
    owner: str = None
    properties: dict[str, str | None] | None = Field(
        {}, description="Table properties."
    )

    def execute(self):
        from laktory import get_spark_session

        spark = get_spark_session()

        table = self.parent
        id = table.full_name

        # Comment
        if "comment" in self.model_fields_set:
            logger.info(f"Setting table '{id}' comment to '{self.comment}'")
            if self.comment:
                spark.sql(f"COMMENT ON TABLE {id} IS '{self.comment}'")
            else:
                spark.sql(f"COMMENT ON TABLE {id} IS NULL")

        # Owner
        if self.owner:
            logger.info(f"Setting table '{id}' owner to '{self.comment}'")
            spark.sql("ALTER TABLE default.df SET OWNER TO `okube`")

        # Options
        "A table option is a key-value pair which you can initialize when you perform a CREATE TABLE. You cannot SET or UNSET a table option."
        # if self.options:
        #     props = []
        #     for k, v in self.options.items():
        #         if v is not None:
        #             values_string = " || ".join(v)
        #             props += [f"{k} = {values_string}"]
        #     if props:
        #         props_string = ",".join(props)
        #         logger.info(f"Setting table '{id}' options to ({props_string})")
        #         spark.sql(f"ALTER TABLE {id} SET OPTIONS({props_string});")

        # props = []
        # for k, v in self.options.items():
        #     if v is None:
        #         props += [k]
        # if props:
        #     props_string = ",".join(props)
        #     logger.info(f"Unsetting table '{id}' options ({props_string})")
        #     spark.sql(f"ALTER TABLE {id} UNSET OPTIONS IF EXISTS ({props_string});")

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
