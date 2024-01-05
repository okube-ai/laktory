import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.permission import Permission
from laktory.models.databricks.permissions import Permissions


class SqlQuery(BaseModel, PulumiResource):
    """
    Databricks SQL Query

    Attributes
    ----------
    comment:
        General description that conveys additional information about this query such as usage notes.
    data_source_id:
        Data source ID of the SQL warehouse that will be used to run the query. Mutually exclusive with `warehouse_id`
    name:
        The title of this query that appears in list views, widget headings, and on the query page.
    parent:
        Id of the workspace folder storing the query
    permissions:
        List of permissions specifications
    query:
        The text of the query to be run.
    run_as_role:
        Run as role.
    tags:
        List of tags
    warehouse_id:
        ID of the SQL warehouse that will be used to run the query. Mutually exclusive with `data_source_id`.

    Examples
    --------
    ```py
    from laktory import models

    q = models.SqlQuery(
        name="create-view",
        query="CREATE VIEW google_stock_prices AS SELECT * FROM stock_prices WHERE symbol = 'GOOGL'",
        warehouse_id="09z739ce103q9374",
        parent="folders/2479128258235163",
        permissions=[
            {
                "group_name": "role-engineers",
                "permission_level": "CAN_RUN",
            }
        ],
    )
    ```
    """

    comment: str = None
    data_source_id: str = None
    name: str
    parent: str
    permissions: list[Permission] = []
    query: str
    run_as_role: Literal["viewer", "owner"] = None
    tags: list[str] = []
    warehouse_id: str = None

    @model_validator(mode="after")
    def check_warehouse_id(self) -> Any:
        if self.warehouse_id is None and self.data_source_id is None:
            raise ValueError(
                "One of `warehouse_id` or `data_source_id` must be provided"
            )

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [
                self,
            ]

            # TODO: Figure out how to fetch data source ids
            # if self.data_source_id is None:
            #     d = self.inject_vars(self.model_dump())
            #     warehouse = databricks.SqlEndpoint.get(
            #         f"warehouse-{name}",
            #         id=d["warehouse_id"],
            #         opts=opts,
            #     )
            #     sql_query.data_source_id = "${var._data_source_id}"
            #     sql_query.vars["_data_source_id"] = warehouse.data_source_id

            if self.permissions:
                self.resources_ += [
                    Permissions(
                        resource_name=f"permissions-{self.resource_name}",
                        access_controls=self.permissions,
                        sql_query_id=f"${{resources.{self.resource_name}.id}}",
                    )
                ]

        return self.resources_

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:SqlQuery"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.SqlQuery

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["permissions", "warehouse_id"]

    @property
    def pulumi_renames(self):
        return {"comment": "description"}
