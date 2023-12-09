import os
from typing import Any
from typing import Literal
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission


class SqlQuery(BaseModel, BaseResource):
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
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def id(self) -> str:
        """Deployed sql query id"""
        if self._resources is None:
            return None
        return self.resources.query.id

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "warehouse_id"]

    @property
    def pulumi_renames(self):
        return {"comment": "description"}

    def deploy_with_pulumi(self, name=None, opts=None):
        """
        Deploy sql query using pulumi.

        Parameters
        ----------
        name:
            Name of the pulumi resource. Default is `{self.resource_name}`
        opts:
            Pulumi resource options

        Returns
        -------
        PulumiSqlQuery:
            Pulumi SQL Query resource
        """
        from laktory.resourcesengines.pulumi.sqlquery import PulumiSqlQuery

        return PulumiSqlQuery(name=name, sql_query=self, opts=opts)


if __name__ == "__main__":
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
