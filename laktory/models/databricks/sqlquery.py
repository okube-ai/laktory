import os
from typing import Any
from typing import Literal
from typing import Union
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.databricks.accesscontrol import AccessControl
from laktory.models.databricks.permissions import Permissions


class SqlQuery(BaseModel, PulumiResource):
    """
    Databricks SQL Query

    Attributes
    ----------
    access_controls:
        SQL Query access controls
    comment:
        General description that conveys additional information about this query such as usage notes.
    data_source_id:
        Data source ID of the SQL warehouse that will be used to run the query.
    name:
        The title of this query that appears in list views, widget headings, and on the query page.
    parent:
        Id of the workspace folder storing the query
    query:
        The text of the query to be run.
    run_as_role:
        Run as role.
    tags:
        List of tags

    Examples
    --------
    ```py
    from laktory import models

    q = models.SqlQuery(
        name="create-view",
        query="CREATE VIEW google_stock_prices AS SELECT * FROM stock_prices WHERE symbol = 'GOOGL'",
        data_source_id="09z739ce103q9374",
        parent="folders/2479128258235163",
        access_controls=[
            {
                "group_name": "role-engineers",
                "permission_level": "CAN_RUN",
            }
        ],
    )
    ```
    """

    access_controls: list[AccessControl] = []
    comment: str = None
    data_source_id: str
    name: str
    parent: str
    query: str
    run_as_role: Literal["viewer", "owner"] = None
    tags: list[str] = []

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [
                self,
            ]

            if self.access_controls:
                self.resources_ += [
                    Permissions(
                        resource_name=f"permissions-{self.resource_name}",
                        access_controls=self.access_controls,
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
        return ["access_controls", "warehouse_id"]

    @property
    def pulumi_renames(self):
        return {"comment": "description"}
