import os
from typing import Any
from pydantic import model_validator
from laktory.models.basemodel import BaseModel
from laktory.models.baseresource import BaseResource
from laktory.models.databricks.permission import Permission


class SqlQuery(BaseModel, BaseResource):
    name: str
    data_source_id: str = None
    query: str
    comment: str = None
    parent: str
    run_as_role: str = None
    tags: list[str] = []
    warehouse_id: str = None
    permissions: list[Permission] = []

    @model_validator(mode="after")
    def check_warehouse_id(self) -> Any:
        if self.warehouse_id is None and self.data_source_id is None:
            raise ValueError(
                "One of `warehouse_id` or `data_source_id` must be provided"
            )

        return self

    @property
    def key(self):
        key = self.name
        return key

    # ----------------------------------------------------------------------- #
    # Resources Engine Methods                                                #
    # ----------------------------------------------------------------------- #

    @property
    def id(self):
        if self._resources is None:
            return None
        return self.resources.query.id

    @property
    def pulumi_excludes(self) -> list[str]:
        return ["permissions", "warehouse_id"]

    @property
    def pulumi_renames(self):
        return {"comment": "description"}

    def deploy_with_pulumi(self, name=None, groups=None, opts=None):
        from laktory.resourcesengines.pulumi.sqlquery import PulumiSqlQuery

        return PulumiSqlQuery(name=name, sql_query=self, opts=opts)
