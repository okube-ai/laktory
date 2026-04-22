from pathlib import Path
from typing import Any
from typing import Union

from pydantic import AliasChoices
from pydantic import Field
from pydantic import computed_field
from pydantic import model_validator

from laktory._settings import settings
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.query_base import QueryBase
from laktory.models.resources.pulumiresource import PulumiResource


class Query(QueryBase, PulumiResource):
    """
    Databricks Query

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

    access_controls: list[AccessControl] = Field([], description="Access controls list")
    name_prefix: str = Field(None, description="")
    name_suffix: str = Field(None, description="")
    parent_path_: str | None = Field(
        None,
        description="""
        The path to a workspace folder (inside laktory root) containing the query. If changed, the query will be
        recreated.
        """,
        validation_alias=AliasChoices("parent_path", "dirpath", "parent_path_"),
        exclude=True,
    )

    @computed_field(description="parent_path")
    @property
    def parent_path(self) -> str:
        if self.parent_path_ is None:
            self.parent_path_ = ""
        if self.parent_path_.startswith("/"):
            self.parent_path_ = self.parent_path_[1:]

        parent_path = Path(settings.workspace_root) / self.parent_path_
        return parent_path.as_posix()

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
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
            "name_prefix",
            "name_suffix",
        ]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes

    @property
    def terraform_renames(self) -> dict[str, str]:
        return self.pulumi_renames
