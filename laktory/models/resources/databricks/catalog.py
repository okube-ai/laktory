from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import model_validator

from laktory.models.grants.cataloggrant import CatalogGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.catalog_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.catalog_base import CatalogBase
from laktory.models.resources.databricks.schema import Schema
from laktory.models.resources.databricks.workspacebinding import WorkspaceBinding


class CatalogLookup(ResourceLookup):
    name: str = Field(serialization_alias="id", description="Catalog name")


class Catalog(CatalogBase):
    """
    A catalog is the first layer of Unity Catalog's three-level namespace. It's
    used to organize your data assets.

    Examples
    --------
    ```py
    from laktory import models

    catalog = models.resources.databricks.Catalog(
        name="dev",
        grants=[
            {"principal": "account users", "privileges": ["USE_CATALOG", "USE_SCHEMA"]}
        ],
        schemas=[
            {
                "name": "engineering",
                "grants": [
                    {"principal": "domain-engineering", "privileges": ["SELECT"]}
                ],
            },
            {
                "name": "sources",
                "volumes": [
                    {
                        "name": "landing",
                        "volume_type": "EXTERNAL",
                        "grants": [
                            {
                                "principal": "account users",
                                "privileges": ["READ_VOLUME"],
                            },
                            {
                                "principal": "role-metastore-admins",
                                "privileges": ["WRITE_VOLUME"],
                            },
                        ],
                    },
                ],
            },
        ],
    )
    ```

    References
    ----------

    * [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#catalogs)
    """

    # Restore required constraint (base has name: str | None = None)
    name: str = Field(..., description="Name of the catalog")

    # Override base defaults
    force_destroy: bool = Field(
        True, description="If `True` catalog can be deleted, even when not empty"
    )
    isolation_mode: Literal["OPEN", "ISOLATED"] = Field(
        "OPEN",
        description="""
    Whether the catalog is accessible from all workspaces or a specific set of workspaces. Can be ISOLATED or OPEN.
    Setting the catalog to ISOLATED will automatically allow access from the current workspace.
    """,
    )

    # foreign_options: Laktory name for the Terraform "options" field.
    # The base has `options_` with serialization_alias="options" for Terraform;
    # "options_" is added to excludes below to prevent double output.
    # TODO: Rename options to resource_options? and rename this one to options?
    foreign_options: dict[str, str] = Field(
        None,
        description="For Foreign Catalogs: the name of the entity from an external data source that maps to a catalog. For example, the database name in a PostgreSQL server.",
    )

    # Laktory-specific
    grant: Union[CatalogGrant, list[CatalogGrant]] = Field(
        None,
        description="""
     Grant(s) operating on the Catalog and authoritative for a specific principal. Other principals within the grants
     are preserved. Mutually exclusive with `grants`.
    """,
    )
    grants: list[CatalogGrant] = Field(
        None,
        description="""
     Grants operating on the Catalog and authoritative for all principals. Replaces any existing grants defined inside
     or outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    lookup_existing: CatalogLookup = Field(
        None,
        exclude=True,
        description="""
    Specifications for looking up existing resource. Other attributes will be ignored.
    """,
    )
    schemas: list[Schema] = Field(
        [], description="List of schemas stored in the catalog"
    )
    workspace_bindings: list[WorkspaceBinding] = Field(
        None,
        description="""
    If you use workspaces to isolate user data access, you may want to limit access from
    specific workspaces in your account, also known as workspace binding.
    """,
    )

    @model_validator(mode="after")
    def assign_name(self):
        for schema in self.schemas:
            schema.catalog_name = self.name
            schema.assign_name()

        if self.workspace_bindings:
            for b in self.workspace_bindings:
                b.securable_type = "catalog"
                b.securable_name = self.name

        return self

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Full name of the catalog `{catalog_name}`"""
        return self.name

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - catalog grants
        - schemas resources
        """
        resources = []

        # Catalog grants
        resources += self.get_grants_additional_resources(
            object={"catalog": f"${{resources.{self.resource_name}.id}}"}
        )
        # Catalog schemas
        if self.schemas:
            for s in self.schemas:
                resources += s.core_resources

        # Workspace Bindings
        if self.workspace_bindings:
            resources += self.workspace_bindings

        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return [
            "schemas",
            "is_unity",
            "grants",
            "grant",
            "workspace_bindings",
            "options_",
        ]

    @property
    def terraform_renames(self) -> dict[str, str]:
        return {"foreign_options": "options"}
