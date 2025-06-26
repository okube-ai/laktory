from typing import Literal
from typing import Union

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.grants.volumegrant import VolumeGrant
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class Volume(BaseModel, PulumiResource, TerraformResource):
    """
    Volumes are Unity Catalog objects representing a logical volume of storage
    in a cloud object storage location. Volumes provide capabilities for
    accessing, storing, governing, and organizing files. While tables provide
    governance over tabular datasets, volumes add governance over non-tabular
    datasets. You can use volumes to store and access files in any format,
    including structured, semi-structured, and unstructured data.

    Examples
    --------
    ```py
    from laktory import models

    volume = models.resources.databricks.Volume(
        name="landing",
        catalog_name="dev",
        comment="Landing zone for raw data",
        schema_name="sources",
        volume_type="EXTERNAL",
        storage_location="abfss://landing@lakehouse-storage.dfs.core.windows.net/",
        grants=[
            {"principal": "account users", "privileges": ["READ_VOLUME"]},
            {"principal": "role-metastore-admins", "privileges": ["WRITE_VOLUME"]},
        ],
    )
    print(volume.full_name)
    # > dev.sources.landing
    print(volume.parent_full_name)
    # > dev.sources
    ```

    References
    ----------

    * [Databricks Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html)
    * [Pulumi Databricks Volume](https://www.pulumi.com/registry/packages/databricks/api-docs/volume/)
    """

    name: str = Field(..., description="Name of the volume")
    catalog_name: str = Field(
        None, description="Name of the catalog storing the volume"
    )
    comment: str = Field(None, description="Text description of the volume")
    grant: Union[VolumeGrant, list[VolumeGrant]] = Field(
        None,
        description="""
    Grant(s) operating on the Volume and authoritative for a specific principal. Other principals within the grants are 
    preserved. Mutually exclusive with `grants`. 
    """,
    )
    grants: list[VolumeGrant] = Field(
        None,
        description="""
    Grants operating on the Volume and authoritative for all principals. Replaces any existing grants defined inside or
    outside of Laktory. Mutually exclusive with `grant`.
    """,
    )
    schema_name: str = Field(None, description="Name of the schema storing the volume")
    storage_location: str = Field(
        None,
        description="Path inside an External Location. Only used for EXTERNAL Volumes.",
    )
    volume_type: Literal["MANAGED", "EXTERNAL"] = Field(
        "MANAGED",
        description="""
    Type of volume. A managed volume is a Unity Catalog-governed storage volume created within the default storage 
    location of the containing schema. An external volume is a Unity Catalog-governed storage volume registered against
    a directory within an external location.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        """Schema full name `{catalog_name}.{schema_name}`"""
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        return _id

    @property
    def full_name(self) -> str:
        """Volume full name `{catalog_name}.{schema_name}.{volume_name}`"""
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        """Table full name (catalog.schema.volume)"""
        return self.full_name

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - volume grants
        """
        resources = []

        # Volume grants
        resources += self.get_grants_additional_resources(
            object={"volume": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Volume"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grant", "grants"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_volume"

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return self.pulumi_excludes
