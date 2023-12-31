from typing import Literal
from typing import Union
from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.grants.volumegrant import VolumeGrant
from laktory.models.databricks.grants import Grants


class Volume(BaseModel, PulumiResource):
    """
    Volumes are Unity Catalog objects representing a logical volume of storage
    in a cloud object storage location. Volumes provide capabilities for
    accessing, storing, governing, and organizing files. While tables provide
    governance over tabular datasets, volumes add governance over non-tabular
    datasets. You can use volumes to store and access files in any format,
    including structured, semi-structured, and unstructured data.

    Attributes
    ----------
    name:
        Name of the volume
    catalog_name:
        Name of the catalog storing the volume
    schema_name:
        Name of the schema storing the volume
    volume_type:
        Type of volume. A managed volume is a Unity Catalog-governed storage volume created within the default storage
        location of the containing schema. An external volume is a Unity Catalog-governed storage volume registered
        against a directory within an external location.
    storage_location:
        Path inside an External Location. Only used for EXTERNAL Volumes.
    grants:
        List of grants operating on the volume

    Examples
    --------
    ```py
    from laktory import models

    volume = models.Volume(
        name="landing",
        catalog_name="dev",
        schema_name="sources",
        volume_type="EXTERNAL",
        storage_location="abfss://landing@lakehouse-storage.dfs.core.windows.net/",
        grants=[
            {"principal": "account users", "privileges": ["READ_VOLUME"]},
            {"principal": "role-metastore-admins", "privileges": ["WRITE_VOLUME"]},
        ],
    )
    print(volume.full_name)
    #> dev.sources.landing
    print(volume.parent_full_name)
    #> dev.sources
    volume.to_pulumi()
    ```

    References
    ----------

    * [Databricks Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html)
    * [Pulumi Databricks Volume](https://www.pulumi.com/registry/packages/databricks/api-docs/volume/)
    """

    name: str
    catalog_name: str = None
    schema_name: str = None
    volume_type: Literal["MANAGED", "EXTERNAL"] = "MANAGED"
    storage_location: str = None
    grants: list[VolumeGrant] = None

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
        return self.full_name

    @property
    def resources(self) -> list[PulumiResource]:
        if self.resources_ is None:
            self.resources_ = [self]

            # Volume grants
            if self.grants:
                self.resources_ += [
                    Grants(
                        resource_name=f"grants-{self.resource_name}",
                        volume=self.full_name,
                        grants=[
                            {"principal": g.principal, "privileges": g.privileges}
                            for g in self.grants
                        ],
                        options={
                            "depends_on": [f"${{resources.{self.resource_name}}}"]
                        },
                    )
                ]

        return self.resources_

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Volume"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.Volume

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["grants"]
