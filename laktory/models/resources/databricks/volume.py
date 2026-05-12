from typing import Literal

from pydantic import Field

from laktory.models.grants.volumegrant import VolumeGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks._unitycatalogmixin import UnityCatalogMixin
from laktory.models.resources.databricks.volume_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.volume_base import VolumeBase


class VolumeLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the volume: `catalog`.`schema`.`volume`",
    )


class Volume(UnityCatalogMixin, VolumeBase):
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
    import io

    from laktory import models

    volume_yaml = '''
    name: landing
    catalog_name: dev
    schema_name: sources
    comment: Landing zone for raw data
    volume_type: EXTERNAL
    storage_location: abfss://landing@lakehouse-storage.dfs.core.windows.net/
    grants:
    - principal: account users
      privileges:
      - READ_VOLUME
    - principal: role-metastore-admins
      privileges:
      - WRITE_VOLUME
    '''
    volume = models.resources.databricks.Volume.model_validate_yaml(
        io.StringIO(volume_yaml)
    )
    print(volume.full_name)
    # > dev.sources.landing
    print(volume.parent_full_name)
    # > dev.sources
    ```

    References
    ----------

    * [Databricks Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html)
    """

    # Relax fields the base marks required but Laktory fills via parent validators
    catalog_name: str = Field(
        None, description="Name of the catalog storing the volume"
    )
    lookup_existing: VolumeLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Volume by full `name` (`catalog.schema.volume`) instead of creating it. The volume becomes available for cross-referencing and child resource deployment (grants, etc.); its own field values are not written to the existing resource.",
    )
    schema_name: str = Field(None, description="Name of the schema storing the volume")

    # Narrow the type from base's plain str
    volume_type: Literal["MANAGED", "EXTERNAL"] = Field(
        "MANAGED",
        description="""
    Type of volume. A managed volume is a Unity Catalog-governed storage volume created within the default storage
    location of the containing schema. An external volume is a Unity Catalog-governed storage volume registered against
    a directory within an external location.
    """,
    )

    # Laktory-specific
    grant: VolumeGrant | list[VolumeGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[VolumeGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Volume — including those set
    outside Laktory — with only the entries listed here. Use only when Laktory owns all access management for this
    resource. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Full volume name `{catalog_name}.{schema_name}.{volume_name}`"""
        if self.lookup_existing:
            return self.lookup_existing.name
        return super().full_name

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
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
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants"]
