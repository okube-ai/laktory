from pydantic import Field

from laktory.models.grants.registeredmodelgrant import RegisteredModelGrant
from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks._unitycatalogmixin import UnityCatalogMixin
from laktory.models.resources.databricks.registeredmodel_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.registeredmodel_base import RegisteredModelBase


class RegisteredModelLookup(ResourceLookup):
    name: str = Field(
        serialization_alias="id",
        description="Full name of the registered model: `catalog`.`schema`.`model`",
    )


class RegisteredModel(UnityCatalogMixin, RegisteredModelBase):
    """
    Databricks Unity Catalog registered model

    Examples
    --------
    ```py
    import io

    from laktory import models

    registered_model_yaml = '''
    name: my-model
    catalog_name: main
    schema_name: default
    comment: "Registered model for my project"
    grants:
    - principal: account users
      privileges:
      - EXECUTE
    '''
    registered_model = models.resources.databricks.RegisteredModel.model_validate_yaml(
        io.StringIO(registered_model_yaml)
    )
    print(registered_model.full_name)
    # > main.default.my-model
    ```

    References
    ----------

    * [Databricks Registered Model](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/registered_model)
    """

    # Laktory-specific
    grant: RegisteredModelGrant | list[RegisteredModelGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[RegisteredModelGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Registered Model - including
    those set outside Laktory - with only the entries listed here. Use only when Laktory owns all access management
    for this resource. Mutually exclusive with `grant`.
    """,
    )
    lookup_existing: RegisteredModelLookup = Field(
        None,
        exclude=True,
        description="Import a pre-existing Registered Model by full `name` (`catalog.schema.model`) instead of creating it. The registered model becomes available for cross-referencing and child resource deployment (grants, etc.); its own field values are not written to the existing resource.",
    )

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Registered model full name `{catalog_name}.{schema_name}.{model_name}`"""
        if self.lookup_existing:
            return self.lookup_existing.name
        return super().full_name

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - registered model grants
        """
        resources = []

        # Registered model grants
        resources += self.get_grants_additional_resources(
            object={"model": f"${{resources.{self.resource_name}.id}}"}
        )
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return [
            "grant",
            "grants",
        ]
