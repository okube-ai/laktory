from pydantic import Field

from laktory.models.grants.cataloggrant import CatalogGrant
from laktory.models.resources.databricks._tagsmixin import TagsMixin
from laktory.models.resources.databricks.postgrescatalog_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgrescatalog_base import PostgresCatalogBase
from laktory.models.resources.databricks.postgrescatalog_base import PostgresCatalogSpec


class PostgresCatalog(TagsMixin, PostgresCatalogBase):
    """
    Lakebase Autoscaling (Postgres) Unity Catalog catalog

    Registers a Postgres database of a branch as a Unity Catalog catalog, so
    its tables can be queried from Databricks and used as the destination of
    synced tables. Can also be declared in the `catalogs` of a
    `PostgresBranch`, which sets `spec.branch`.

    Examples
    --------
    ```py
    import io

    from laktory import models

    catalog_yaml = '''
    catalog_id: app_pg
    spec:
      branch: projects/app/branches/production
      postgres_database: app_db
      create_database_if_missing: true
    tags:
      domain: app
    grants:
    - principal: account users
      privileges:
      - USE_CATALOG
      - SELECT
    '''
    catalog = models.resources.databricks.PostgresCatalog.model_validate_yaml(
        io.StringIO(catalog_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_catalog](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_catalog)
    """

    # Required (base marks it optional)
    spec: PostgresCatalogSpec = Field(
        ...,
        description="Catalog configuration: Postgres database and branch (set automatically when declared in the catalogs of a branch)",
    )

    # Laktory-specific
    grant: CatalogGrant | list[CatalogGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[CatalogGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Catalog - including those set
    outside Laktory - with only the entries listed here. Use only when Laktory owns all access management for this
    resource. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.catalog_id

    @property
    def additional_core_resources(self) -> list:
        """
        - catalog grants
        - catalog tags

        Unity Catalog objects are named after `catalog_id` (the Terraform `id` is the
        Lakebase path `catalogs/{catalog_id}`), so they depend on the catalog explicitly.
        """
        depends_on = [f"${{resources.{self.resource_name}}}"]
        resources = self.get_grants_additional_resources(
            object={"catalog": self.catalog_id},
            options={"depends_on": depends_on},
        )
        for r in self.get_tags_additional_resources("catalogs", self.catalog_id):
            r.resource_options.depends_on = depends_on
            resources += [r]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants", "tags"]
