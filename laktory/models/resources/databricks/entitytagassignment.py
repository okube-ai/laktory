from typing import Literal

from pydantic import Field

from laktory.models.resources.databricks.entitytagassignment_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.entitytagassignment_base import (
    EntityTagAssignmentBase,
)


class EntityTagAssignment(EntityTagAssignmentBase):
    """
    Databricks Entity Tag Assignment

    Assigns a tag to a Unity Catalog entity (catalog, schema, table, column or
    volume). Tags can also be declared directly on `Catalog`, `Schema`,
    `Table`, `TableColumn` and `Volume` with their `tags` field.

    Examples
    --------
    ```py
    import io

    from laktory import models

    tag_yaml = '''
    entity_type: tables
    entity_name: dev.finance.slv_stock_prices
    tag_key: data_classification
    tag_value: confidential
    '''
    tag = models.resources.databricks.EntityTagAssignment.model_validate_yaml(
        io.StringIO(tag_yaml)
    )
    ```

    References
    ----------

    * [Databricks Tags](https://docs.databricks.com/aws/en/database-objects/tags)
    * [Terraform databricks_entity_tag_assignment](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/entity_tag_assignment)
    """

    # Narrow the type from base's plain str
    entity_type: Literal["catalogs", "schemas", "tables", "columns", "volumes"] = Field(
        ..., description="The type of the entity to which the tag is assigned"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return f"{self.entity_type}-{self.entity_name}-{self.tag_key}"
