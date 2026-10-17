from pydantic import Field

from laktory.models.resources.databricks.dataclassificationcatalogconfig_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.dataclassificationcatalogconfig_base import (
    DataClassificationCatalogConfigBase,
)


class DataClassificationCatalogConfig(DataClassificationCatalogConfigBase):
    """
    Databricks Data Classification catalog configuration

    Enables Data Classification on a Unity Catalog catalog: sensitive data
    (PII) is automatically identified and, optionally, tagged. Combined with
    attribute-based access control (`PolicyInfo`), classified columns can be
    masked or filtered without listing them. Can also be declared directly on
    a `Catalog` with its `data_classification` field.

    Examples
    --------
    ```py
    import io

    from laktory import models

    config_yaml = '''
    parent: catalogs/dev
    auto_tag_configs:
    - classification_tag: class.email_address
      auto_tagging_mode: AUTO_TAGGING_ENABLED
    excluded_schemas:
      names:
      - sandbox
    '''
    config = (
        models.resources.databricks.DataClassificationCatalogConfig.model_validate_yaml(
            io.StringIO(config_yaml)
        )
    )
    ```

    References
    ----------

    * [Databricks Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification)
    * [Terraform databricks_data_classification_catalog_config](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/data_classification_catalog_config)
    """

    # Relax field the base marks required but Laktory fills from the parent catalog
    parent: str = Field(
        None,
        description="Parent catalog in the format `catalogs/{catalog_name}`. Set automatically when declared as the `data_classification` of a catalog.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.parent
