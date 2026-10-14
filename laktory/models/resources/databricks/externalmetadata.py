from laktory.models.resources.databricks.externalmetadata_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.externalmetadata_base import (
    ExternalMetadataBase,
)


class ExternalMetadata(ExternalMetadataBase):
    """
    Databricks External Metadata

    Object representing an asset of an external system (e.g. a Salesforce
    object upstream or a Power BI report downstream) used to extend Unity
    Catalog lineage beyond Databricks.

    Examples
    --------
    ```py
    import io

    from laktory import models

    metadata_yaml = '''
    name: stock_prices_report
    system_type: POWER_BI
    entity_type: Report
    description: Daily stock prices report
    url: https://app.powerbi.com/groups/finance/reports/stock-prices
    '''
    metadata = models.resources.databricks.ExternalMetadata.model_validate_yaml(
        io.StringIO(metadata_yaml)
    )
    ```

    References
    ----------

    * [Databricks External Lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage)
    * [Terraform databricks_external_metadata](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/external_metadata)
    """
