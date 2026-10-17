from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import DataClassificationCatalogConfig

config = DataClassificationCatalogConfig.model_validate(
    {
        "parent": "catalogs/laktory",
        "auto_tag_configs": [
            {
                "classification_tag": "class.email_address",
                "auto_tagging_mode": "AUTO_TAGGING_ENABLED",
            }
        ],
        "excluded_schemas": {"names": ["sandbox"]},
    }
)


def test_dataclassificationcatalogconfig():
    assert config.auto_tag_configs[0].classification_tag == "class.email_address"
    assert config.resource_name == (
        "data-classification-catalog-config-catalogs-laktory"
    )
    assert (
        config.terraform_resource_type
        == "databricks_data_classification_catalog_config"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(config)
