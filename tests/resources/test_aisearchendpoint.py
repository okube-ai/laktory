from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AiSearchEndpoint
from laktory.models.resources.databricks import AiSearchIndex


def get_endpoint():
    return AiSearchEndpoint.model_validate(
        {
            "parent": "workspaces/1234567890",
            "endpoint_id": "laktory-ep",
            "endpoint_type": "STANDARD",
            "indexes": [
                {
                    "index_id": "laktory.unit_tests.news_index",
                    "primary_key": "id",
                    "index_type": "DELTA_SYNC",
                    "delta_sync_index_spec": {
                        "source_table": "laktory.unit_tests.news",
                        "pipeline_type": "TRIGGERED",
                        "embedding_source_columns": [
                            {
                                "name": "text",
                                "embedding_model_endpoint": "databricks-gte-large-en",
                            }
                        ],
                    },
                }
            ],
        }
    )


def test_aisearchendpoint():
    endpoint = get_endpoint()
    assert endpoint.resource_name == "ai-search-endpoint-laktory-ep"
    assert endpoint.terraform_resource_type == "databricks_ai_search_endpoint"
    assert "indexes" not in endpoint.terraform_properties

    (index,) = endpoint.additional_core_resources
    assert isinstance(index, AiSearchIndex)
    assert index.parent == "${resources.ai-search-endpoint-laktory-ep.name}"
    assert index.resource_name == "ai-search-index-laktory-unit_tests-news_index"
    assert index.terraform_resource_type == "databricks_ai_search_index"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_endpoint())
