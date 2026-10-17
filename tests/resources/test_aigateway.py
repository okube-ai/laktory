from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AiGatewayMcpService
from laktory.models.resources.databricks import AiGatewayModelProviderService
from laktory.models.resources.databricks import AiGatewayModelService

model_service = AiGatewayModelService.model_validate(
    {
        "parent": "schemas/laktory.unit_tests",
        "model_service_id": "customer_support",
        "config": {
            "routing": {
                "destinations": [
                    {
                        "name": "primary",
                        "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                        "pay_per_token_config": {
                            "model": "models/system.ai.databricks-gpt-5"
                        },
                        "traffic_percentage": 100,
                    }
                ]
            }
        },
    }
)

provider_service = AiGatewayModelProviderService.model_validate(
    {
        "parent": "schemas/laktory.unit_tests",
        "model_provider_service_id": "openai_prod",
        "config": {
            "provider_type": "EXTERNAL_MODEL_PROVIDER_TYPE_OPENAI",
            "targets": [
                {"model": "gpt-5", "native_api_types": ["openai/v1/chat/completions"]}
            ],
            "openai": {
                "direct": {
                    "api_key": {
                        "secret_reference": {
                            "name": "secrets/laktory.unit_tests.openai_api_key"
                        }
                    }
                }
            },
        },
    }
)

mcp_service = AiGatewayMcpService.model_validate(
    {
        "parent": "schemas/laktory.unit_tests",
        "mcp_service_id": "knowledge_tools",
        "config": {"source_connection": {"name": "connections/laktory.unit_tests.mcp"}},
    }
)


def test_aigateway_services():
    assert model_service.resource_name == (
        "ai-gateway-model-service-schemas-laktory-unit_tests-customer_support"
    )
    assert provider_service.resource_name == (
        "ai-gateway-model-provider-service-schemas-laktory-unit_tests-openai_prod"
    )
    assert mcp_service.resource_name == (
        "ai-gateway-mcp-service-schemas-laktory-unit_tests-knowledge_tools"
    )
    assert (
        model_service.terraform_properties["config"]["routing"]["destinations"][0][
            "traffic_percentage"
        ]
        == 100
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(model_service)
    plan_resource(provider_service)
    plan_resource(mcp_service)
