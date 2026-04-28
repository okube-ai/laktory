from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import VectorSearchEndpoint

vector_search_endpoint = VectorSearchEndpoint(
    endpoint_type="STANDARD",
    name="default",
)


def test_vector_search_endpoint():
    assert vector_search_endpoint.name == "default"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(vector_search_endpoint)
