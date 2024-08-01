from laktory.models.resources.databricks import VectorSearchEndpoint

vector_search_endpoint = VectorSearchEndpoint(
    name="default",
)


def test_vector_search_endpoint():
    print(vector_search_endpoint)
    assert vector_search_endpoint.name == "default"


if __name__ == "__main__":
    test_vector_search_endpoint()
