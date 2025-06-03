from laktory.models.resources.databricks import VectorSearchIndex

vector_search_index = VectorSearchIndex(
    name="dev.finances.market_news_vs_index",
    primary_key="id",
    endpoint_name="default",
    index_type="DELTA_SYNC",
    delta_sync_index_spec={
        "source_table": "dev.finances.market_news",
        "embedding_vector_columns": [
            {"name": "embedding", "embedding_dimension": 4095}
        ],
        "pipeline_type": "TRIGGERED",
    },
)


def test_vector_search_index():
    print(vector_search_index)
    assert vector_search_index.name == "dev.finances.market_news_vs_index"
    assert vector_search_index.delta_sync_index_spec.pipeline_type == "TRIGGERED"


if __name__ == "__main__":
    test_vector_search_index()
