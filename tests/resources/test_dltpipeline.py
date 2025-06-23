from laktory import models

pl = models.resources.databricks.Pipeline(
    name="pl-stock-prices",
    catalog="dev1",
    schema="markets1",
)


def test_pipeline():
    data = pl.model_dump(exclude_unset=False)
    print(data)
    assert data == {
        "access_controls": [],
        "allow_duplicate_names": None,
        "budget_policy_id": None,
        "catalog": "dev1",
        "cause": None,
        "channel": "PREVIEW",
        "cluster_id": None,
        "clusters": [],
        "creator_user_name": None,
        "configuration": {},
        "continuous": None,
        "deployment": None,
        "development": None,
        "edition": None,
        "event_log": None,
        "expected_last_modified": None,
        "filters": None,
        "gateway_definition": None,
        "health": None,
        "last_modified": None,
        "latest_updates": None,
        "libraries": None,
        "name": "pl-stock-prices",
        "name_prefix": None,
        "name_suffix": None,
        "notifications": [],
        "photon": None,
        "restart_window": None,
        "root_path": None,
        "run_as": None,
        "run_as_user_name": None,
        "schema_": "markets1",
        "serverless": None,
        "state": None,
        "storage": None,
        "target": None,
        "trigger": None,
        "url": None,
    }


def test_pipeline_pulumi():
    print(pl.resource_name)
    assert pl.resource_name == "dlt-pipeline-pl-stock-prices"
    assert pl.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
        "is_enabled": True,
    }

    assert pl.pulumi_properties == {
        "catalog": "dev1",
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "name": "pl-stock-prices",
        "notifications": [],
        "schema": "markets1",
    }

    # Resources
    assert len(pl.core_resources) == 1
    r = pl.core_resources[-1]
    r.options.aliases = ["my-file"]
    assert pl.core_resources[-1].options.aliases == ["my-file"]


if __name__ == "__main__":
    test_pipeline()
    test_pipeline_pulumi()
