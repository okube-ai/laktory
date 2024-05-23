from laktory import models


pl = models.resources.databricks.DLTPipeline(
    name="pl-stock-prices",
    catalog="dev1",
    target="markets1",
)


def test_pipeline():
    print(pl.model_dump())
    assert pl.model_dump() == {
        "access_controls": [],
        "allow_duplicate_names": None,
        "catalog": "dev1",
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "continuous": None,
        "development": None,
        "edition": None,
        "libraries": None,
        "name": "pl-stock-prices",
        "notifications": [],
        "photon": None,
        "serverless": None,
        "storage": None,
        "target": "markets1",
    }


def test_pipeline_pulumi():
    print(pl.resource_name)
    assert pl.resource_name == "dlt-pl-stock-prices"
    assert pl.options.model_dump(exclude_none=True) == {
        "depends_on": [],
        "delete_before_replace": True,
    }
    print(pl.pulumi_properties)
    assert pl.pulumi_properties == {
        "catalog": "dev1",
        "channel": "PREVIEW",
        "clusters": [],
        "configuration": {},
        "name": "pl-stock-prices",
        "notifications": [],
        "target": "markets1",
    }

    # Resources
    assert len(pl.core_resources) == 1
    r = pl.core_resources[-1]
    r.options.aliases = ["my-file"]
    assert pl.core_resources[-1].options.aliases == ["my-file"]


if __name__ == "__main__":
    test_pipeline()
    test_pipeline_pulumi()
