from laktory import models

pl = models.resources.databricks.Pipeline(
    name="pl-stock-prices",
    catalog="dev1",
    schema="markets1",
    libraries=[{"notebook": {"path": "/pipelines/dlt_brz_template.py"}}],
)


def test_pipeline():
    data = pl.model_dump(exclude_unset=False)
    print(data)
    assert data == {
        "allow_duplicate_names": None,
        "budget_policy_id": None,
        "catalog": "dev1",
        "cause": None,
        "channel": None,
        "cluster_id": None,
        "configuration": None,
        "continuous": None,
        "creator_user_name": None,
        "development": None,
        "edition": None,
        "expected_last_modified": None,
        "health": None,
        "last_modified": None,
        "name": "pl-stock-prices",
        "photon": None,
        "root_path": None,
        "run_as_user_name": None,
        "schema_": "markets1",
        "serverless": None,
        "state": None,
        "storage": None,
        "tags": None,
        "target": None,
        "url": None,
        "usage_policy_id": None,
        "cluster": None,
        "deployment": None,
        "environment": None,
        "event_log": None,
        "filters": None,
        "gateway_definition": None,
        "ingestion_definition": None,
        "latest_updates": None,
        "library": [
            {
                "jar": None,
                "whl": None,
                "file": None,
                "glob": None,
                "maven": None,
                "notebook": {"path": "/pipelines/dlt_brz_template.py"},
            }
        ],
        "notification": None,
        "restart_window": None,
        "run_as": None,
        "timeouts": None,
        "trigger": None,
        "access_controls": [],
        "name_prefix": None,
        "name_suffix": None,
    }
