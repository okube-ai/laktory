from laktory import models
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan

pl = models.resources.databricks.Pipeline(
    name="pl-stock-prices",
    catalog="dev1",
    schema="markets1",
    libraries=[{"notebook": {"path": "/pipelines/dlt_brz_template.py"}}],
)


def test_pipeline():
    assert pl.name == "pl-stock-prices"
    assert pl.catalog == "dev1"
    data = pl.model_dump(exclude_unset=True)
    assert data["schema_"] == "markets1"


def test_pipeline_terraform_schema_rename():
    props = pl.terraform_properties
    assert props.get("schema") == "markets1"
    assert "schema_" not in props


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(pl)
