from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Schema
from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks.table_base import TableColumn

schema = Schema(
    name="flights",
    catalog_name="laktory_testing",
    tables=[
        Table(
            name="f1549",
            data_source_format="DELTA",
            table_type="MANAGED",
            columns=[
                {
                    "name": "airspeed",
                    "type": "double",
                },
                {
                    "name": "altitude",
                    "type": "double",
                },
            ],
        ),
        Table(
            name="f0002",
            data_source_format="DELTA",
            table_type="MANAGED",
            columns=[
                {
                    "name": "airspeed",
                    "type": "double",
                },
                {
                    "name": "altitude",
                    "type": "double",
                },
            ],
        ),
    ],
)


def test_model():
    assert schema.tables[0].column[0].name == "airspeed"
    assert isinstance(schema.tables[0].column[0], TableColumn)
    assert schema.name == "flights"
    assert schema.full_name == "laktory_testing.flights"


def test_schema_additional_resources():
    resources = schema.additional_core_resources
    types_found = {type(r).__name__ for r in resources}
    assert "Table" in types_found


def test_terraform_plan():
    skip_terraform_plan()
    schema_simple = Schema(name="test", catalog_name="dev")
    plan_resource(schema_simple)


if __name__ == "__main__":
    test_model()
