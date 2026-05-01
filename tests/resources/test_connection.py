from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Connection
from laktory.models.resources.databricks.grant import Grant
from laktory.models.resources.databricks.grants import Grants

connection = Connection(
    name="my-mysql",
    connection_type="MYSQL",
    comment="Connection to MySQL database",
    options={"host": "mysql.example.com", "port": "3306", "user": "admin"},
    grants=[
        {"principal": "account users", "privileges": ["USE_CONNECTION"]},
        {
            "principal": "role-engineers",
            "privileges": ["USE_CONNECTION", "CREATE_FOREIGN_CATALOG"],
        },
    ],
)


def test_model():
    assert connection.name == "my-mysql"
    assert connection.connection_type == "MYSQL"
    assert connection.comment == "Connection to MySQL database"
    assert connection.options["host"] == "mysql.example.com"
    assert connection.terraform_resource_type == "databricks_connection"
    assert connection.resource_name == "connection-my-mysql"


def test_additional_resources():
    resources = connection.additional_core_resources
    assert len(resources) == 1
    assert isinstance(resources[0], Grants)
    assert resources[0].resource_name == "grants-connection-my-mysql"


def test_grant():
    c = Connection(
        name="my-mysql",
        connection_type="MYSQL",
        grant={"principal": "account users", "privileges": ["USE_CONNECTION"]},
    )
    resources = c.additional_core_resources
    assert len(resources) == 1
    assert isinstance(resources[0], Grant)
    assert resources[0].resource_name == "grant-connection-my-mysql-account-users"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(Connection(name="test-conn", connection_type="MYSQL"))
