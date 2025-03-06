from laktory._testing import Paths
from laktory.models import BaseModel
from laktory.models.resources.databricks import Job
from laktory.models.resources.databricks import Schema
from laktory.models.resources.databricks import Table

paths = Paths(__file__)


schema = Schema(
    name="my_schema",
    catalog_name="my_catalog",
    tables=[
        Table(
            name="AAPL",
            columns=[
                {
                    "name": "open",
                    "type": "double",
                },
                {
                    "name": "close",
                    "type": "double",
                },
            ],
        ),
        Table(
            name="GOOGL",
            columns=[
                {
                    "name": "low",
                    "type": "double",
                },
                {
                    "name": "high",
                    "type": "double",
                },
            ],
        ),
    ],
)


def test_read_yaml():
    class Prices(BaseModel):
        open: float = None
        close: float = None

    class Stock(BaseModel):
        name: str = None
        symbol: str = None
        prices: Prices = None
        exchange: str = None
        fees: float = None
        rate: float = None

    class Stocks(BaseModel):
        stocks: list[Stock] = None
        query: str = None

    with open(paths.data / "yaml_loader" / "stocks_with_vars.yaml", "r") as fp:
        b = Stocks.model_validate_yaml(fp)

    data = b.model_dump(exclude_unset=True)
    assert data == {
        "stocks": [
            {
                "name": "apple",
                "symbol": "AAPL",
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "amazon",
                "symbol": "AMZN",
                "prices": {"open": 2.0, "close": 4.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "google",
                "symbol": "GOOGL",
                "prices": {"open": 5.0, "close": 6.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "microsoft",
                "symbol": "MSFT",
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
        ],
        "query": "SELECT\n    *\nFORM\n    {df}\nWHERE\n-- COMMENT\n    SYMBOL = 'AAPL'\n;\n",
    }


def test_dump_yaml():
    assert schema.model_dump_yaml(exclude_unset=True).startswith(
        "catalog_name: my_catalog"
    )


def test_camelize():
    schema._configure_serializer(camel=True)
    dump = schema.model_dump()
    schema._configure_serializer(camel=False)
    print(dump)
    assert dump == {
        "comment": None,
        "grants": None,
        "individual_grants": None,
        "name": "my_schema",
        "storageRoot": None,
        "tables": [
            {
                "columns": [
                    {
                        "name": "open",
                        "comment": None,
                        "identity": None,
                        "nullable": None,
                        "type": "double",
                        "typeJson": None,
                    },
                    {
                        "name": "close",
                        "comment": None,
                        "identity": None,
                        "nullable": None,
                        "type": "double",
                        "typeJson": None,
                    },
                ],
                "comment": None,
                "grants": None,
                "individual_grants": None,
                "name": "AAPL",
                "properties": None,
                "catalogName": "my_catalog",
                "dataSourceFormat": "DELTA",
                "schemaName": "my_schema",
                "storageCredentialName": None,
                "storageLocation": None,
                "tableType": "MANAGED",
                "viewDefinition": None,
                "warehouseId": None,
            },
            {
                "columns": [
                    {
                        "name": "low",
                        "comment": None,
                        "identity": None,
                        "nullable": None,
                        "type": "double",
                        "typeJson": None,
                    },
                    {
                        "name": "high",
                        "comment": None,
                        "identity": None,
                        "nullable": None,
                        "type": "double",
                        "typeJson": None,
                    },
                ],
                "comment": None,
                "grants": None,
                "individual_grants": None,
                "name": "GOOGL",
                "properties": None,
                "catalogName": "my_catalog",
                "dataSourceFormat": "DELTA",
                "schemaName": "my_schema",
                "storageCredentialName": None,
                "storageLocation": None,
                "tableType": "MANAGED",
                "viewDefinition": None,
                "warehouseId": None,
            },
        ],
        "volumes": [],
        "catalogName": "my_catalog",
        "forceDestroy": True,
    }


def test_singular():
    job = Job(
        name="my-job",
        clusters=[
            {
                "name": "main",
                "spark_version": "14.0.x-scala2.12",
                "node_type_id": "${vars.node_type_id}",
                "spark_env_vars": {
                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                    "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                },
            }
        ],
    )

    job._configure_serializer(singular=True)
    dump = job.model_dump()
    job._configure_serializer(singular=False)
    print(dump)
    assert dump == {
        "continuous": None,
        "control_run_state": None,
        "description": None,
        "email_notifications": None,
        "format": None,
        "git_source": None,
        "health": None,
        "max_concurrent_runs": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "name": "my-job",
        "name_prefix": None,
        "name_suffix": None,
        "notification_settings": None,
        "queue": None,
        "retry_on_timeout": None,
        "run_as": None,
        "schedule": None,
        "tags": {},
        "timeout_seconds": None,
        "trigger": None,
        "webhook_notifications": None,
        "access_control": [],
        "cluster": [
            {
                "apply_policy_default_values": None,
                "autoscale": None,
                "autotermination_minutes": None,
                "cluster_id": None,
                "custom_tags": None,
                "data_security_mode": "USER_ISOLATION",
                "driver_instance_pool_id": None,
                "driver_node_type_id": None,
                "enable_elastic_disk": None,
                "enable_local_disk_encryption": None,
                "idempotency_token": None,
                "init_scripts": [],
                "instance_pool_id": None,
                "name": "main",
                "node_type_id": "${vars.node_type_id}",
                "num_workers": None,
                "policy_id": None,
                "runtime_engine": None,
                "single_user_name": None,
                "spark_conf": {},
                "spark_env_vars": {
                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                    "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                },
                "spark_version": "14.0.x-scala2.12",
                "ssh_public_keys": [],
            }
        ],
        "environment": None,
        "parameter": [],
        "task": [],
    }


if __name__ == "__main__":
    test_read_yaml()
    test_dump_yaml()
    test_camelize()
    test_singular()
