from pydantic import ConfigDict
import os

from laktory.models import BaseModel
from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks import Schema
from laktory.models.resources.databricks import Job
from laktory._testing import Paths

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
    class OHLC(BaseModel):
        open: float = None
        high: float = None
        low: float = None
        close: float = None

    class Price(BaseModel):
        timestamp: float
        ohlc: OHLC

    class StockPrices(BaseModel):
        symbol: str
        prices: list[Price]

    with open(os.path.join(paths.data, "stockprices0.yaml"), "r") as fp:
        stockprices = StockPrices.model_validate_yaml(fp)

    assert stockprices.model_dump() == {
        "symbol": "AAPL",
        "prices": [
            {
                "timestamp": 1.0,
                "ohlc": {"open": 0.0, "high": None, "low": None, "close": 1.0},
            },
            {
                "timestamp": 2.0,
                "ohlc": {"open": 1.0, "high": None, "low": None, "close": 2.0},
            },
            {
                "timestamp": 3.0,
                "ohlc": {"open": None, "high": None, "low": 3.0, "close": 4.0},
            },
        ],
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
        "name": "my_schema",
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
        "parameter": [],
        "task": [],
    }


def test_inject_includes():

    class Business(BaseModel):
        model_config = ConfigDict(extra="allow")

    with open(os.path.join(paths.data, "model_businesses.yaml"), "r") as fp:
        b = Business.model_validate_yaml(fp)

    data = b.model_dump()
    print(data)
    assert data == {
        "businesses": {
            "apple": {
                "symbol": "aapl",
                "address": {"street": "Sand Hill", "city": "Palo Alto"},
                "queries": ["SELECT\n--    name,\n    *\nFROM\n    table\n;"],
            },
            "amazon": {
                "symbol": "amzn",
                "address": {"street": "Sand Hill", "city": "Palo Alto"},
                "sector": "tech",
                "profitable": True,
            },
            "google": {
                "symbol": "googl",
                "emails": [
                    "mr.ceo@gmail.com",
                    "john.doe@gmail.com",
                    "jane.doe@gmail.com",
                    "sam.doe@gmail.com",
                ],
            },
            "microsoft": {
                "symbol": "msft",
                "address": {"street": "Sand Hill", "city": "Palo Alto"},
                "sector": "tech",
                "profitable": True,
                "emails": [
                    "john.doe@gmail.com",
                    "jane.doe@gmail.com",
                    "sam.doe@gmail.com",
                ],
            },
        }
    }


if __name__ == "__main__":
    test_read_yaml()
    test_dump_yaml()
    test_camelize()
    test_singular()
    test_inject_includes()
