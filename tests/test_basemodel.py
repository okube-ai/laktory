import os

from laktory.models import BaseModel
from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks import Schema
from laktory.models.resources.databricks import Job
from laktory import settings
from laktory._testing import Paths

paths = Paths(__file__)

env = "env"
schema_name = "schema"


schema = Schema(
    name="${vars.env}.${vars.schema_name}",
    catalog_name="${vars.env}",
    tables=[
        Table(
            name="AAPL",
            columns=[
                {
                    "name": "open",
                    "type": "double",
                    # "spark_func_kwargs": {
                    #     "window_length": 2
                    # },  # window_length should not be converted to camel
                },
                {
                    "name": "${resources.close.id}",
                    "type": "double",
                },
            ],
        ),
        Table(
            name="GOOGL",
            columns=[
                {
                    "name": "${vars.dynamic_column}",
                    "type": "double",
                },
                {
                    "name": "high",
                    "type": "double",
                },
            ],
        ),
    ],
    variables={
        "DYNAMIC_COLUMN": "low",
        "env": env,
        "schema_name": schema_name,
        # r"\$\{resources\.([\w.]+)\}": r"\1",
        r"\$\{resources\.([\w.]+)\}": r"${ \1 }",
    },
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
        "catalog_name: ${vars.env}"
    )


def test_camelize():
    settings.camel_serialization = True
    dump = schema.model_dump()
    print(dump)
    assert dump == {
        "comment": None,
        "grants": None,
        "name": "${vars.env}.${vars.schema_name}",
        "tables": [
            {
                "columns": [
                    {
                        "comment": None,
                        "name": "open",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "raiseMissingArgException": True,
                        "schemaName": None,
                        "tableName": "AAPL",
                    },
                    {
                        "comment": None,
                        "name": "${resources.close.id}",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "raiseMissingArgException": True,
                        "schemaName": None,
                        "tableName": "AAPL",
                    },
                ],
                "comment": None,
                "grants": None,
                "name": "AAPL",
                "catalogName": "${vars.env}",
                "dataSourceFormat": "DELTA",
                "primaryKey": None,
                "schemaName": "${vars.env}.${vars.schema_name}",
                "tableType": "MANAGED",
                "viewDefinition": None,
                "warehouseId": "08b717ce051a0261",
            },
            {
                "columns": [
                    {
                        "comment": None,
                        "name": "${vars.dynamic_column}",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "raiseMissingArgException": True,
                        "schemaName": None,
                        "tableName": "GOOGL",
                    },
                    {
                        "comment": None,
                        "name": "high",
                        "pii": None,
                        "type": "double",
                        "unit": None,
                        "catalogName": None,
                        "raiseMissingArgException": True,
                        "schemaName": None,
                        "tableName": "GOOGL",
                    },
                ],
                "comment": None,
                "grants": None,
                "name": "GOOGL",
                "catalogName": "${vars.env}",
                "dataSourceFormat": "DELTA",
                "primaryKey": None,
                "schemaName": "${vars.env}.${vars.schema_name}",
                "tableType": "MANAGED",
                "viewDefinition": None,
                "warehouseId": "08b717ce051a0261",
            },
        ],
        "volumes": [],
        "catalogName": "${vars.env}",
        "forceDestroy": True,
    }

    settings.camel_serialization = False


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

    settings.singular_serialization = True
    dump = job.model_dump()
    print(dump)
    assert dump == {
        "continuous": None,
        "control_run_state": None,
        "email_notifications": None,
        "format": None,
        "health": None,
        "max_concurrent_runs": None,
        "max_retries": None,
        "min_retry_interval_millis": None,
        "name": "my-job",
        "notification_settings": None,
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
    settings.singular_serialization = False


def test_inject_vars():
    d0 = schema.model_dump()
    d1 = schema.inject_vars(d0)
    assert d1["tables"][-1]["columns"][0]["name"] == "low"
    assert d1["tables"][0]["columns"][1]["name"] == "${ close.id }"


def test_inject_includes():

    import yaml

    class M(BaseModel):
        a: list[int]
        b: list[int]

    def read_yaml(fp):
        if hasattr(fp, "name"):
            dirpath = os.path.dirname(fp.name)
        else:
            dirpath = "./"

        # def merge_includes(lines):

        def inject_includes(d):
            if isinstance(d, dict):
                for key, value in d.items():
                    d[key] = inject_includes(value)
            elif isinstance(d, list):
                for i, item in enumerate(d):
                    d[i] = inject_includes(item)
            elif "${include." in str(d):
                path = d.replace("${include.", "")[:-1]
                if not os.path.isabs(path):
                    path = os.path.join(dirpath, path)
                with open(path, "r", encoding="utf-8") as _fp:
                    d = yaml.safe_load(_fp)
                    d = inject_includes(d)
            return d

        # lines = fp.readlines()

        def parse_lines(lines):
            _lines = []
            for line in lines:
                line = line.replace("\n", "")
                indent = " " * (len(line) - len(line.lstrip()))
                if line.strip().startswith("#"):
                    continue
                if "<<: ${include." in line:
                    path = line.replace("<<: ${include.", "").strip()[:-1]
                    if not os.path.isabs(path):
                        path = os.path.join(dirpath, path)
                    with open(path, "r", encoding="utf-8") as _fp:
                        new_lines = _fp.readlines()
                        _lines += [indent + __line for __line in parse_lines(new_lines)]
                elif "${include." in line:
                    _lines += [line.split("${include")[0]]
                    indent = indent + " " * 2
                    path = line.split("${include.")[1].strip()[:-1]
                    if not os.path.isabs(path):
                        path = os.path.join(dirpath, path)
                    with open(path, "r", encoding="utf-8") as _fp:
                        new_lines = _fp.readlines()
                        _lines += [indent + __line for __line in parse_lines(new_lines)]

                else:
                    _lines += [line]

            return _lines

        lines = parse_lines(fp.readlines())

        for l in lines:
            print(l)

        data = yaml.safe_load("\n".join(lines))

        return data

    with open("m-1.yaml", "r") as fp:
        # d = yaml.safe_load(fp)
        d = read_yaml(fp)
        # m = M.model_validate_yaml(fp)

    print(d)


if __name__ == "__main__":
    test_read_yaml()
    test_dump_yaml()
    test_camelize()
    test_singular()
    test_inject_vars()
