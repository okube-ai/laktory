import os
from laktory import app
from laktory import settings
from laktory import models
from laktory.cli._quickstart import read_template
from typer.testing import CliRunner

runner = CliRunner()
settings.cli_raise_external_exceptions = True
dirpath = os.path.dirname(__file__)


def test_preview_pulumi():
    filepath = os.path.join(dirpath, "stack.yaml")
    result = runner.invoke(app, ["preview", "--env", "dev", "--filepath", filepath])
    assert result.exit_code == 0


def test_preview_terraform():
    filepath = os.path.join(dirpath, "stack.yaml")

    # Ideally, we would run `laktory init`, but the runner does not seem to handle running multiple commands
    with open(filepath, "r") as fp:
        pstack = models.Stack.model_validate_yaml(fp).to_terraform(env="dev")
        pstack.init(flags=["-migrate-state", "-upgrade"])

    result = runner.invoke(
        app,
        ["preview", "--backend", "terraform", "--env", "dev", "--filepath", filepath],
    )
    assert result.exit_code == 0


def test_quickstart_stack():
    stack = read_template()
    data = stack.model_dump()
    print(data)
    assert data == {
        "variables": {},
        "backend": "terraform",
        "description": None,
        "environments": {
            "dev": {"variables": {"env": "dev", "is_dev": True}, "resources": None}
        },
        "name": "quickstart",
        "organization": None,
        "pulumi": {"config": {}, "outputs": {}},
        "resources": {
            "dbfsfiles": {
                "dbfs-file-stock-prices": {
                    "access_controls": [],
                    "dirpath": None,
                    "path": "/Workspace/.laktory/landing/events/yahoo-finance/stock_price/stock_prices.json",
                    "source": "./data/stock_prices.json",
                }
            },
            "catalogs": {},
            "clusters": {},
            "directories": {},
            "externallocations": {},
            "groups": {},
            "jobs": {},
            "metastoredataaccesses": {},
            "metastores": {},
            "notebooks": {
                "notebook-pipelines-dlt-brz-template": {
                    "access_controls": [],
                    "dirpath": None,
                    "language": None,
                    "path": "/.laktory/pipelines/dlt_brz_template.py",
                    "source": "./notebooks/pipelines/dlt_brz_template.py",
                },
                "notebook-pipelines-dlt-slv-template": {
                    "access_controls": [],
                    "dirpath": None,
                    "language": None,
                    "path": "/.laktory/pipelines/dlt_slv_template.py",
                    "source": "./notebooks/pipelines/dlt_slv_template.py",
                },
            },
            "pipelines": {
                "pl-quickstart": {
                    "access_controls": [],
                    "allow_duplicate_names": None,
                    "catalog": None,
                    "channel": "PREVIEW",
                    "clusters": [
                        {
                            "access_controls": [],
                            "apply_policy_default_values": None,
                            "autoscale": {"min_workers": 1, "max_workers": 2},
                            "custom_tags": None,
                            "driver_instance_pool_id": None,
                            "driver_node_type_id": None,
                            "enable_local_disk_encryption": None,
                            "init_scripts": [],
                            "instance_pool_id": None,
                            "name": "default",
                            "node_type_id": "TBD",
                            "num_workers": None,
                            "policy_id": None,
                            "spark_conf": {},
                            "spark_env_vars": {},
                            "ssh_public_keys": [],
                        }
                    ],
                    "configuration": {"pipeline_name": "pl-quickstart"},
                    "continuous": None,
                    "development": "${vars.is_dev}",
                    "edition": None,
                    "libraries": [
                        {
                            "file": None,
                            "notebook": {
                                "path": "/.laktory/pipelines/dlt_brz_template.py"
                            },
                        },
                        {
                            "file": None,
                            "notebook": {
                                "path": "/.laktory/pipelines/dlt_slv_template.py"
                            },
                        },
                    ],
                    "name": "pl-quickstart",
                    "notifications": [],
                    "photon": None,
                    "serverless": None,
                    "storage": None,
                    "tables": [
                        {
                            "builder": {
                                "add_laktory_columns": True,
                                "as_dlt_view": False,
                                "drop_duplicates": None,
                                "drop_source_columns": False,
                                "event_source": {
                                    "name": "stock_price",
                                    "description": None,
                                    "producer": {
                                        "name": "yahoo-finance",
                                        "description": None,
                                        "party": 1,
                                    },
                                    "events_root_": "dbfs:/Workspace/.laktory/landing/events/",
                                    "event_root_": None,
                                    "drops": None,
                                    "filter": None,
                                    "read_as_stream": True,
                                    "renames": None,
                                    "selects": None,
                                    "watermark": None,
                                    "fmt": "JSON",
                                    "header": True,
                                    "multiline": False,
                                    "read_options": {},
                                    "schema_location": None,
                                    "type": "STORAGE_EVENTS",
                                },
                                "layer": "BRONZE",
                                "pipeline_name": "pl-quickstart",
                                "table_source": None,
                                "template": "BRONZE",
                                "spark_chain": None,
                            },
                            "catalog_name": None,
                            "columns": [],
                            "comment": None,
                            "data": None,
                            "data_source_format": "DELTA",
                            "expectations": [],
                            "grants": None,
                            "name": "brz_stock_prices",
                            "primary_key": None,
                            "schema_name": "default",
                            "table_type": "MANAGED",
                            "timestamp_key": None,
                            "view_definition": None,
                            "warehouse_id": "08b717ce051a0261",
                        },
                        {
                            "builder": {
                                "add_laktory_columns": True,
                                "as_dlt_view": False,
                                "drop_duplicates": None,
                                "drop_source_columns": True,
                                "event_source": None,
                                "layer": "SILVER",
                                "pipeline_name": "pl-quickstart",
                                "table_source": {
                                    "drops": None,
                                    "filter": None,
                                    "read_as_stream": True,
                                    "renames": None,
                                    "selects": None,
                                    "watermark": None,
                                    "catalog_name": None,
                                    "cdc": None,
                                    "fmt": "DELTA",
                                    "from_pipeline": True,
                                    "name": "brz_stock_prices",
                                    "path": None,
                                    "schema_name": "default",
                                },
                                "template": "SILVER",
                                "spark_chain": {
                                    "nodes": [
                                        {
                                            "allow_missing_column_args": False,
                                            "name": "created_at",
                                            "spark_func_args": [],
                                            "spark_func_kwargs": {},
                                            "spark_func_name": None,
                                            "sql_expression": "data.created_at",
                                            "type": "timestamp",
                                            "unit": None,
                                        },
                                        {
                                            "allow_missing_column_args": False,
                                            "name": "symbol",
                                            "spark_func_args": [
                                                {"value": "data._created_at"}
                                            ],
                                            "spark_func_kwargs": {},
                                            "spark_func_name": "coalesce",
                                            "sql_expression": None,
                                            "type": "string",
                                            "unit": None,
                                        },
                                        {
                                            "allow_missing_column_args": False,
                                            "name": "open",
                                            "spark_func_args": [],
                                            "spark_func_kwargs": {},
                                            "spark_func_name": None,
                                            "sql_expression": "data.open",
                                            "type": "double",
                                            "unit": None,
                                        },
                                        {
                                            "allow_missing_column_args": False,
                                            "name": "close",
                                            "spark_func_args": [],
                                            "spark_func_kwargs": {},
                                            "spark_func_name": None,
                                            "sql_expression": "data.close",
                                            "type": "double",
                                            "unit": None,
                                        },
                                    ]
                                },
                            },
                            "catalog_name": None,
                            "columns": [],
                            "comment": None,
                            "data": None,
                            "data_source_format": "DELTA",
                            "expectations": [
                                {
                                    "name": "positive_price",
                                    "expression": "open > 0",
                                    "action": "FAIL",
                                }
                            ],
                            "grants": None,
                            "name": "slv_stock_prices",
                            "primary_key": None,
                            "schema_name": "default",
                            "table_type": "MANAGED",
                            "timestamp_key": None,
                            "view_definition": None,
                            "warehouse_id": "08b717ce051a0261",
                        },
                    ],
                    "target": "default",
                    "udfs": [],
                }
            },
            "schemas": {},
            "secrets": {},
            "secretscopes": {},
            "serviceprincipals": {},
            "sqlqueries": {},
            "tables": {},
            "providers": {
                "databricks": {
                    "account_id": None,
                    "auth_type": None,
                    "azure_client_id": None,
                    "azure_client_secret": None,
                    "azure_environment": None,
                    "azure_login_app_id": None,
                    "azure_tenant_id": None,
                    "azure_use_msi": None,
                    "azure_workspace_resource_id": None,
                    "client_id": None,
                    "client_secret": None,
                    "cluster_id": None,
                    "config_file": None,
                    "databricks_cli_path": None,
                    "debug_headers": None,
                    "debug_truncate_bytes": None,
                    "google_credentials": None,
                    "google_service_account": None,
                    "host": "${vars.DATABRICKS_HOST}",
                    "http_timeout_seconds": None,
                    "metadata_service_url": None,
                    "password": None,
                    "profile": None,
                    "rate_limit": None,
                    "retry_timeout_seconds": None,
                    "skip_verify": None,
                    "token": "${vars.DATABRICKS_TOKEN}",
                    "username": None,
                    "warehouse_id": None,
                }
            },
            "users": {},
            "volumes": {},
            "warehouses": {},
            "workspacefiles": {},
        },
        "terraform": {"backend": None},
    }


def test_quickstart_pulumi():
    filepath = os.path.join(dirpath, "stack_quickstart_pulumi.yaml")
    result = runner.invoke(
        app,
        [
            "quickstart",
            "--backend",
            "pulumi",
            "-o",
            "okube",
            "-n",
            "Standard_DS3_v2",
            "--filepath",
            filepath,
        ],
    )

    # TODO: Add validation for the generated stack?
    with open(filepath) as fp:
        stack = models.Stack.model_validate_yaml(fp)

    data = stack.model_dump(exclude_none=True)

    assert stack.backend == "pulumi"
    assert stack.name == "quickstart"
    assert stack.pulumi.config["databricks:host"] == "${vars.DATABRICKS_HOST}"
    assert stack.pulumi.config["databricks:token"] == "${vars.DATABRICKS_TOKEN}"
    assert len(stack.resources.dbfsfiles) == 1
    assert len(stack.resources.notebooks) == 2
    assert len(stack.resources.pipelines) == 1


def test_quickstart_terraform():
    filepath = os.path.join(dirpath, "stack_quickstart_terraform.yaml")
    result = runner.invoke(
        app,
        [
            "quickstart",
            "--backend",
            "terraform",
            "-o",
            "okube",
            "-n",
            "Standard_DS3_v2",
            "--filepath",
            filepath,
        ],
    )

    with open(filepath) as fp:
        stack = models.Stack.model_validate_yaml(fp)

    data = stack.model_dump(exclude_none=True)

    assert stack.backend == "terraform"
    assert stack.name == "quickstart"
    assert stack.resources.providers["databricks"].host == "${vars.DATABRICKS_HOST}"
    assert stack.resources.providers["databricks"].token == "${vars.DATABRICKS_TOKEN}"
    assert len(stack.resources.dbfsfiles) == 1
    assert len(stack.resources.notebooks) == 2
    assert len(stack.resources.pipelines) == 1


def atest_deploy_pulumi():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    for filename in [
        "stack.yaml",
        "stack_empty.yaml",
    ]:
        filepath = os.path.join(dirpath, filename)
        result = runner.invoke(
            app,
            [
                "deploy",
                "-e",
                "dev",
                "--filepath",
                filepath,
                "--pulumi-options",
                "--yes",
            ],
        )
        assert result.exit_code == 0


def atest_deploy_terraform():
    # TODO: Figure out how to run in isolation. Currently, pulumi up commands
    # are run concurrently because of the multiple python testing environment
    # which result in:  Conflict: Another update is currently in progress
    for filename in [
        "stack.yaml",
        "stack_empty.yaml",
    ]:
        filepath = os.path.join(dirpath, filename)
        result = runner.invoke(
            app,
            [
                "deploy",
                "-e",
                "dev",
                "--backend",
                "terraform",
                "--filepath",
                filepath,
                "--terraform-options",
                "--auto-approve",
            ],
        )
        assert result.exit_code == 0


if __name__ == "__main__":
    test_preview_pulumi()
    test_preview_terraform()
    test_quickstart_pulumi()
    test_quickstart_stack()
    test_quickstart_terraform()
    # atest_deploy_pulumi()
    # atest_deploy_terraform()
