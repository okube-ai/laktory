from pathlib import Path

import pytest

from laktory import models
from laktory._settings import settings
from laktory._testing import skip_pulumi_preview
from laktory._testing import skip_terraform_plan

root = Path(__file__).parent


@pytest.fixture
def stack():
    with open(root / "data/stack.yaml", "r") as fp:
        stack = models.Stack.model_validate_yaml(fp)

    stack.terraform.backend = {
        "azurerm": {
            "resource_group_name": "o3-rg-laktory-dev",
            "storage_account_name": "o3stglaktorydev",
            "container_name": "unit-testing",
            "key": "terraform/dev.terraform.tfstate",
        }
    }

    return stack


@pytest.fixture
def full_stack():
    from tests.resources.test_alert import alert
    from tests.resources.test_catalog import catalog
    from tests.resources.test_cluster_policy import cluster_policy
    from tests.resources.test_dashboard import dashboard
    from tests.resources.test_directory import directory
    from tests.resources.test_job import job
    from tests.resources.test_job import job_for_each
    from tests.resources.test_metastore import metastore
    from tests.resources.test_mlflow_experiment import mlexp
    from tests.resources.test_mlflow_model import mlmodel
    from tests.resources.test_mlflow_webhook import mlwebhook
    from tests.resources.test_notebook import get_notebook
    from tests.resources.test_permissions import permissions
    from tests.resources.test_pipeline_orchestrators import get_pl_dlt
    from tests.resources.test_query import query
    from tests.resources.test_repo import repo
    from tests.resources.test_schema import schema
    from tests.resources.test_user import group
    from tests.resources.test_user import user
    from tests.resources.test_vectorsearchendpoint import vector_search_endpoint
    from tests.resources.test_vectorsearchindex import vector_search_index
    from tests.resources.test_workspacebinding import workspace_binding
    from tests.resources.test_workspacefile import get_workspace_file

    # Update paths because preview is executed in tmp_path
    nb = get_notebook()
    workspace_file = get_workspace_file()
    nb.source = str(root / "resources" / nb.source)
    workspace_file.source = str(root / "resources" / workspace_file.source)

    _resources = {
        "databricks_alerts": [alert],
        "databricks_catalogs": [catalog],
        "databricks_clusterpolicies": [cluster_policy],
        "databricks_dashboards": [dashboard],
        "databricks_directories": [directory],
        "databricks_jobs": [job, job_for_each],
        "databricks_metastores": [metastore],
        "databricks_mlflowexperiments": [mlexp],
        "databricks_mlflowmodels": [mlmodel],
        "databricks_mlflowwebhooks": [mlwebhook],
        "databricks_notebooks": [nb],
        "databricks_permissions": [permissions],
        "databricks_queries": [query],
        "databricks_repos": [repo],
        "databricks_schemas": [schema],
        "databricks_groups": [group],
        "databricks_users": [user],
        "databricks_vectorsearchendpoints": [vector_search_endpoint],
        "databricks_vectorsearchindexes": [vector_search_index],
        "databricks_workspacefiles": [workspace_file],
        "databricks_workspacebindings": [workspace_binding],
        "pipelines": [get_pl_dlt()],  # required by job
    }

    resources = {}
    for k, v in _resources.items():
        resources[k] = {r.resource_name: r for r in v}

    resources["providers"] = {
        "provider-workspace-neptune": {
            "host": "${vars.DATABRICKS_HOST}",
            # "azure_client_id": "0",
            # "azure_client_secret": "0",
            # "azure_tenant_id": "0",
        },
        "databricks": {
            "host": "${vars.DATABRICKS_HOST}",
            "token": "${vars.DATABRICKS_TOKEN}",
        },
        "databricks1": {
            "host": "${vars.DATABRICKS_HOST}",
        },
        "databricks2": {
            "host": "${vars.DATABRICKS_HOST}",
        },
    }

    stack = models.Stack(
        organization="okube",
        name="unit-testing",
        backend="pulumi",
        pulumi={
            "config": {
                "databricks:host": "${vars.DATABRICKS_HOST}",
                "databricks:token": "${vars.DATABRICKS_TOKEN}",
            }
        },
        resources=resources,
        environments={"dev": {}},
    )

    return stack


def test_stack_model(stack):
    stack.model_dump()


def test_stack_env_model(stack):
    # dev
    _stack = stack.get_env("dev").inject_vars()
    pl = _stack.resources.pipelines["pl-custom-name"]

    assert _stack.variables == {
        "business_unit": "laktory",
        "workflow_name": "UNDEFINED",
        "env": "dev",
        "is_dev": True,
        "node_type_id": "Standard_DS3_v2",
    }
    assert pl.orchestrator.development is None
    assert pl.nodes[0].dlt_template is None
    assert pl.variables == {
        "workflow_name": "pl-stock-prices-ut-stack",
        "business_unit": "laktory",
        "env": "dev",
        "is_dev": True,
        "node_type_id": "Standard_DS3_v2",
    }

    # prod
    _stack = stack.get_env("prod")
    pl = _stack.resources.pipelines["pl-custom-name"]
    assert _stack.variables == {
        "business_unit": "laktory",
        "workflow_name": "UNDEFINED",
        "env": "prod",
        "is_dev": False,
        "node_type_id": "Standard_DS4_v2",
    }
    assert not pl.orchestrator.development
    assert pl.nodes[0].dlt_template is None
    assert pl.variables == {
        "workflow_name": "pl-stock-prices-ut-stack",
        "business_unit": "laktory",
        "env": "prod",
        "is_dev": False,
        "node_type_id": "Standard_DS4_v2",
    }


def test_stack_resources_unique_name():
    with pytest.raises(ValueError):
        models.Stack(
            name="stack",
            organization="o3",
            resources=models.StackResources(
                databricks_schemas={"finance": {"name": "schema_finance"}},
                databricks_catalogs={
                    "finance": {
                        "name": "catalog_finance",
                    }
                },
            ),
        )


def test_pulumi_stack(monkeypatch, stack):
    monkeypatch.setenv("DATABRICKS_HOST", "my-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "my-token")

    pstack = stack.to_pulumi(env_name=None)
    assert pstack.organization == "okube"

    data_default = pstack.model_dump()
    # data_default["resources"]["dlt-custom_name"]["properties"]["configuration"]["config"] = "_config"
    assert data_default == {
        "config": {"databricks:host": "my-host", "databricks:token": "my-token"},
        "name": "unit-testing",
        "outputs": {},
        "resources": {
            "databricks": {
                "options": {},
                "properties": {"host": "my-host", "token": "my-token"},
                "type": "pulumi:providers:databricks",
            },
            "dlt-custom-name": {
                "options": {"dependsOn": [], "provider": "${databricks}"},
                "properties": {
                    "channel": "PREVIEW",
                    "clusters": [],
                    "configuration": {
                        "business_unit": "laktory",
                        "config": '{"name": '
                        '"pl-stock-prices-ut-stack", '
                        '"nodes": '
                        '[{"dlt_template": '
                        "null, "
                        '"name": '
                        '"first_node", '
                        '"source": '
                        '{"format": '
                        '"JSON", '
                        '"path": '
                        '"/tmp/"}}], '
                        '"orchestrator": '
                        '{"access_controls": '
                        '[{"group_name": '
                        '"account '
                        'users", '
                        '"permission_level": '
                        '"CAN_VIEW"}, '
                        '{"group_name": '
                        '"role-engineers", '
                        '"permission_level": '
                        '"CAN_RUN"}], '
                        '"configuration": '
                        '{"business_unit": '
                        '"laktory", '
                        '"workflow_name": '
                        '"pl-stock-prices-ut-stack", '
                        '"pipeline_name": '
                        '"pl-stock-prices-ut-stack", '
                        '"requirements": '
                        '"[\\"laktory==0.8.0\\"]", '
                        '"config": '
                        '"{\\"name\\": '
                        '\\"pl-stock-prices-ut-stack\\", '
                        '\\"nodes\\": '
                        '[{\\"dlt_template\\": '
                        "null, "
                        '\\"name\\": '
                        '\\"first_node\\", '
                        '\\"source\\": '
                        '{\\"format\\": '
                        '\\"JSON\\", '
                        '\\"path\\": '
                        '\\"/tmp/\\"}}], '
                        '\\"orchestrator\\": '
                        '{\\"access_controls\\": '
                        '[{\\"group_name\\": '
                        '\\"account '
                        'users\\", '
                        '\\"permission_level\\": '
                        '\\"CAN_VIEW\\"}, '
                        '{\\"group_name\\": '
                        '\\"role-engineers\\", '
                        '\\"permission_level\\": '
                        '\\"CAN_RUN\\"}], '
                        '\\"configuration\\": '
                        '{\\"business_unit\\": '
                        '\\"laktory\\", '
                        '\\"workflow_name\\": '
                        '\\"pl-stock-prices-ut-stack\\", '
                        '\\"pipeline_name\\": '
                        '\\"pl-stock-prices-ut-stack\\", '
                        '\\"requirements\\": '
                        '\\"[\\\\\\"laktory==0.8.0\\\\\\"]\\", '
                        '\\"config\\": '
                        '\\"{\\\\\\"name\\\\\\": '
                        '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                        '\\\\\\"nodes\\\\\\": '
                        '[{\\\\\\"dlt_template\\\\\\": '
                        "null, "
                        '\\\\\\"name\\\\\\": '
                        '\\\\\\"first_node\\\\\\", '
                        '\\\\\\"source\\\\\\": '
                        '{\\\\\\"format\\\\\\": '
                        '\\\\\\"JSON\\\\\\", '
                        '\\\\\\"path\\\\\\": '
                        '\\\\\\"/tmp/\\\\\\"}}], '
                        '\\\\\\"orchestrator\\\\\\": '
                        '{\\\\\\"access_controls\\\\\\": '
                        '[{\\\\\\"group_name\\\\\\": '
                        '\\\\\\"account '
                        'users\\\\\\", '
                        '\\\\\\"permission_level\\\\\\": '
                        '\\\\\\"CAN_VIEW\\\\\\"}, '
                        '{\\\\\\"group_name\\\\\\": '
                        '\\\\\\"role-engineers\\\\\\", '
                        '\\\\\\"permission_level\\\\\\": '
                        '\\\\\\"CAN_RUN\\\\\\"}], '
                        '\\\\\\"configuration\\\\\\": '
                        '{\\\\\\"business_unit\\\\\\": '
                        '\\\\\\"laktory\\\\\\", '
                        '\\\\\\"workflow_name\\\\\\": '
                        '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                        '\\\\\\"pipeline_name\\\\\\": '
                        '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                        '\\\\\\"requirements\\\\\\": '
                        '\\\\\\"[\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\"]\\\\\\", '
                        '\\\\\\"config\\\\\\": '
                        '\\\\\\"{\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\": '
                        '[{\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\": '
                        "null, "
                        '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\": '
                        '{\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\"}}], '
                        '\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\": '
                        '{\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\": '
                        '[{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"account '
                        'users\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\"}, '
                        '{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\"}], '
                        '\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\": '
                        '{\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\"}, '
                        '\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\": '
                        '[{\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\": '
                        '{\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\"}}], '
                        '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                        '\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\": '
                        '\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\"}}\\\\\\"}, '
                        '\\\\\\"libraries\\\\\\": '
                        '[{\\\\\\"notebook\\\\\\": '
                        '{\\\\\\"path\\\\\\": '
                        '\\\\\\"/pipelines/dlt_brz_template.py\\\\\\"}}], '
                        '\\\\\\"name\\\\\\": '
                        '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                        '\\\\\\"type\\\\\\": '
                        '\\\\\\"DATABRICKS_DLT\\\\\\"}}\\"}, '
                        '\\"libraries\\": '
                        '[{\\"notebook\\": '
                        '{\\"path\\": '
                        '\\"/pipelines/dlt_brz_template.py\\"}}], '
                        '\\"name\\": '
                        '\\"pl-stock-prices-ut-stack\\", '
                        '\\"type\\": '
                        '\\"DATABRICKS_DLT\\"}}"}, '
                        '"libraries": '
                        '[{"notebook": '
                        '{"path": '
                        '"/pipelines/dlt_brz_template.py"}}], '
                        '"name": '
                        '"pl-stock-prices-ut-stack", '
                        '"type": '
                        '"DATABRICKS_DLT"}}',
                        "pipeline_name": "pl-stock-prices-ut-stack",
                        "requirements": '["laktory==0.8.0"]',
                        "workflow_name": "pl-stock-prices-ut-stack",
                    },
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "name": "pl-stock-prices-ut-stack",
                    "notifications": [],
                },
                "type": "databricks:Pipeline",
            },
            "job-stock-prices-ut-stack": {
                "options": {},
                "properties": {
                    "jobClusters": [
                        {
                            "jobClusterKey": "main",
                            "newCluster": {
                                "dataSecurityMode": "USER_ISOLATION",
                                "initScripts": [],
                                "nodeTypeId": "${vars.node_type_id}",
                                "sparkConf": {},
                                "sparkEnvVars": {
                                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                    "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                                },
                                "sparkVersion": "16.3.x-scala2.12",
                                "sshPublicKeys": [],
                            },
                        }
                    ],
                    "name": "job-stock-prices-ut-stack",
                    "parameters": [],
                    "tags": {},
                    "tasks": [
                        {
                            "jobClusterKey": "main",
                            "libraries": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "notebookTask": {
                                "notebookPath": "/jobs/ingest_stock_metadata.py"
                            },
                            "taskKey": "ingest-metadata",
                        },
                        {
                            "pipelineTask": {"pipelineId": "${dlt-custom-name.id}"},
                            "taskKey": "run-pipeline",
                        },
                    ],
                },
                "type": "databricks:Job",
            },
            "notebook-external": {
                "get": {"id": "/Workspace/external"},
                "options": {},
                "type": "databricks:Notebook",
            },
            "permissions-dlt-custom-name": {
                "options": {
                    "dependsOn": ["${dlt-custom-name}"],
                    "provider": "${databricks}",
                },
                "properties": {
                    "accessControls": [
                        {"groupName": "account users", "permissionLevel": "CAN_VIEW"},
                        {"groupName": "role-engineers", "permissionLevel": "CAN_RUN"},
                    ],
                    "pipelineId": "${dlt-custom-name.id}",
                },
                "type": "databricks:Permissions",
            },
            "permissions-notebook-external": {
                "options": {"dependsOn": ["${notebook-external}"]},
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_READ"}
                    ],
                    "notebookPath": "${notebook-external.path}",
                },
                "type": "databricks:Permissions",
            },
            "permissions-warehouse-external": {
                "options": {"dependsOn": ["${warehouse-external}"]},
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_USE"}
                    ],
                    "sqlEndpointId": "${warehouse-external.id}",
                },
                "type": "databricks:Permissions",
            },
            "permissions_test": {
                "options": {},
                "properties": {
                    "accessControls": [
                        {"permissionLevel": "CAN_MANAGE", "userName": "user1"},
                        {"permissionLevel": "CAN_RUN", "userName": "user2"},
                    ],
                    "pipelineId": "pipeline_123",
                },
                "type": "databricks:Permissions",
            },
            "warehouse-external": {
                "get": {"id": "d2fa41bf94858c4b"},
                "options": {},
                "type": "databricks:SqlEndpoint",
            },
        },
        "runtime": "yaml",
        "variables": {},
    }

    # Prod
    data = stack.to_pulumi(env_name="prod").model_dump()
    assert (
        data
        == {
            "config": {"databricks:host": "my-host", "databricks:token": "my-token"},
            "name": "unit-testing",
            "outputs": {},
            "resources": {
                "databricks": {
                    "options": {},
                    "properties": {"host": "my-host", "token": "my-token"},
                    "type": "pulumi:providers:databricks",
                },
                "dlt-custom-name": {
                    "options": {"dependsOn": [], "provider": "${databricks}"},
                    "properties": {
                        "channel": "PREVIEW",
                        "clusters": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "config": '{"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"nodes": '
                            '[{"dlt_template": '
                            "null, "
                            '"name": '
                            '"first_node", '
                            '"source": '
                            '{"format": '
                            '"JSON", '
                            '"path": '
                            '"/tmp/"}}], '
                            '"orchestrator": '
                            '{"access_controls": '
                            '[{"group_name": '
                            '"account '
                            'users", '
                            '"permission_level": '
                            '"CAN_VIEW"}, '
                            '{"group_name": '
                            '"role-engineers", '
                            '"permission_level": '
                            '"CAN_RUN"}], '
                            '"configuration": '
                            '{"business_unit": '
                            '"laktory", '
                            '"workflow_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"pipeline_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"requirements": '
                            '"[\\"laktory==0.8.0\\"]", '
                            '"config": '
                            '"{\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"nodes\\": '
                            '[{\\"dlt_template\\": '
                            "null, "
                            '\\"name\\": '
                            '\\"first_node\\", '
                            '\\"source\\": '
                            '{\\"format\\": '
                            '\\"JSON\\", '
                            '\\"path\\": '
                            '\\"/tmp/\\"}}], '
                            '\\"orchestrator\\": '
                            '{\\"access_controls\\": '
                            '[{\\"group_name\\": '
                            '\\"account '
                            'users\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_VIEW\\"}, '
                            '{\\"group_name\\": '
                            '\\"role-engineers\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_RUN\\"}], '
                            '\\"configuration\\": '
                            '{\\"business_unit\\": '
                            '\\"laktory\\", '
                            '\\"workflow_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"pipeline_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"requirements\\": '
                            '\\"[\\\\\\"laktory==0.8.0\\\\\\"]\\", '
                            '\\"config\\": '
                            '\\"{\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"nodes\\\\\\": '
                            '[{\\\\\\"dlt_template\\\\\\": '
                            "null, "
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"first_node\\\\\\", '
                            '\\\\\\"source\\\\\\": '
                            '{\\\\\\"format\\\\\\": '
                            '\\\\\\"JSON\\\\\\", '
                            '\\\\\\"path\\\\\\": '
                            '\\\\\\"/tmp/\\\\\\"}}], '
                            '\\\\\\"orchestrator\\\\\\": '
                            '{\\\\\\"access_controls\\\\\\": '
                            '[{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"account '
                            'users\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_VIEW\\\\\\"}, '
                            '{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"role-engineers\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_RUN\\\\\\"}], '
                            '\\\\\\"configuration\\\\\\": '
                            '{\\\\\\"business_unit\\\\\\": '
                            '\\\\\\"laktory\\\\\\", '
                            '\\\\\\"workflow_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"pipeline_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"requirements\\\\\\": '
                            '\\\\\\"[\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\"]\\\\\\", '
                            '\\\\\\"config\\\\\\": '
                            '\\\\\\"{\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\"development\\\\\\\\\\\\\\": '
                            "false, "
                            '\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\"}}\\\\\\"}, '
                            '\\\\\\"development\\\\\\": '
                            "false, "
                            '\\\\\\"libraries\\\\\\": '
                            '[{\\\\\\"notebook\\\\\\": '
                            '{\\\\\\"path\\\\\\": '
                            '\\\\\\"/pipelines/dlt_brz_template.py\\\\\\"}}], '
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"type\\\\\\": '
                            '\\\\\\"DATABRICKS_DLT\\\\\\"}}\\"}, '
                            '\\"development\\": '
                            "false, "
                            '\\"libraries\\": '
                            '[{\\"notebook\\": '
                            '{\\"path\\": '
                            '\\"/pipelines/dlt_brz_template.py\\"}}], '
                            '\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"type\\": '
                            '\\"DATABRICKS_DLT\\"}}"}, '
                            '"development": '
                            "false, "
                            '"libraries": '
                            '[{"notebook": '
                            '{"path": '
                            '"/pipelines/dlt_brz_template.py"}}], '
                            '"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"type": '
                            '"DATABRICKS_DLT"}}',
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "requirements": '["laktory==0.8.0"]',
                            "workflow_name": "pl-stock-prices-ut-stack",
                        },
                        "development": False,
                        "libraries": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notifications": [],
                    },
                    "type": "databricks:Pipeline",
                },
                "job-stock-prices-ut-stack": {
                    "options": {},
                    "properties": {
                        "jobClusters": [
                            {
                                "jobClusterKey": "main",
                                "newCluster": {
                                    "dataSecurityMode": "USER_ISOLATION",
                                    "initScripts": [],
                                    "nodeTypeId": "Standard_DS4_v2",
                                    "sparkConf": {},
                                    "sparkEnvVars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "prod",
                                    },
                                    "sparkVersion": "16.3.x-scala2.12",
                                    "sshPublicKeys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameters": [],
                        "tags": {},
                        "tasks": [
                            {
                                "jobClusterKey": "main",
                                "libraries": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebookTask": {
                                    "notebookPath": "/jobs/ingest_stock_metadata.py"
                                },
                                "taskKey": "ingest-metadata",
                            },
                            {
                                "pipelineTask": {"pipelineId": "${dlt-custom-name.id}"},
                                "taskKey": "run-pipeline",
                            },
                        ],
                    },
                    "type": "databricks:Job",
                },
                "notebook-external": {
                    "get": {"id": "/Workspace/external"},
                    "options": {},
                    "type": "databricks:Notebook",
                },
                "permissions-dlt-custom-name": {
                    "options": {
                        "dependsOn": ["${dlt-custom-name}"],
                        "provider": "${databricks}",
                    },
                    "properties": {
                        "accessControls": [
                            {
                                "groupName": "account users",
                                "permissionLevel": "CAN_VIEW",
                            },
                            {
                                "groupName": "role-engineers",
                                "permissionLevel": "CAN_RUN",
                            },
                        ],
                        "pipelineId": "${dlt-custom-name.id}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions-notebook-external": {
                    "options": {"dependsOn": ["${notebook-external}"]},
                    "properties": {
                        "accessControls": [
                            {
                                "groupName": "role-analysts",
                                "permissionLevel": "CAN_READ",
                            }
                        ],
                        "notebookPath": "${notebook-external.path}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions-warehouse-external": {
                    "options": {"dependsOn": ["${warehouse-external}"]},
                    "properties": {
                        "accessControls": [
                            {"groupName": "role-analysts", "permissionLevel": "CAN_USE"}
                        ],
                        "sqlEndpointId": "${warehouse-external.id}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions_test": {
                    "options": {},
                    "properties": {
                        "accessControls": [
                            {"permissionLevel": "CAN_MANAGE", "userName": "user1"},
                            {"permissionLevel": "CAN_RUN", "userName": "user2"},
                        ],
                        "pipelineId": "pipeline_123",
                    },
                    "type": "databricks:Permissions",
                },
                "warehouse-external": {
                    "get": {"id": "d2fa41bf94858c4b"},
                    "options": {},
                    "type": "databricks:SqlEndpoint",
                },
            },
            "runtime": "yaml",
            "variables": {},
        }
        != {
            "config": {"databricks:host": "my-host", "databricks:token": "my-token"},
            "name": "unit-testing",
            "outputs": {},
            "resources": {
                "databricks": {
                    "options": {},
                    "properties": {"host": "my-host", "token": "my-token"},
                    "type": "pulumi:providers:databricks",
                },
                "dlt-custom-name": {
                    "options": {"dependsOn": [], "provider": "${databricks}"},
                    "properties": {
                        "channel": "PREVIEW",
                        "clusters": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "workflow_name": "pl-stock-prices-ut-stack",
                            "workspace_laktory_root": "/.laktory/",
                        },
                        "development": False,
                        "libraries": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notifications": [],
                    },
                    "type": "databricks:Pipeline",
                },
                "job-stock-prices-ut-stack": {
                    "options": {},
                    "properties": {
                        "jobClusters": [
                            {
                                "jobClusterKey": "main",
                                "newCluster": {
                                    "dataSecurityMode": "USER_ISOLATION",
                                    "initScripts": [],
                                    "nodeTypeId": "Standard_DS4_v2",
                                    "sparkConf": {},
                                    "sparkEnvVars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "prod",
                                    },
                                    "sparkVersion": "16.3.x-scala2.12",
                                    "sshPublicKeys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameters": [],
                        "tags": {},
                        "tasks": [
                            {
                                "jobClusterKey": "main",
                                "libraries": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebookTask": {
                                    "notebookPath": "/jobs/ingest_stock_metadata.py"
                                },
                                "taskKey": "ingest-metadata",
                            },
                            {
                                "pipelineTask": {"pipelineId": "${dlt-custom-name.id}"},
                                "taskKey": "run-pipeline",
                            },
                        ],
                    },
                    "type": "databricks:Job",
                },
                "notebook-external": {
                    "get": {"id": "/Workspace/external"},
                    "options": {},
                    "type": "databricks:Notebook",
                },
                "permissions-dlt-custom-name": {
                    "options": {
                        "dependsOn": ["${dlt-custom-name}"],
                        "provider": "${databricks}",
                    },
                    "properties": {
                        "accessControls": [
                            {
                                "groupName": "account users",
                                "permissionLevel": "CAN_VIEW",
                            },
                            {
                                "groupName": "role-engineers",
                                "permissionLevel": "CAN_RUN",
                            },
                        ],
                        "pipelineId": "${dlt-custom-name.id}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions-notebook-external": {
                    "options": {"dependsOn": ["${notebook-external}"]},
                    "properties": {
                        "accessControls": [
                            {
                                "groupName": "role-analysts",
                                "permissionLevel": "CAN_READ",
                            }
                        ],
                        "notebookPath": "${notebook-external.path}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions-warehouse-external": {
                    "options": {"dependsOn": ["${warehouse-external}"]},
                    "properties": {
                        "accessControls": [
                            {"groupName": "role-analysts", "permissionLevel": "CAN_USE"}
                        ],
                        "sqlEndpointId": "${warehouse-external.id}",
                    },
                    "type": "databricks:Permissions",
                },
                "permissions_test": {
                    "options": {},
                    "properties": {
                        "accessControls": [
                            {"permissionLevel": "CAN_MANAGE", "userName": "user1"},
                            {"permissionLevel": "CAN_RUN", "userName": "user2"},
                        ],
                        "pipelineId": "pipeline_123",
                    },
                    "type": "databricks:Permissions",
                },
                "warehouse-external": {
                    "get": {"id": "d2fa41bf94858c4b"},
                    "options": {},
                    "type": "databricks:SqlEndpoint",
                },
            },
            "runtime": "yaml",
            "variables": {},
        }
    )


def test_terraform_stack(monkeypatch, stack):
    # To prevent from exposing sensitive data, we overwrite some env vars
    monkeypatch.setenv("DATABRICKS_HOST", "my-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "my-token")

    data_default = stack.to_terraform().model_dump()
    assert (
        data_default
        == {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "${vars.node_type_id}",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "config": '{"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"nodes": '
                            '[{"dlt_template": '
                            "null, "
                            '"name": '
                            '"first_node", '
                            '"source": '
                            '{"format": '
                            '"JSON", '
                            '"path": '
                            '"/tmp/"}}], '
                            '"orchestrator": '
                            '{"access_controls": '
                            '[{"group_name": '
                            '"account '
                            'users", '
                            '"permission_level": '
                            '"CAN_VIEW"}, '
                            '{"group_name": '
                            '"role-engineers", '
                            '"permission_level": '
                            '"CAN_RUN"}], '
                            '"configuration": '
                            '{"business_unit": '
                            '"laktory", '
                            '"workflow_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"pipeline_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"requirements": '
                            '"[\\"laktory==0.8.0\\"]", '
                            '"config": '
                            '"{\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"nodes\\": '
                            '[{\\"dlt_template\\": '
                            "null, "
                            '\\"name\\": '
                            '\\"first_node\\", '
                            '\\"source\\": '
                            '{\\"format\\": '
                            '\\"JSON\\", '
                            '\\"path\\": '
                            '\\"/tmp/\\"}}], '
                            '\\"orchestrator\\": '
                            '{\\"access_controls\\": '
                            '[{\\"group_name\\": '
                            '\\"account '
                            'users\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_VIEW\\"}, '
                            '{\\"group_name\\": '
                            '\\"role-engineers\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_RUN\\"}], '
                            '\\"configuration\\": '
                            '{\\"business_unit\\": '
                            '\\"laktory\\", '
                            '\\"workflow_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"pipeline_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"requirements\\": '
                            '\\"[\\\\\\"laktory==0.8.0\\\\\\"]\\", '
                            '\\"config\\": '
                            '\\"{\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"nodes\\\\\\": '
                            '[{\\\\\\"dlt_template\\\\\\": '
                            "null, "
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"first_node\\\\\\", '
                            '\\\\\\"source\\\\\\": '
                            '{\\\\\\"format\\\\\\": '
                            '\\\\\\"JSON\\\\\\", '
                            '\\\\\\"path\\\\\\": '
                            '\\\\\\"/tmp/\\\\\\"}}], '
                            '\\\\\\"orchestrator\\\\\\": '
                            '{\\\\\\"access_controls\\\\\\": '
                            '[{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"account '
                            'users\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_VIEW\\\\\\"}, '
                            '{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"role-engineers\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_RUN\\\\\\"}], '
                            '\\\\\\"configuration\\\\\\": '
                            '{\\\\\\"business_unit\\\\\\": '
                            '\\\\\\"laktory\\\\\\", '
                            '\\\\\\"workflow_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"pipeline_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"requirements\\\\\\": '
                            '\\\\\\"[\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\"]\\\\\\", '
                            '\\\\\\"config\\\\\\": '
                            '\\\\\\"{\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\"}}\\\\\\"}, '
                            '\\\\\\"libraries\\\\\\": '
                            '[{\\\\\\"notebook\\\\\\": '
                            '{\\\\\\"path\\\\\\": '
                            '\\\\\\"/pipelines/dlt_brz_template.py\\\\\\"}}], '
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"type\\\\\\": '
                            '\\\\\\"DATABRICKS_DLT\\\\\\"}}\\"}, '
                            '\\"libraries\\": '
                            '[{\\"notebook\\": '
                            '{\\"path\\": '
                            '\\"/pipelines/dlt_brz_template.py\\"}}], '
                            '\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"type\\": '
                            '\\"DATABRICKS_DLT\\"}}"}, '
                            '"libraries": '
                            '[{"notebook": '
                            '{"path": '
                            '"/pipelines/dlt_brz_template.py"}}], '
                            '"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"type": '
                            '"DATABRICKS_DLT"}}',
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "requirements": '["laktory==0.8.0"]',
                            "workflow_name": "pl-stock-prices-ut-stack",
                        },
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
            },
            "terraform": {
                "backend": {
                    "azurerm": {
                        "container_name": "unit-testing",
                        "key": "terraform/dev.terraform.tfstate",
                        "resource_group_name": "o3-rg-laktory-dev",
                        "storage_account_name": "o3stglaktorydev",
                    }
                },
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                },
            },
        }
        != {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "${vars.node_type_id}",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "workflow_name": "pl-stock-prices-ut-stack",
                            "workspace_laktory_root": "/.laktory/",
                        },
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
                "databricks_workspace_file": {},
            },
            "terraform": {
                "backend": {
                    "azurerm": {
                        "container_name": "unit-testing",
                        "key": "terraform/dev.terraform.tfstate",
                        "resource_group_name": "o3-rg-laktory-dev",
                        "storage_account_name": "o3stglaktorydev",
                    }
                },
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                },
            },
        }
    )

    # Dev
    data = stack.to_terraform(env_name="dev").model_dump()
    assert (
        data
        == {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "Standard_DS3_v2",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "dev",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "config": '{"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"nodes": '
                            '[{"dlt_template": '
                            "null, "
                            '"name": '
                            '"first_node", '
                            '"source": '
                            '{"format": '
                            '"JSON", '
                            '"path": '
                            '"/tmp/"}}], '
                            '"orchestrator": '
                            '{"access_controls": '
                            '[{"group_name": '
                            '"account '
                            'users", '
                            '"permission_level": '
                            '"CAN_VIEW"}, '
                            '{"group_name": '
                            '"role-engineers", '
                            '"permission_level": '
                            '"CAN_RUN"}], '
                            '"configuration": '
                            '{"business_unit": '
                            '"laktory", '
                            '"workflow_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"pipeline_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"requirements": '
                            '"[\\"laktory==0.8.0\\"]", '
                            '"config": '
                            '"{\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"nodes\\": '
                            '[{\\"dlt_template\\": '
                            "null, "
                            '\\"name\\": '
                            '\\"first_node\\", '
                            '\\"source\\": '
                            '{\\"format\\": '
                            '\\"JSON\\", '
                            '\\"path\\": '
                            '\\"/tmp/\\"}}], '
                            '\\"orchestrator\\": '
                            '{\\"access_controls\\": '
                            '[{\\"group_name\\": '
                            '\\"account '
                            'users\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_VIEW\\"}, '
                            '{\\"group_name\\": '
                            '\\"role-engineers\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_RUN\\"}], '
                            '\\"configuration\\": '
                            '{\\"business_unit\\": '
                            '\\"laktory\\", '
                            '\\"workflow_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"pipeline_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"requirements\\": '
                            '\\"[\\\\\\"laktory==0.8.0\\\\\\"]\\", '
                            '\\"config\\": '
                            '\\"{\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"nodes\\\\\\": '
                            '[{\\\\\\"dlt_template\\\\\\": '
                            "null, "
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"first_node\\\\\\", '
                            '\\\\\\"source\\\\\\": '
                            '{\\\\\\"format\\\\\\": '
                            '\\\\\\"JSON\\\\\\", '
                            '\\\\\\"path\\\\\\": '
                            '\\\\\\"/tmp/\\\\\\"}}], '
                            '\\\\\\"orchestrator\\\\\\": '
                            '{\\\\\\"access_controls\\\\\\": '
                            '[{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"account '
                            'users\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_VIEW\\\\\\"}, '
                            '{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"role-engineers\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_RUN\\\\\\"}], '
                            '\\\\\\"configuration\\\\\\": '
                            '{\\\\\\"business_unit\\\\\\": '
                            '\\\\\\"laktory\\\\\\", '
                            '\\\\\\"workflow_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"pipeline_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"requirements\\\\\\": '
                            '\\\\\\"[\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\"]\\\\\\", '
                            '\\\\\\"config\\\\\\": '
                            '\\\\\\"{\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\"}}\\\\\\"}, '
                            '\\\\\\"libraries\\\\\\": '
                            '[{\\\\\\"notebook\\\\\\": '
                            '{\\\\\\"path\\\\\\": '
                            '\\\\\\"/pipelines/dlt_brz_template.py\\\\\\"}}], '
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"type\\\\\\": '
                            '\\\\\\"DATABRICKS_DLT\\\\\\"}}\\"}, '
                            '\\"libraries\\": '
                            '[{\\"notebook\\": '
                            '{\\"path\\": '
                            '\\"/pipelines/dlt_brz_template.py\\"}}], '
                            '\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"type\\": '
                            '\\"DATABRICKS_DLT\\"}}"}, '
                            '"libraries": '
                            '[{"notebook": '
                            '{"path": '
                            '"/pipelines/dlt_brz_template.py"}}], '
                            '"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"type": '
                            '"DATABRICKS_DLT"}}',
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "requirements": '["laktory==0.8.0"]',
                            "workflow_name": "pl-stock-prices-ut-stack",
                        },
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
            },
            "terraform": {
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                }
            },
        }
        != {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "Standard_DS3_v2",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "dev",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "workflow_name": "pl-stock-prices-ut-stack",
                            "workspace_laktory_root": "/.laktory/",
                        },
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
                "databricks_workspace_file": {},
            },
            "terraform": {
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                }
            },
        }
    )

    # Prod
    data = stack.to_terraform(env_name="prod").model_dump()
    assert (
        data
        == {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "Standard_DS4_v2",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "prod",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "config": '{"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"nodes": '
                            '[{"dlt_template": '
                            "null, "
                            '"name": '
                            '"first_node", '
                            '"source": '
                            '{"format": '
                            '"JSON", '
                            '"path": '
                            '"/tmp/"}}], '
                            '"orchestrator": '
                            '{"access_controls": '
                            '[{"group_name": '
                            '"account '
                            'users", '
                            '"permission_level": '
                            '"CAN_VIEW"}, '
                            '{"group_name": '
                            '"role-engineers", '
                            '"permission_level": '
                            '"CAN_RUN"}], '
                            '"configuration": '
                            '{"business_unit": '
                            '"laktory", '
                            '"workflow_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"pipeline_name": '
                            '"pl-stock-prices-ut-stack", '
                            '"requirements": '
                            '"[\\"laktory==0.8.0\\"]", '
                            '"config": '
                            '"{\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"nodes\\": '
                            '[{\\"dlt_template\\": '
                            "null, "
                            '\\"name\\": '
                            '\\"first_node\\", '
                            '\\"source\\": '
                            '{\\"format\\": '
                            '\\"JSON\\", '
                            '\\"path\\": '
                            '\\"/tmp/\\"}}], '
                            '\\"orchestrator\\": '
                            '{\\"access_controls\\": '
                            '[{\\"group_name\\": '
                            '\\"account '
                            'users\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_VIEW\\"}, '
                            '{\\"group_name\\": '
                            '\\"role-engineers\\", '
                            '\\"permission_level\\": '
                            '\\"CAN_RUN\\"}], '
                            '\\"configuration\\": '
                            '{\\"business_unit\\": '
                            '\\"laktory\\", '
                            '\\"workflow_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"pipeline_name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"requirements\\": '
                            '\\"[\\\\\\"laktory==0.8.0\\\\\\"]\\", '
                            '\\"config\\": '
                            '\\"{\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"nodes\\\\\\": '
                            '[{\\\\\\"dlt_template\\\\\\": '
                            "null, "
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"first_node\\\\\\", '
                            '\\\\\\"source\\\\\\": '
                            '{\\\\\\"format\\\\\\": '
                            '\\\\\\"JSON\\\\\\", '
                            '\\\\\\"path\\\\\\": '
                            '\\\\\\"/tmp/\\\\\\"}}], '
                            '\\\\\\"orchestrator\\\\\\": '
                            '{\\\\\\"access_controls\\\\\\": '
                            '[{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"account '
                            'users\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_VIEW\\\\\\"}, '
                            '{\\\\\\"group_name\\\\\\": '
                            '\\\\\\"role-engineers\\\\\\", '
                            '\\\\\\"permission_level\\\\\\": '
                            '\\\\\\"CAN_RUN\\\\\\"}], '
                            '\\\\\\"configuration\\\\\\": '
                            '{\\\\\\"business_unit\\\\\\": '
                            '\\\\\\"laktory\\\\\\", '
                            '\\\\\\"workflow_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"pipeline_name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"requirements\\\\\\": '
                            '\\\\\\"[\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\"]\\\\\\", '
                            '\\\\\\"config\\\\\\": '
                            '\\\\\\"{\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pipeline_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"requirements\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"[\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory==0.8.0\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"]\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"config\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"nodes\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"dlt_template\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            "null, "
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"first_node\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"source\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"format\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"JSON\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/tmp/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"orchestrator\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"access_controls\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"account '
                            'users\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_VIEW\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"group_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"role-engineers\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"permission_level\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"CAN_RUN\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"configuration\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"business_unit\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"laktory\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"workflow_name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"}}\\\\\\\\\\\\\\"}, '
                            '\\\\\\\\\\\\\\"development\\\\\\\\\\\\\\": '
                            "false, "
                            '\\\\\\\\\\\\\\"libraries\\\\\\\\\\\\\\": '
                            '[{\\\\\\\\\\\\\\"notebook\\\\\\\\\\\\\\": '
                            '{\\\\\\\\\\\\\\"path\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"/pipelines/dlt_brz_template.py\\\\\\\\\\\\\\"}}], '
                            '\\\\\\\\\\\\\\"name\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"pl-stock-prices-ut-stack\\\\\\\\\\\\\\", '
                            '\\\\\\\\\\\\\\"type\\\\\\\\\\\\\\": '
                            '\\\\\\\\\\\\\\"DATABRICKS_DLT\\\\\\\\\\\\\\"}}\\\\\\"}, '
                            '\\\\\\"development\\\\\\": '
                            "false, "
                            '\\\\\\"libraries\\\\\\": '
                            '[{\\\\\\"notebook\\\\\\": '
                            '{\\\\\\"path\\\\\\": '
                            '\\\\\\"/pipelines/dlt_brz_template.py\\\\\\"}}], '
                            '\\\\\\"name\\\\\\": '
                            '\\\\\\"pl-stock-prices-ut-stack\\\\\\", '
                            '\\\\\\"type\\\\\\": '
                            '\\\\\\"DATABRICKS_DLT\\\\\\"}}\\"}, '
                            '\\"development\\": '
                            "false, "
                            '\\"libraries\\": '
                            '[{\\"notebook\\": '
                            '{\\"path\\": '
                            '\\"/pipelines/dlt_brz_template.py\\"}}], '
                            '\\"name\\": '
                            '\\"pl-stock-prices-ut-stack\\", '
                            '\\"type\\": '
                            '\\"DATABRICKS_DLT\\"}}"}, '
                            '"development": '
                            "false, "
                            '"libraries": '
                            '[{"notebook": '
                            '{"path": '
                            '"/pipelines/dlt_brz_template.py"}}], '
                            '"name": '
                            '"pl-stock-prices-ut-stack", '
                            '"type": '
                            '"DATABRICKS_DLT"}}',
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "requirements": '["laktory==0.8.0"]',
                            "workflow_name": "pl-stock-prices-ut-stack",
                        },
                        "development": False,
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
            },
            "terraform": {
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                }
            },
        }
        != {
            "data": {
                "databricks_notebook": {
                    "notebook-external": {
                        "format": "SOURCE",
                        "path": "/Workspace/external",
                    }
                },
                "databricks_sql_warehouse": {
                    "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
                },
            },
            "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
            "resource": {
                "databricks_job": {
                    "job-stock-prices-ut-stack": {
                        "job_cluster": [
                            {
                                "job_cluster_key": "main",
                                "new_cluster": {
                                    "data_security_mode": "USER_ISOLATION",
                                    "init_scripts": [],
                                    "node_type_id": "Standard_DS4_v2",
                                    "spark_conf": {},
                                    "spark_env_vars": {
                                        "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                        "LAKTORY_WORKSPACE_ENV": "prod",
                                    },
                                    "spark_version": "16.3.x-scala2.12",
                                    "ssh_public_keys": [],
                                },
                            }
                        ],
                        "name": "job-stock-prices-ut-stack",
                        "parameter": [],
                        "tags": {},
                        "task": [
                            {
                                "job_cluster_key": "main",
                                "library": [
                                    {"pypi": {"package": "laktory==0.0.27"}},
                                    {"pypi": {"package": "yfinance"}},
                                ],
                                "notebook_task": {
                                    "notebook_path": "/jobs/ingest_stock_metadata.py"
                                },
                                "task_key": "ingest-metadata",
                            },
                            {
                                "pipeline_task": {
                                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                                },
                                "task_key": "run-pipeline",
                            },
                        ],
                    }
                },
                "databricks_permissions": {
                    "permissions-dlt-custom-name": {
                        "access_control": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                            },
                        ],
                        "depends_on": ["databricks_pipeline.dlt-custom-name"],
                        "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                        "provider": "databricks",
                    },
                    "permissions-notebook-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_READ",
                            }
                        ],
                        "depends_on": ["data.databricks_notebook.notebook-external"],
                        "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    },
                    "permissions-warehouse-external": {
                        "access_control": [
                            {
                                "group_name": "role-analysts",
                                "permission_level": "CAN_USE",
                            }
                        ],
                        "depends_on": [
                            "data.databricks_sql_warehouse.warehouse-external"
                        ],
                        "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    },
                    "permissions_test": {
                        "access_control": [
                            {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                            {"permission_level": "CAN_RUN", "user_name": "user2"},
                        ],
                        "pipeline_id": "pipeline_123",
                    },
                },
                "databricks_pipeline": {
                    "dlt-custom-name": {
                        "channel": "PREVIEW",
                        "cluster": [],
                        "configuration": {
                            "business_unit": "laktory",
                            "pipeline_name": "pl-stock-prices-ut-stack",
                            "workflow_name": "pl-stock-prices-ut-stack",
                            "workspace_laktory_root": "/.laktory/",
                        },
                        "development": False,
                        "library": [
                            {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notification": [],
                        "provider": "databricks",
                    }
                },
                "databricks_workspace_file": {},
            },
            "terraform": {
                "required_providers": {
                    "databricks": {
                        "source": "databricks/databricks",
                        "version": ">=1.49",
                    }
                }
            },
        }
    )


@pytest.mark.parametrize("is_full", [True, False])
def test_terraform_plan(monkeypatch, stack, full_stack, is_full):
    if is_full:
        stack = full_stack

    c0 = settings.cli_raise_external_exceptions
    settings.cli_raise_external_exceptions = True

    # Pulumi requires valid Databricks Host and Token and Pulumi Token to run a preview.
    skip_terraform_plan()

    tstack = stack.to_terraform(env_name="dev")
    tstack.init(flags=["-reconfigure"])
    tstack.plan()

    settings.cli_raise_external_exceptions = c0


@pytest.mark.parametrize("is_full", [True, False])
def test_pulumi_preview(monkeypatch, stack, full_stack, is_full):
    if is_full:
        stack = full_stack

    c0 = settings.cli_raise_external_exceptions
    settings.cli_raise_external_exceptions = True

    # Pulumi requires valid Databricks Host and Token and Pulumi Token to run a preview.
    skip_pulumi_preview()

    _stack = stack.to_pulumi("dev")
    _stack.preview(stack="okube/dev")

    settings.cli_raise_external_exceptions = c0


def test_stack_settings():
    current_root = settings.laktory_root
    custom_root = "/custom/path/"

    assert settings.laktory_root != custom_root

    _ = models.Stack(name="one_stack", settings={"laktory_root": custom_root})

    assert settings.laktory_root == custom_root
    settings.laktory_root = current_root


def test_get_env():
    stack = models.Stack(
        name="stack-${vars.v0}-${vars.v1}",
        variables={
            "v0": "value0",
            "v1": "value1",
        },
        environments={
            "dev": {
                "variables": {
                    "v1": "dev",
                }
            },
            "prd": {
                "variables": {
                    "v1": "prd",
                }
            },
        },
    )

    dev = stack.get_env("dev")
    assert dev.name == "stack-${vars.v0}-${vars.v1}"

    dev = stack.get_env("dev").inject_vars()
    assert dev.name == "stack-value0-dev"

    prd = stack.get_env("prd").inject_vars()
    assert prd.name == "stack-value0-prd"
