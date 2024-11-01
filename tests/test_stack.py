import copy
import os

from laktory import models
from laktory._testing.stackvalidator import StackValidator
from laktory._testing import Paths

paths = Paths(__file__)

with open(os.path.join(paths.data, "stack.yaml"), "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)

stack.terraform.backend = {
    "azurerm": {
        "resource_group_name": "o3-rg-laktory-dev",
        "storage_account_name": "o3stglaktorydev",
        "container_name": "unit-testing",
        "key": "terraform/dev.terraform.tfstate",
    }
}


def test_stack_model():
    data = stack.model_dump()
    print(data)
    assert data == {
        "variables": {},
        "backend": "pulumi",
        "description": None,
        "environments": {
            "dev": {
                "variables": {
                    "env": "dev",
                    "is_dev": True,
                    "node_type_id": "Standard_DS3_v2",
                },
                "resources": None,
            },
            "prod": {
                "variables": {
                    "env": "prod",
                    "is_dev": False,
                    "node_type_id": "Standard_DS4_v2",
                },
                "resources": {
                    "pipelines": {"pl-custom-name": {"dlt": {"development": False}}}
                },
            },
        },
        "name": "unit-testing",
        "organization": "okube",
        "pulumi": {
            "config": {
                "databricks:host": "${vars.DATABRICKS_HOST}",
                "databricks:token": "${vars.DATABRICKS_TOKEN}",
            },
            "outputs": {},
        },
        "resources": {
            "databricks_dashboards": {},
            "databricks_dbfsfiles": {},
            "databricks_catalogs": {},
            "databricks_clusters": {},
            "databricks_directories": {},
            "databricks_externallocations": {},
            "databricks_grants": {},
            "databricks_groups": {},
            "databricks_jobs": {
                "job-stock-prices-ut-stack": {
                    "access_controls": [],
                    "clusters": [
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
                    "continuous": None,
                    "control_run_state": None,
                    "description": None,
                    "email_notifications": None,
                    "format": None,
                    "health": None,
                    "max_concurrent_runs": None,
                    "max_retries": None,
                    "min_retry_interval_millis": None,
                    "name": "job-stock-prices-ut-stack",
                    "notification_settings": None,
                    "parameters": [],
                    "retry_on_timeout": None,
                    "run_as": None,
                    "schedule": None,
                    "tags": {},
                    "tasks": [
                        {
                            "condition_task": None,
                            "depends_ons": None,
                            "description": None,
                            "email_notifications": None,
                            "existing_cluster_id": None,
                            "health": None,
                            "job_cluster_key": "main",
                            "libraries": [
                                {
                                    "cran": None,
                                    "egg": None,
                                    "jar": None,
                                    "maven": None,
                                    "pypi": {
                                        "package": "laktory==0.0.27",
                                        "repo": None,
                                    },
                                    "whl": None,
                                },
                                {
                                    "cran": None,
                                    "egg": None,
                                    "jar": None,
                                    "maven": None,
                                    "pypi": {"package": "yfinance", "repo": None},
                                    "whl": None,
                                },
                            ],
                            "max_retries": None,
                            "min_retry_interval_millis": None,
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py",
                                "base_parameters": None,
                                "warehouse_id": None,
                                "source": None,
                            },
                            "notification_settings": None,
                            "pipeline_task": None,
                            "retry_on_timeout": None,
                            "run_if": None,
                            "run_job_task": None,
                            "sql_task": None,
                            "task_key": "ingest-metadata",
                            "timeout_seconds": None,
                        },
                        {
                            "condition_task": None,
                            "depends_ons": None,
                            "description": None,
                            "email_notifications": None,
                            "existing_cluster_id": None,
                            "health": None,
                            "job_cluster_key": None,
                            "libraries": None,
                            "max_retries": None,
                            "min_retry_interval_millis": None,
                            "notebook_task": None,
                            "notification_settings": None,
                            "pipeline_task": {
                                "pipeline_id": "${resources.dlt-custom-name.id}",
                                "full_refresh": None,
                            },
                            "retry_on_timeout": None,
                            "run_if": None,
                            "run_job_task": None,
                            "sql_task": None,
                            "task_key": "run-pipeline",
                            "timeout_seconds": None,
                        },
                    ],
                    "timeout_seconds": None,
                    "trigger": None,
                    "webhook_notifications": None,
                }
            },
            "databricks_metastoredataaccesses": {},
            "databricks_metastores": {},
            "databricks_networkconnectivityconfig": {},
            "databricks_notebooks": {
                "notebook-external": {
                    "access_controls": [
                        {
                            "group_name": "role-analysts",
                            "permission_level": "CAN_READ",
                            "service_principal_name": None,
                            "user_name": None,
                        }
                    ],
                    "rootpath": "/.laktory/",
                    "dirpath": "",
                    "language": None,
                    "path": "/.laktory",
                    "source": "",
                }
            },
            "databricks_dltpipelines": {},
            "databricks_schemas": {},
            "databricks_secrets": {},
            "databricks_secretscopes": {},
            "databricks_serviceprincipals": {},
            "databricks_sqlqueries": {},
            "databricks_tables": {},
            "databricks_users": {},
            "databricks_volumes": {},
            "databricks_vectorsearchendpoints": {},
            "databricks_vectorsearchindexes": {},
            "databricks_warehouses": {
                "warehouse-external": {
                    "cluster_size": "2X-Small",
                    "access_controls": [
                        {
                            "group_name": "role-analysts",
                            "permission_level": "CAN_USE",
                            "service_principal_name": None,
                            "user_name": None,
                        }
                    ],
                    "auto_stop_mins": None,
                    "channel_name": None,
                    "enable_photon": None,
                    "enable_serverless_compute": None,
                    "instance_profile_arn": None,
                    "jdbc_url": None,
                    "max_num_clusters": None,
                    "min_num_clusters": None,
                    "name": "",
                    "num_clusters": None,
                    "spot_instance_policy": None,
                    "tags": None,
                    "warehouse_type": None,
                }
            },
            "databricks_workspacefiles": {},
            "pipelines": {
                "pl-custom-name": {
                    "databricks_job": None,
                    "dataframe_type": "SPARK",
                    "dlt": {
                        "access_controls": [
                            {
                                "group_name": "account users",
                                "permission_level": "CAN_VIEW",
                                "service_principal_name": None,
                                "user_name": None,
                            },
                            {
                                "group_name": "role-engineers",
                                "permission_level": "CAN_RUN",
                                "service_principal_name": None,
                                "user_name": None,
                            },
                        ],
                        "allow_duplicate_names": None,
                        "catalog": None,
                        "channel": "PREVIEW",
                        "clusters": [],
                        "configuration": {},
                        "continuous": None,
                        "development": None,
                        "edition": None,
                        "libraries": [
                            {
                                "file": None,
                                "notebook": {"path": "/pipelines/dlt_brz_template.py"},
                            }
                        ],
                        "name": "pl-stock-prices-ut-stack",
                        "notifications": [],
                        "photon": None,
                        "serverless": None,
                        "storage": None,
                        "target": None,
                    },
                    "name": "pl-stock-prices-ut-stack",
                    "nodes": [
                        {
                            "add_layer_columns": True,
                            "dlt_template": None,
                            "dataframe_type": "SPARK",
                            "description": None,
                            "drop_duplicates": None,
                            "drop_source_columns": None,
                            "transformer": None,
                            "expectations": [],
                            "expectations_checkpoint_location": None,
                            "layer": None,
                            "name": "first_node",
                            "primary_key": None,
                            "sinks": None,
                            "root_path": None,
                            "source": {
                                "as_stream": False,
                                "broadcast": False,
                                "cdc": None,
                                "dataframe_type": "SPARK",
                                "drops": None,
                                "filter": None,
                                "limit": None,
                                "renames": None,
                                "sample": None,
                                "selects": None,
                                "watermark": None,
                                "format": "JSON",
                                "header": True,
                                "multiline": False,
                                "path": "/tmp/",
                                "read_options": {},
                                "schema_location": None,
                            },
                            "timestamp_key": None,
                        }
                    ],
                    "orchestrator": "DLT",
                    "udfs": [],
                    "root_path": None,
                    "workspacefile": None,
                }
            },
            "providers": {
                "databricks": {
                    "alias": None,
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
        },
        "terraform": {
            "backend": {
                "azurerm": {
                    "resource_group_name": "o3-rg-laktory-dev",
                    "storage_account_name": "o3stglaktorydev",
                    "container_name": "unit-testing",
                    "key": "terraform/dev.terraform.tfstate",
                }
            }
        },
    }

    return stack


def test_stack_env_model():

    # dev
    _stack = stack.get_env("dev")
    pl = _stack.resources.pipelines["pl-custom-name"]
    assert _stack.variables == {
        "env": "dev",
        "is_dev": True,
        "node_type_id": "Standard_DS3_v2",
    }
    assert pl.dlt.development is None
    assert pl.nodes[0].dlt_template is None

    # prod
    _stack = stack.get_env("prod")
    pl = _stack.resources.pipelines["pl-custom-name"]
    assert _stack.variables == {
        "env": "prod",
        "is_dev": False,
        "node_type_id": "Standard_DS4_v2",
    }
    assert pl.dlt.development == False
    assert pl.nodes[0].dlt_template is None


def test_pulumi_stack():
    pstack = stack.to_pulumi(env_name=None)
    assert pstack.organization == "okube"
    data_default = pstack.model_dump()
    data_default["config"]["databricks:token"] = "***"
    data_default["resources"]["databricks"]["properties"]["token"] = "***"
    print(data_default)
    assert data_default == {
        "variables": {},
        "name": "unit-testing",
        "runtime": "yaml",
        "config": {
            "databricks:host": "https://adb-2211091707396001.1.azuredatabricks.net/",
            "databricks:token": "***",
        },
        "resources": {
            "job-stock-prices-ut-stack": {
                "type": "databricks:Job",
                "properties": {
                    "name": "job-stock-prices-ut-stack",
                    "parameters": [],
                    "tags": {},
                    "tasks": [
                        {
                            "libraries": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "jobClusterKey": "main",
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
                                "sparkVersion": "14.0.x-scala2.12",
                                "sshPublicKeys": [],
                            },
                        }
                    ],
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "notebook-external": {
                "type": "databricks:Notebook",
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
                "get": {"id": "/Workspace/external"},
            },
            "permissions-notebook-external": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_READ"}
                    ],
                    "notebookPath": "${notebook-external.path}",
                },
                "options": {
                    "dependsOn": ["${notebook-external}"],
                    "deleteBeforeReplace": True,
                },
            },
            "warehouse-external": {
                "type": "databricks:SqlEndpoint",
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
                "get": {"id": "d2fa41bf94858c4b"},
            },
            "permissions-warehouse-external": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_USE"}
                    ],
                    "sqlEndpointId": "${warehouse-external.id}",
                },
                "options": {
                    "dependsOn": ["${warehouse-external}"],
                    "deleteBeforeReplace": True,
                },
            },
            "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:WorkspaceFile",
                "properties": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "users", "permissionLevel": "CAN_READ"}
                    ],
                    "workspaceFilePath": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                },
                "options": {
                    "dependsOn": [
                        "${workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json}"
                    ],
                    "deleteBeforeReplace": True,
                },
            },
            "dlt-custom-name": {
                "type": "databricks:Pipeline",
                "properties": {
                    "channel": "PREVIEW",
                    "clusters": [],
                    "configuration": {},
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "name": "pl-stock-prices-ut-stack",
                    "notifications": [],
                },
                "options": {
                    "provider": "${databricks}",
                    "dependsOn": [],
                    "deleteBeforeReplace": True,
                },
            },
            "permissions-dlt-custom-name": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "account users", "permissionLevel": "CAN_VIEW"},
                        {"groupName": "role-engineers", "permissionLevel": "CAN_RUN"},
                    ],
                    "pipelineId": "${dlt-custom-name.id}",
                },
                "options": {
                    "provider": "${databricks}",
                    "dependsOn": ["${dlt-custom-name}"],
                    "deleteBeforeReplace": True,
                },
            },
            "databricks": {
                "type": "pulumi:providers:databricks",
                "properties": {
                    "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
                    "token": "***",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
        },
        "outputs": {},
    }

    # Prod
    data = stack.to_pulumi(env_name="prod").model_dump()
    data["config"]["databricks:token"] = "***"
    data["resources"]["databricks"]["properties"]["token"] = "***"
    print(data)
    assert data == {
        "variables": {
            "env": "prod",
            "is_dev": False,
            "node_type_id": "Standard_DS4_v2",
        },
        "name": "unit-testing",
        "runtime": "yaml",
        "config": {
            "databricks:host": "https://adb-2211091707396001.1.azuredatabricks.net/",
            "databricks:token": "***",
        },
        "resources": {
            "job-stock-prices-ut-stack": {
                "type": "databricks:Job",
                "properties": {
                    "name": "job-stock-prices-ut-stack",
                    "parameters": [],
                    "tags": {},
                    "tasks": [
                        {
                            "libraries": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "jobClusterKey": "main",
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
                                "sparkVersion": "14.0.x-scala2.12",
                                "sshPublicKeys": [],
                            },
                        }
                    ],
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "notebook-external": {
                "type": "databricks:Notebook",
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
                "get": {"id": "/Workspace/external"},
            },
            "permissions-notebook-external": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_READ"}
                    ],
                    "notebookPath": "${notebook-external.path}",
                },
                "options": {
                    "dependsOn": ["${notebook-external}"],
                    "deleteBeforeReplace": True,
                },
            },
            "warehouse-external": {
                "type": "databricks:SqlEndpoint",
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
                "get": {"id": "d2fa41bf94858c4b"},
            },
            "permissions-warehouse-external": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "role-analysts", "permissionLevel": "CAN_USE"}
                    ],
                    "sqlEndpointId": "${warehouse-external.id}",
                },
                "options": {
                    "dependsOn": ["${warehouse-external}"],
                    "deleteBeforeReplace": True,
                },
            },
            "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:WorkspaceFile",
                "properties": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "users", "permissionLevel": "CAN_READ"}
                    ],
                    "workspaceFilePath": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                },
                "options": {
                    "dependsOn": [
                        "${workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json}"
                    ],
                    "deleteBeforeReplace": True,
                },
            },
            "dlt-custom-name": {
                "type": "databricks:Pipeline",
                "properties": {
                    "channel": "PREVIEW",
                    "clusters": [],
                    "configuration": {},
                    "development": False,
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "name": "pl-stock-prices-ut-stack",
                    "notifications": [],
                },
                "options": {
                    "provider": "${databricks}",
                    "dependsOn": [],
                    "deleteBeforeReplace": True,
                },
            },
            "permissions-dlt-custom-name": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "account users", "permissionLevel": "CAN_VIEW"},
                        {"groupName": "role-engineers", "permissionLevel": "CAN_RUN"},
                    ],
                    "pipelineId": "${dlt-custom-name.id}",
                },
                "options": {
                    "provider": "${databricks}",
                    "dependsOn": ["${dlt-custom-name}"],
                    "deleteBeforeReplace": True,
                },
            },
            "databricks": {
                "type": "pulumi:providers:databricks",
                "properties": {
                    "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
                    "token": "***",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
        },
        "outputs": {},
    }


def test_pulumi_preview():
    pstack = stack.to_pulumi(env_name="dev")
    pstack.preview(stack="okube/dev")


def test_terraform_stack():
    data_default = stack.to_terraform().model_dump()
    data_default["provider"]["databricks"]["token"] = "***"
    print(data_default)
    assert data_default == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            },
            "backend": {
                "azurerm": {
                    "resource_group_name": "o3-rg-laktory-dev",
                    "storage_account_name": "o3stglaktorydev",
                    "container_name": "unit-testing",
                    "key": "terraform/dev.terraform.tfstate",
                }
            },
        },
        "provider": {
            "databricks": {
                "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
                "token": "***",
            }
        },
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "tags": {},
                    "parameter": [],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                        },
                        {
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                            "task_key": "run-pipeline",
                        },
                    ],
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
                                "spark_version": "14.0.x-scala2.12",
                                "ssh_public_keys": [],
                            },
                        }
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions-warehouse-external": {
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                },
                "permissions-dlt-custom-name": {
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                }
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "channel": "PREVIEW",
                    "configuration": {},
                    "name": "pl-stock-prices-ut-stack",
                    "cluster": [],
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "notification": [],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b"}
            },
        },
    }

    # Dev
    data = stack.to_terraform(env_name="dev").model_dump()
    data["provider"]["databricks"]["token"] = "***"
    print(data)
    assert data == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            }
        },
        "provider": {
            "databricks": {
                "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
                "token": "***",
            }
        },
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "tags": {},
                    "parameter": [],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                        },
                        {
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                            "task_key": "run-pipeline",
                        },
                    ],
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
                                "spark_version": "14.0.x-scala2.12",
                                "ssh_public_keys": [],
                            },
                        }
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions-warehouse-external": {
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                },
                "permissions-dlt-custom-name": {
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                }
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "channel": "PREVIEW",
                    "configuration": {},
                    "name": "pl-stock-prices-ut-stack",
                    "cluster": [],
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "notification": [],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b"}
            },
        },
    }

    # Prod
    data = stack.to_terraform(env_name="prod").model_dump()
    data["provider"]["databricks"]["token"] = "***"
    print(data)
    assert data == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            }
        },
        "provider": {
            "databricks": {
                "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
                "token": "***",
            }
        },
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "tags": {},
                    "parameter": [],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                        },
                        {
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                            "task_key": "run-pipeline",
                        },
                    ],
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
                                "spark_version": "14.0.x-scala2.12",
                                "ssh_public_keys": [],
                            },
                        }
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions-warehouse-external": {
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                },
                "permissions-dlt-custom-name": {
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                }
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "channel": "PREVIEW",
                    "configuration": {},
                    "development": False,
                    "name": "pl-stock-prices-ut-stack",
                    "cluster": [],
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "notification": [],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b"}
            },
        },
    }


def test_terraform_plan():
    tstack = stack.to_terraform(env_name="dev")
    tstack.terraform.backend = (
        None  # TODO: Add credentials to git actions to use azure backend
    )
    tstack.init(flags=["-migrate-state", "-upgrade"])
    tstack.plan()


def test_all_resources():
    from tests.test_catalog import catalog
    from tests.test_directory import directory
    from tests.test_dashboard import dashboard
    from tests.test_job import job
    from tests.test_pipeline_orchestrators import pl_dlt
    from tests.test_metastore import metastore
    from tests.test_notebook import nb
    from tests.test_schema import schema
    from tests.test_sql_query import query
    from tests.test_user import user
    from tests.test_user import group
    from tests.test_workspacefile import workspace_file
    from tests.test_vectorsearchendpoint import vector_search_endpoint
    from tests.test_vectorsearchindex import vector_search_index
    from laktory._testing import Paths

    paths = Paths(__file__)

    nb.source = os.path.join(paths.root, nb.source)
    workspace_file.source = os.path.join(paths.root, workspace_file.source)

    validator = StackValidator(
        resources={
            "databricks_catalogs": [catalog],
            "databricks_dashboards": [dashboard],
            "databricks_directories": [directory],
            "databricks_jobs": [job],
            "databricks_metastores": [metastore],
            "databricks_notebooks": [nb],
            "databricks_schemas": [schema],
            "databricks_sqlqueries": [query],
            "databricks_groups": [group],
            "databricks_users": [user],
            "databricks_vectorsearchendpoints": [vector_search_endpoint],
            "databricks_vectorsearchindexes": [vector_search_index],
            "databricks_workspacefiles": [workspace_file],
            "pipelines": [pl_dlt],  # required by job
        },
        providers={
            "provider-workspace-neptune": {
                "host": "${vars.DATABRICKS_HOST}",
                # "azure_client_id": "0",
                # "azure_client_secret": "0",
                # "azure_tenant_id": "0",
            },
            "databricks1": {
                "host": "${vars.DATABRICKS_HOST}",
            },
            "databricks2": {
                "host": "${vars.DATABRICKS_HOST}",
            },
        },
    )
    validator.validate()


if __name__ == "__main__":
    test_stack_model()
    test_stack_env_model()
    test_pulumi_stack()
    test_pulumi_preview()
    test_terraform_stack()
    test_terraform_plan()
    test_all_resources()
