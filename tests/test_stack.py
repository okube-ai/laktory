import copy
import os
from laktory import models

dirpath = os.path.dirname(__file__)

with open(os.path.join(dirpath, "stack.yaml"), "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)


def test_stack_model():
    data = stack.model_dump()
    print(data)
    assert data == {
        "variables": {},
        "config": {
            "databricks:host": "${vars.DATABRICKS_HOST}",
            "databricks:token": "${vars.DATABRICKS_TOKEN}",
        },
        "description": None,
        "name": "unit-testing",
        "backend": "pulumi",
        "pulumi_outputs": {},
        "resources": {
            "catalogs": {},
            "clusters": {},
            "directories": {},
            "groups": {},
            "jobs": {
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
                                "pipeline_id": "${resources.pl-custom-name.id}",
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
            "notebooks": {},
            "pipelines": {
                "pl-custom-name": {
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
                    "tables": [],
                    "target": None,
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
                "databricks-provider": {
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
        "environments": {
            "dev": {
                "variables": {
                    "env": "dev",
                    "is_dev": True,
                    "node_type_id": "Standard_DS3_v2",
                },
                "config": None,
                "resources": None,
            },
            "prod": {
                "variables": {
                    "env": "prod",
                    "is_dev": False,
                    "node_type_id": "Standard_DS4_v2",
                },
                "config": None,
                "resources": {"pipelines": {"pl-custom-name": {"development": False}}},
            },
        },
    }

    return stack


def test_pulumi_stack():
    data_default = stack.to_pulumi(env=None).model_dump()
    data_default["config"]["databricks:token"] = "***"
    data_default["resources"]["databricks-provider"]["properties"]["token"] = "***"
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
                            "pipelineTask": {"pipelineId": "${pl-custom-name.id}"},
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
            "pl-custom-name": {
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
                    "provider": "${databricks-provider}",
                    "dependsOn": [],
                    "deleteBeforeReplace": True,
                },
            },
            "permissions-pl-custom-name": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "account users", "permissionLevel": "CAN_VIEW"},
                        {"groupName": "role-engineers", "permissionLevel": "CAN_RUN"},
                    ],
                    "pipelineId": "${pl-custom-name.id}",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:WorkspaceFile",
                "properties": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                },
                "options": {"dependsOn": [], "deleteBeforeReplace": True},
            },
            "permissions-file-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                "type": "databricks:Permissions",
                "properties": {
                    "accessControls": [
                        {"groupName": "account users", "permissionLevel": "CAN_READ"}
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
            "databricks-provider": {
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

    # Dev
    data = stack.to_pulumi(env="dev").model_dump()
    data["config"]["databricks:token"] = "***"
    data["resources"]["databricks-provider"]["properties"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    data0["variables"]["env"] = "dev"
    data0["variables"]["is_dev"] = True
    data0["variables"]["node_type_id"] = "Standard_DS3_v2"
    cluster = data0["resources"]["job-stock-prices-ut-stack"]["properties"]["jobClusters"][0]["newCluster"]
    cluster["nodeTypeId"] = "Standard_DS3_v2"
    cluster["sparkEnvVars"]["LAKTORY_WORKSPACE_ENV"] = "dev"
    assert data == data0

    # Prod
    data = stack.to_pulumi(env="prod").model_dump()
    data["config"]["databricks:token"] = "***"
    data["resources"]["databricks-provider"]["properties"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    data0["variables"]["env"] = "prod"
    data0["variables"]["is_dev"] = False
    data0["variables"]["node_type_id"] = "Standard_DS4_v2"
    cluster = data0["resources"]["job-stock-prices-ut-stack"]["properties"]["jobClusters"][0]["newCluster"]
    cluster["nodeTypeId"] = "Standard_DS4_v2"
    cluster["sparkEnvVars"]["LAKTORY_WORKSPACE_ENV"] = "prod"
    data0["resources"]["pl-custom-name"]["properties"]["development"] = False
    assert data == data0


if __name__ == "__main__":
    test_stack_model()
    test_pulumi_stack()
