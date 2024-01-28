import copy
import os
from laktory import models

dirpath = os.path.dirname(__file__)

with open(os.path.join(dirpath, "stack.yaml"), "r") as fp:
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
                "resources": {"pipelines": {"pl-custom-name": {"development": False}}},
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


def test_pulumi_stack():
    pstack = stack.to_pulumi(env=None)
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
                    "provider": "${databricks}",
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

    # Dev
    data = stack.to_pulumi(env="dev").model_dump()
    data["config"]["databricks:token"] = "***"
    data["resources"]["databricks"]["properties"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    data0["variables"]["env"] = "dev"
    data0["variables"]["is_dev"] = True
    data0["variables"]["node_type_id"] = "Standard_DS3_v2"
    cluster = data0["resources"]["job-stock-prices-ut-stack"]["properties"][
        "jobClusters"
    ][0]["newCluster"]
    cluster["nodeTypeId"] = "Standard_DS3_v2"
    cluster["sparkEnvVars"]["LAKTORY_WORKSPACE_ENV"] = "dev"
    assert data == data0

    # Prod
    data = stack.to_pulumi(env="prod").model_dump()
    data["config"]["databricks:token"] = "***"
    data["resources"]["databricks"]["properties"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    data0["variables"]["env"] = "prod"
    data0["variables"]["is_dev"] = False
    data0["variables"]["node_type_id"] = "Standard_DS4_v2"
    cluster = data0["resources"]["job-stock-prices-ut-stack"]["properties"][
        "jobClusters"
    ][0]["newCluster"]
    cluster["nodeTypeId"] = "Standard_DS4_v2"
    cluster["sparkEnvVars"]["LAKTORY_WORKSPACE_ENV"] = "prod"
    data0["resources"]["pl-custom-name"]["properties"]["development"] = False
    assert data == data0


def test_pulumi_preview():
    pstack = stack.to_pulumi(env="dev")
    pstack.preview(stack="okube/dev")


def test_terraform_stack():
    data_default = stack.to_terraform().model_dump()
    data_default["provider"]["databricks"]["token"] = "***"
    print(data_default)
    assert data_default == {
        "terraform": {
            "required_providers": {"databricks": {"source": "databricks/databricks"}},
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
                                "pipeline_id": "${databricks_pipeline.pl-custom-name.id}"
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
            "databricks_pipeline": {
                "pl-custom-name": {
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
            "databricks_permissions": {
                "permissions-pl-custom-name": {
                    "pipeline_id": "${databricks_pipeline.pl-custom-name.id}",
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                },
                "permissions-file-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_READ"}
                    ],
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                },
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "source": "./tmp-pl-stock-prices-ut-stack.json",
                }
            },
        },
    }

    # Dev
    data = stack.to_terraform(env="dev").model_dump()
    data["provider"]["databricks"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    print(data0["resource"]["databricks_job"]["job-stock-prices-ut-stack"])
    cluster = data0["resource"]["databricks_job"]["job-stock-prices-ut-stack"][
        "job_cluster"
    ][0]["new_cluster"]
    cluster["node_type_id"] = "Standard_DS3_v2"
    cluster["spark_env_vars"]["LAKTORY_WORKSPACE_ENV"] = "dev"
    assert data == data0

    # Prod
    data = stack.to_terraform(env="prod").model_dump()
    data["provider"]["databricks"]["token"] = "***"
    data0 = copy.deepcopy(data_default)
    cluster = data0["resource"]["databricks_job"]["job-stock-prices-ut-stack"][
        "job_cluster"
    ][0]["new_cluster"]
    cluster["node_type_id"] = "Standard_DS4_v2"
    cluster["spark_env_vars"]["LAKTORY_WORKSPACE_ENV"] = "prod"
    data0["resource"]["databricks_pipeline"]["pl-custom-name"]["development"] = False
    assert data == data0


def test_terraform_plan():
    tstack = stack.to_terraform(env="dev")
    tstack.terraform.backend = None  # TODO: Add credentials to git actions to use azure backend
    tstack.init(flags=["-migrate-state"])
    tstack.plan()


if __name__ == "__main__":
    test_stack_model()
    test_pulumi_stack()
    test_pulumi_preview()
    test_terraform_stack()
    test_terraform_plan()
