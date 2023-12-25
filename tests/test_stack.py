import os
from laktory import models

pipeline = models.Pipeline(
    name="pl-stock-prices-stack",
    libraries=[
        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}},
    ],
    permissions=[
        {"group_name": "account users", "permission_level": "CAN_VIEW"},
        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
    ],
)

job = models.Job(
    name="job-stock-prices-stack",
    clusters=[
        {
            "name": "main",
            "spark_version": "14.0.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
        }
    ],
    tasks=[
        {
            "task_key": "ingest-metadata",
            "job_cluster_key": "main",
            "notebook_task": {
                "notebook_path": "/jobs/ingest_stock_metadata.py",
            },
            "libraries": [
                {"pypi": {"package": "laktory==0.0.27"}},
                {"pypi": {"package": "yfinance"}},
            ],
        },
        {
            "task_key": "run-pipeline",
            "pipeline_task": {
                # "pipeline_id": "${resources.pipelines.pl-stocks-prices-stack.id}",  # BUNDLES STYLE  {resources.resource_type.resource_key.id}
                # "pipeline_id": "${databricks_pipeline.lhouse_project_pipeline.id}",  # TERRAFORM STYLE {resource_type.resource_key.id}
                # "pipeline_id": "${pl-stocks-prices-stack.id}",  # PULUMI STYLE {resource_key.id}
                "pipeline_id": "${pipelines.pl-stock-prices.id}",  # LAKTORY STYLE {resource_type.resource_key.id}
            },
            "libraries": [
                {"pypi": {"package": "laktory==0.0.27"}},
                {"pypi": {"package": "yfinance"}},
            ],
        },
    ],
)


stack = models.Stack(
    name="workspace",
    resources={
        "jobs": [job],
        "pipelines": [pipeline],
    },
)


def test_stack_model():
    data = stack.model_dump()
    print(data)
    assert data == {
        "name": "workspace",
        "description": None,
        "resources": {
            "catalogs": [],
            "cluster": [],
            "groups": [],
            "jobs": [
                {
                    "resource_name": "job-job-stock-prices-stack",
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
                            "is_pinned": None,
                            "libraries": None,
                            "name": "main",
                            "node_type_id": "Standard_DS3_v2",
                            "num_workers": None,
                            "permissions": None,
                            "policy_id": None,
                            "runtime_engine": None,
                            "single_user_name": None,
                            "spark_conf": {},
                            "spark_env_vars": {},
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
                    "name": "job-stock-prices-stack",
                    "notification_settings": None,
                    "parameters": [],
                    "permissions": [],
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
                            "notebook_task": None,
                            "notification_settings": None,
                            "pipeline_task": {
                                "pipeline_id": "${pipelines.pl-stock-prices.id}",
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
            ],
            "notebooks": [],
            "pipelines": [
                {
                    "resource_name": "pipeline-pl-stock-prices-stack",
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
                    "name": "pl-stock-prices-stack",
                    "notifications": [],
                    "permissions": [
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
                    "photon": None,
                    "serverless": None,
                    "storage": None,
                    "tables": [],
                    "target": None,
                    "udfs": [],
                }
            ],
            "schemas": [],
            "secret_scopes": [],
            "sql_queries": [],
            "tables": [],
            "users": [],
            "warehouse": [],
            "workspace_files": [],
        },
        "environments": [],
        "variables": {},
        "pulumi_outputs": {},
    }


def test_pulumi_stack():
    pstack = stack.to_pulumi_stack()
    stack.write_pulumi_stack()
    data = pstack.model_dump()
    print(data)

    assert data == {
        "name": "workspace",
        "runtime": "yaml",
        "config": {},
        "variables": {},
        "resources": {
            "job-job-stock-prices-stack": {
                "type": "databricks:Job",
                "properties": {
                    "continuous": None,
                    "format": None,
                    "health": None,
                    "name": "job-stock-prices-stack",
                    "parameters": [],
                    "schedule": None,
                    "tags": {},
                    "tasks": [
                        {
                            "description": None,
                            "health": None,
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
                            "conditionTask": None,
                            "dependsOns": None,
                            "emailNotifications": None,
                            "existingClusterId": None,
                            "jobClusterKey": "main",
                            "maxRetries": None,
                            "minRetryIntervalMillis": None,
                            "notebookTask": {
                                "source": None,
                                "notebookPath": "/jobs/ingest_stock_metadata.py",
                                "baseParameters": None,
                            },
                            "notificationSettings": None,
                            "pipelineTask": None,
                            "retryOnTimeout": None,
                            "runIf": None,
                            "runJobTask": None,
                            "sqlTask": None,
                            "taskKey": "ingest-metadata",
                            "timeoutSeconds": None,
                        },
                        {
                            "description": None,
                            "health": None,
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
                            "conditionTask": None,
                            "dependsOns": None,
                            "emailNotifications": None,
                            "existingClusterId": None,
                            "jobClusterKey": None,
                            "maxRetries": None,
                            "minRetryIntervalMillis": None,
                            "notebookTask": None,
                            "notificationSettings": None,
                            "pipelineTask": {
                                "pipelineId": "${pl-stock-prices.id}",
                                "fullRefresh": None,
                            },
                            "retryOnTimeout": None,
                            "runIf": None,
                            "runJobTask": None,
                            "sqlTask": None,
                            "taskKey": "run-pipeline",
                            "timeoutSeconds": None,
                        },
                    ],
                    "trigger": None,
                    "resourceName": "job-job-stock-prices-stack",
                    "controlRunState": None,
                    "emailNotifications": None,
                    "maxConcurrentRuns": None,
                    "maxRetries": None,
                    "minRetryIntervalMillis": None,
                    "notificationSettings": None,
                    "retryOnTimeout": None,
                    "runAs": None,
                    "timeoutSeconds": None,
                    "webhookNotifications": None,
                    "jobClusters": [
                        {
                            "jobClusterKey": "main",
                            "newCluster": {
                                "autoscale": None,
                                "libraries": None,
                                "permissions": None,
                                "applyPolicyDefaultValues": None,
                                "autoterminationMinutes": None,
                                "clusterId": None,
                                "customTags": None,
                                "dataSecurityMode": "USER_ISOLATION",
                                "driverInstancePoolId": None,
                                "driverNodeTypeId": None,
                                "enableElasticDisk": None,
                                "enableLocalDiskEncryption": None,
                                "idempotencyToken": None,
                                "initScripts": [],
                                "instancePoolId": None,
                                "isPinned": None,
                                "nodeTypeId": "Standard_DS3_v2",
                                "numWorkers": None,
                                "policyId": None,
                                "runtimeEngine": None,
                                "singleUserName": None,
                                "sparkConf": {},
                                "sparkEnvVars": {},
                                "sparkVersion": "14.0.x-scala2.12",
                                "sshPublicKeys": [],
                            },
                        }
                    ],
                },
            },
            "pipeline-pl-stock-prices-stack": {
                "type": "databricks:Pipeline",
                "properties": {
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
                    "name": "pl-stock-prices-stack",
                    "notifications": [],
                    "photon": None,
                    "serverless": None,
                    "storage": None,
                    "target": None,
                    "resourceName": "pipeline-pl-stock-prices-stack",
                    "allowDuplicateNames": None,
                },
            },
            "permissions-pipeline-pl-stock-prices-stack": {
                "type": "databricks:Permissions",
                "properties": {
                    "resourceName": "permissions-pipeline-pl-stock-prices-stack",
                    "accessControls": [
                        {
                            "groupName": "account users",
                            "permissionLevel": "CAN_VIEW",
                            "servicePrincipalName": None,
                            "userName": None,
                        },
                        {
                            "groupName": "role-engineers",
                            "permissionLevel": "CAN_RUN",
                            "servicePrincipalName": None,
                            "userName": None,
                        },
                    ],
                    "pipelineId": "TODO",
                    "jobId": None,
                    "clusterId": None,
                    "directoryId": None,
                    "directoryPath": None,
                    "experimentId": None,
                    "notebookId": None,
                    "objectType": None,
                    "registeredModelId": None,
                    "repoId": None,
                    "repoPath": None,
                    "servingEndpointId": None,
                    "sqlAlertId": None,
                    "sqlDashboardId": None,
                    "sqlEndpointId": None,
                    "sqlQueryId": None,
                    "workspaceFileId": None,
                    "workspaceFilePath": None,
                },
            },
        },
        "outputs": {},
    }


def test_pulumi_preview():
    stack.pulumi_preview("okube/dev")


if __name__ == "__main__":
    test_stack_model()
    test_pulumi_stack()
    # test_pulumi_preview()
