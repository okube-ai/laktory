# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_job
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class JobContinuous(BaseModel):
    pause_status: str | None = Field(
        None,
        description="Indicate whether this trigger is paused or not. Either `PAUSED` or `UNPAUSED`. When the `pause_status` field is omitted in the block, the server will default to using `UNPAUSED` as a value for `pause_status`",
    )
    task_retry_mode: str | None = Field(
        None,
        description="Controls task level retry behaviour. Allowed values are: * `NEVER` (default): The failed task will not be retried. * `ON_FAILURE`: Retry a failed task if at least one other task in the job is still running its first attempt. When this condition is no longer met or the retry limit is reached, the job run is cancelled and a new run is started",
    )


class JobDbtTask(BaseModel):
    catalog: str | None = Field(
        None, description="The name of the catalog to use inside Unity Catalog"
    )
    commands: list[str] = Field(
        ...,
        description="(Array) Series of dbt commands to execute in sequence. Every command must start with 'dbt'",
    )
    profiles_directory: str | None = Field(
        None,
        description="The relative path to the directory in the repository specified by `git_source` where dbt should look in for the `profiles.yml` file. If not specified, defaults to the repository's root directory. Equivalent to passing `--profile-dir` to a dbt command",
    )
    project_directory: str | None = Field(
        None,
        description="The path where dbt should look for `dbt_project.yml`. Equivalent to passing `--project-dir` to the dbt CLI. * If `source` is `GIT`: Relative path to the directory in the repository specified in the `git_source` block. Defaults to the repository's root directory when not specified. * If `source` is `WORKSPACE`: Absolute path to the folder in the workspace",
    )
    schema_: str | None = Field(
        None,
        description="The name of the schema dbt should run in. Defaults to `default`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobDeployment(BaseModel):
    kind: str = Field(...)
    metadata_file_path: str | None = Field(None)


class JobEmailNotifications(BaseModel):
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )
    on_duration_warning_threshold_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
    )
    on_start: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
    )
    on_streaming_backlog_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
    )


class JobEnvironmentSpec(BaseModel):
    base_environment: str | None = Field(None)
    client: str | None = Field(None)
    dependencies: list[str] | None = Field(
        None,
        description="(list of strings) List of pip dependencies, as supported by the version of pip in this environment. Each dependency is a pip requirement file line.  See [API docs](https://docs.databricks.com/api/workspace/jobs/create#environments-spec-dependencies) for more information",
    )
    environment_version: str | None = Field(
        None,
        description="client version used by the environment. Each version comes with a specific Python version and a set of Python packages",
    )
    java_dependencies: list[str] | None = Field(None)


class JobEnvironment(BaseModel):
    environment_key: str = Field(
        ...,
        description="an unique identifier of the Environment.  It will be referenced from `environment_key` attribute of corresponding task",
    )
    spec: JobEnvironmentSpec | None = Field(
        None,
        description="block describing the Environment. Consists of following attributes:",
    )


class JobGitSourceGitSnapshot(BaseModel):
    used_commit: str | None = Field(None)


class JobGitSourceJobSource(BaseModel):
    dirty_state: str | None = Field(None)
    import_from_git_branch: str = Field(...)
    job_config_path: str = Field(...)


class JobGitSourceSparseCheckout(BaseModel):
    patterns: list[str] | None = Field(None)


class JobGitSource(BaseModel):
    branch: str | None = Field(
        None,
        description="name of the Git branch to use. Conflicts with `tag` and `commit`",
    )
    commit: str | None = Field(
        None, description="hash of Git commit to use. Conflicts with `branch` and `tag`"
    )
    provider: str | None = Field(
        None,
        description="case insensitive name of the Git provider.  Following values are supported right now (could be a subject for change, consult [Repos API documentation](https://docs.databricks.com/dev-tools/api/latest/repos.html)): `gitHub`, `gitHubEnterprise`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`, `gitLab`, `gitLabEnterpriseEdition`",
    )
    tag: str | None = Field(
        None,
        description="name of the Git branch to use. Conflicts with `branch` and `commit`",
    )
    url: str = Field(..., description="URL of the job on the given workspace")
    git_snapshot: JobGitSourceGitSnapshot | None = Field(None)
    job_source: JobGitSourceJobSource | None = Field(None)
    sparse_checkout: JobGitSourceSparseCheckout | None = Field(None)


class JobHealthRules(BaseModel):
    metric: str = Field(
        ...,
        description="string specifying the metric to check, like `RUN_DURATION_SECONDS`, `STREAMING_BACKLOG_FILES`, etc. - check the [Jobs REST API documentation](https://docs.databricks.com/api/workspace/jobs/create#health-rules-metric) for the full list of supported metrics",
    )
    op: str = Field(
        ...,
        description="string specifying the operation used to evaluate the given metric. The only supported operation is `GREATER_THAN`",
    )
    value: int = Field(
        ..., description="integer value used to compare to the given metric"
    )


class JobHealth(BaseModel):
    rules: list[JobHealthRules] | None = Field(
        None,
        description="(List) list of rules that are represented as objects with the following attributes:",
    )


class JobJobClusterNewClusterAutoscale(BaseModel):
    max_workers: int | None = Field(None)
    min_workers: int | None = Field(None)


class JobJobClusterNewClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: int | None = Field(None)
    ebs_volume_iops: int | None = Field(None)
    ebs_volume_size: int | None = Field(None)
    ebs_volume_throughput: int | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: int | None = Field(None)
    zone_id: str | None = Field(None)


class JobJobClusterNewClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class JobJobClusterNewClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: (
        JobJobClusterNewClusterAzureAttributesLogAnalyticsInfo | None
    ) = Field(None)


class JobJobClusterNewClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobJobClusterNewClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterClusterLogConf(BaseModel):
    dbfs: JobJobClusterNewClusterClusterLogConfDbfs | None = Field(None)
    s3: JobJobClusterNewClusterClusterLogConfS3 | None = Field(None)
    volumes: JobJobClusterNewClusterClusterLogConfVolumes | None = Field(None)


class JobJobClusterNewClusterClusterMountInfoNetworkFilesystemInfo(BaseModel):
    mount_options: str | None = Field(None)
    server_address: str = Field(...)


class JobJobClusterNewClusterClusterMountInfo(BaseModel):
    local_mount_dir_path: str = Field(...)
    remote_mount_dir_path: str | None = Field(None)
    network_filesystem_info: (
        JobJobClusterNewClusterClusterMountInfoNetworkFilesystemInfo | None
    ) = Field(None)


class JobJobClusterNewClusterDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class JobJobClusterNewClusterDockerImage(BaseModel):
    url: str = Field(..., description="URL of the job on the given workspace")
    basic_auth: JobJobClusterNewClusterDockerImageBasicAuth | None = Field(None)


class JobJobClusterNewClusterDriverNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobJobClusterNewClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    boot_disk_size: int | None = Field(None)
    first_on_demand: int | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: int | None = Field(None)
    use_preemptible_executors: bool | None = Field(None)
    zone_id: str | None = Field(None)


class JobJobClusterNewClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobJobClusterNewClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class JobJobClusterNewClusterInitScripts(BaseModel):
    abfss: JobJobClusterNewClusterInitScriptsAbfss | None = Field(None)
    dbfs: JobJobClusterNewClusterInitScriptsDbfs | None = Field(None)
    file: JobJobClusterNewClusterInitScriptsFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    gcs: JobJobClusterNewClusterInitScriptsGcs | None = Field(None)
    s3: JobJobClusterNewClusterInitScriptsS3 | None = Field(None)
    volumes: JobJobClusterNewClusterInitScriptsVolumes | None = Field(None)
    workspace: JobJobClusterNewClusterInitScriptsWorkspace | None = Field(None)


class JobJobClusterNewClusterLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobJobClusterNewClusterLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobJobClusterNewClusterLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobJobClusterNewClusterLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobJobClusterNewClusterLibraryCran | None = Field(None)
    maven: JobJobClusterNewClusterLibraryMaven | None = Field(None)
    pypi: JobJobClusterNewClusterLibraryPypi | None = Field(None)


class JobJobClusterNewClusterWorkerNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobJobClusterNewClusterWorkloadTypeClients(BaseModel):
    jobs: bool | None = Field(None)
    notebooks: bool | None = Field(None)


class JobJobClusterNewClusterWorkloadType(BaseModel):
    clients: JobJobClusterNewClusterWorkloadTypeClients | None = Field(None)


class JobJobClusterNewCluster(BaseModel):
    apply_policy_default_values: bool | None = Field(None)
    cluster_id: str | None = Field(None)
    cluster_name: str | None = Field(None)
    custom_tags: dict[str, str] | None = Field(None)
    data_security_mode: str | None = Field(None)
    driver_instance_pool_id: str | None = Field(None)
    driver_node_type_id: str | None = Field(None)
    enable_elastic_disk: bool | None = Field(None)
    enable_local_disk_encryption: bool | None = Field(None)
    idempotency_token: str | None = Field(None)
    instance_pool_id: str | None = Field(None)
    is_single_node: bool | None = Field(None)
    kind: str | None = Field(None)
    node_type_id: str | None = Field(None)
    num_workers: int | None = Field(None)
    policy_id: str | None = Field(None)
    remote_disk_throughput: int | None = Field(None)
    runtime_engine: str | None = Field(None)
    single_user_name: str | None = Field(None)
    spark_conf: dict[str, str] | None = Field(None)
    spark_env_vars: dict[str, str] | None = Field(None)
    spark_version: str | None = Field(None)
    ssh_public_keys: list[str] | None = Field(None)
    total_initial_remote_disk_size: int | None = Field(None)
    use_ml_runtime: bool | None = Field(None)
    autoscale: JobJobClusterNewClusterAutoscale | None = Field(None)
    aws_attributes: JobJobClusterNewClusterAwsAttributes | None = Field(None)
    azure_attributes: JobJobClusterNewClusterAzureAttributes | None = Field(None)
    cluster_log_conf: JobJobClusterNewClusterClusterLogConf | None = Field(None)
    cluster_mount_info: list[JobJobClusterNewClusterClusterMountInfo] | None = (
        PluralField(None, plural="cluster_mount_infos")
    )
    docker_image: JobJobClusterNewClusterDockerImage | None = Field(None)
    driver_node_type_flexibility: (
        JobJobClusterNewClusterDriverNodeTypeFlexibility | None
    ) = Field(None)
    gcp_attributes: JobJobClusterNewClusterGcpAttributes | None = Field(None)
    init_scripts: list[JobJobClusterNewClusterInitScripts] | None = Field(None)
    library: list[JobJobClusterNewClusterLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    worker_node_type_flexibility: (
        JobJobClusterNewClusterWorkerNodeTypeFlexibility | None
    ) = Field(None)
    workload_type: JobJobClusterNewClusterWorkloadType | None = Field(
        None, description="isn't supported"
    )


class JobJobCluster(BaseModel):
    job_cluster_key: str = Field(
        ...,
        description="Identifier that can be referenced in `task` block, so that cluster is shared between tasks",
    )
    new_cluster: JobJobClusterNewCluster | None = Field(
        None,
        description="Block with almost the same set of parameters as for [databricks_cluster](cluster.md) resource, except following (check the [REST API documentation for full list of supported parameters](https://docs.databricks.com/api/workspace/jobs/create#job_clusters-new_cluster)):",
    )


class JobLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobLibraryCran | None = Field(None)
    maven: JobLibraryMaven | None = Field(None)
    pypi: JobLibraryPypi | None = Field(None)


class JobNewClusterAutoscale(BaseModel):
    max_workers: int | None = Field(None)
    min_workers: int | None = Field(None)


class JobNewClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: int | None = Field(None)
    ebs_volume_iops: int | None = Field(None)
    ebs_volume_size: int | None = Field(None)
    ebs_volume_throughput: int | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: int | None = Field(None)
    zone_id: str | None = Field(None)


class JobNewClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class JobNewClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: JobNewClusterAzureAttributesLogAnalyticsInfo | None = Field(
        None
    )


class JobNewClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class JobNewClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobNewClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class JobNewClusterClusterLogConf(BaseModel):
    dbfs: JobNewClusterClusterLogConfDbfs | None = Field(None)
    s3: JobNewClusterClusterLogConfS3 | None = Field(None)
    volumes: JobNewClusterClusterLogConfVolumes | None = Field(None)


class JobNewClusterClusterMountInfoNetworkFilesystemInfo(BaseModel):
    mount_options: str | None = Field(None)
    server_address: str = Field(...)


class JobNewClusterClusterMountInfo(BaseModel):
    local_mount_dir_path: str = Field(...)
    remote_mount_dir_path: str | None = Field(None)
    network_filesystem_info: (
        JobNewClusterClusterMountInfoNetworkFilesystemInfo | None
    ) = Field(None)


class JobNewClusterDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class JobNewClusterDockerImage(BaseModel):
    url: str = Field(..., description="URL of the job on the given workspace")
    basic_auth: JobNewClusterDockerImageBasicAuth | None = Field(None)


class JobNewClusterDriverNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobNewClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    boot_disk_size: int | None = Field(None)
    first_on_demand: int | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: int | None = Field(None)
    use_preemptible_executors: bool | None = Field(None)
    zone_id: str | None = Field(None)


class JobNewClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobNewClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class JobNewClusterInitScripts(BaseModel):
    abfss: JobNewClusterInitScriptsAbfss | None = Field(None)
    dbfs: JobNewClusterInitScriptsDbfs | None = Field(None)
    file: JobNewClusterInitScriptsFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    gcs: JobNewClusterInitScriptsGcs | None = Field(None)
    s3: JobNewClusterInitScriptsS3 | None = Field(None)
    volumes: JobNewClusterInitScriptsVolumes | None = Field(None)
    workspace: JobNewClusterInitScriptsWorkspace | None = Field(None)


class JobNewClusterLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobNewClusterLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobNewClusterLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobNewClusterLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobNewClusterLibraryCran | None = Field(None)
    maven: JobNewClusterLibraryMaven | None = Field(None)
    pypi: JobNewClusterLibraryPypi | None = Field(None)


class JobNewClusterWorkerNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobNewClusterWorkloadTypeClients(BaseModel):
    jobs: bool | None = Field(None)
    notebooks: bool | None = Field(None)


class JobNewClusterWorkloadType(BaseModel):
    clients: JobNewClusterWorkloadTypeClients | None = Field(None)


class JobNewCluster(BaseModel):
    apply_policy_default_values: bool | None = Field(None)
    cluster_id: str | None = Field(None)
    cluster_name: str | None = Field(None)
    custom_tags: dict[str, str] | None = Field(None)
    data_security_mode: str | None = Field(None)
    driver_instance_pool_id: str | None = Field(None)
    driver_node_type_id: str | None = Field(None)
    enable_elastic_disk: bool | None = Field(None)
    enable_local_disk_encryption: bool | None = Field(None)
    idempotency_token: str | None = Field(None)
    instance_pool_id: str | None = Field(None)
    is_single_node: bool | None = Field(None)
    kind: str | None = Field(None)
    node_type_id: str | None = Field(None)
    num_workers: int | None = Field(None)
    policy_id: str | None = Field(None)
    remote_disk_throughput: int | None = Field(None)
    runtime_engine: str | None = Field(None)
    single_user_name: str | None = Field(None)
    spark_conf: dict[str, str] | None = Field(None)
    spark_env_vars: dict[str, str] | None = Field(None)
    spark_version: str | None = Field(None)
    ssh_public_keys: list[str] | None = Field(None)
    total_initial_remote_disk_size: int | None = Field(None)
    use_ml_runtime: bool | None = Field(None)
    autoscale: JobNewClusterAutoscale | None = Field(None)
    aws_attributes: JobNewClusterAwsAttributes | None = Field(None)
    azure_attributes: JobNewClusterAzureAttributes | None = Field(None)
    cluster_log_conf: JobNewClusterClusterLogConf | None = Field(None)
    cluster_mount_info: list[JobNewClusterClusterMountInfo] | None = PluralField(
        None, plural="cluster_mount_infos"
    )
    docker_image: JobNewClusterDockerImage | None = Field(None)
    driver_node_type_flexibility: JobNewClusterDriverNodeTypeFlexibility | None = Field(
        None
    )
    gcp_attributes: JobNewClusterGcpAttributes | None = Field(None)
    init_scripts: list[JobNewClusterInitScripts] | None = Field(None)
    library: list[JobNewClusterLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    worker_node_type_flexibility: JobNewClusterWorkerNodeTypeFlexibility | None = Field(
        None
    )
    workload_type: JobNewClusterWorkloadType | None = Field(
        None, description="isn't supported"
    )


class JobNotebookTask(BaseModel):
    base_parameters: dict[str, str] | None = Field(
        None,
        description="(Map) Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters and in run-now, the value from run-now will be used. If the notebook takes a parameter that is not specified in the job's base_parameters or the run-now override parameters, the default value from the notebook will be used. Retrieve these parameters in a notebook using `dbutils.widgets.get`",
    )
    notebook_path: str = Field(
        ...,
        description="The path of the [databricks_notebook](notebook.md#path) to be run in the Databricks workspace or remote repository. For notebooks stored in the Databricks workspace, the path must be absolute and begin with a slash. For notebooks stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobNotificationSettings(BaseModel):
    no_alert_for_canceled_runs: bool | None = Field(
        None, description="(Bool) don't send alert for cancelled runs"
    )
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )


class JobParameter(BaseModel):
    default: str = Field(..., description="Default value of the parameter")
    name: str = Field(
        ...,
        description="The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`",
    )


class JobPipelineTask(BaseModel):
    full_refresh: bool | None = Field(
        None,
        description="(Bool) Specifies if there should be full refresh of the pipeline",
    )
    pipeline_id: str = Field(..., description="The pipeline's unique ID")


class JobPythonWheelTask(BaseModel):
    entry_point: str | None = Field(
        None, description="Python function as entry point for the task"
    )
    named_parameters: dict[str, str] | None = Field(
        None, description="Named parameters for the task"
    )
    package_name: str | None = Field(None, description="Name of Python package")
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobQueue(BaseModel):
    enabled: bool = Field(..., description="If true, enable queueing for the job")


class JobRunAs(BaseModel):
    group_name: str | None = Field(None)
    service_principal_name: str | None = Field(
        None,
        description="The application ID of an active service principal. Setting this field requires the `servicePrincipal/user` role",
    )
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobRunJobTask(BaseModel):
    job_id: int = Field(..., description="(String) ID of the job")
    job_parameters: dict[str, str] | None = Field(
        None, description="(Map) Job parameters for the task"
    )


class JobSchedule(BaseModel):
    pause_status: str | None = Field(
        None,
        description="Indicate whether this trigger is paused or not. Either `PAUSED` or `UNPAUSED`. When the `pause_status` field is omitted in the block, the server will default to using `UNPAUSED` as a value for `pause_status`",
    )
    quartz_cron_expression: str = Field(
        ...,
        description="A [Cron expression using Quartz syntax](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html) that describes the schedule for a job. This field is required",
    )
    timezone_id: str = Field(
        ...,
        description="A Java timezone ID. The schedule for a job will be resolved with respect to this timezone. See Java TimeZone for details. This field is required",
    )


class JobSparkJarTask(BaseModel):
    jar_uri: str | None = Field(None)
    main_class_name: str | None = Field(
        None,
        description="The full name of the class containing the main method to be executed. This class must be contained in a JAR provided as a library. The code should use `SparkContext.getOrCreate` to obtain a Spark context; otherwise, runs of the job will fail",
    )
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobSparkPythonTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    python_file: str = Field(
        ...,
        description="The URI of the Python file to be executed. Cloud file URIs (e.g. `s3:/`, `abfss:/`, `gs:/`), workspace paths and remote repository are supported. For Python files stored in the Databricks workspace, the path must be absolute and begin with `/`. For files stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )


class JobSparkSubmitTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobTaskAlertTaskSubscribers(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskAlertTask(BaseModel):
    alert_id: str | None = Field(
        None,
        description="(String) identifier of the Databricks Alert ([databricks_alert](alert.md))",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    workspace_path: str | None = Field(None)
    subscribers: list[JobTaskAlertTaskSubscribers] | None = Field(None)


class JobTaskCleanRoomsNotebookTask(BaseModel):
    clean_room_name: str = Field(...)
    etag: str | None = Field(None)
    notebook_base_parameters: dict[str, str] | None = Field(None)
    notebook_name: str = Field(...)


class JobTaskCompute(BaseModel):
    hardware_accelerator: str | None = Field(
        None,
        description="Hardware accelerator configuration for Serverless GPU workloads. Supported values are: * `GPU_1xA10`: GPU_1xA10: Single A10 GPU configuration. * `GPU_8xH100`: GPU_8xH100: 8x H100 GPU configuration",
    )


class JobTaskConditionTask(BaseModel):
    left: str = Field(
        ...,
        description="The left operand of the condition task. It could be a string value, job state, or a parameter reference",
    )
    op: str = Field(
        ...,
        description="string specifying the operation used to evaluate the given metric. The only supported operation is `GREATER_THAN`",
    )
    right: str = Field(
        ...,
        description="The right operand of the condition task. It could be a string value, job state, or parameter reference",
    )


class JobTaskDashboardTaskSubscriptionSubscribers(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskDashboardTaskSubscription(BaseModel):
    custom_subject: str | None = Field(
        None, description="string specifying a custom subject of email sent"
    )
    paused: bool | None = Field(None)
    subscribers: list[JobTaskDashboardTaskSubscriptionSubscribers] | None = Field(None)


class JobTaskDashboardTask(BaseModel):
    dashboard_id: str | None = Field(
        None,
        description="(String) identifier of the Databricks SQL Dashboard [databricks_sql_dashboard](sql_dashboard.md)",
    )
    filters: dict[str, str] | None = Field(None)
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    subscription: JobTaskDashboardTaskSubscription | None = Field(None)


class JobTaskDbtCloudTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    dbt_cloud_job_id: int | None = Field(None)


class JobTaskDbtPlatformTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    dbt_platform_job_id: str | None = Field(None)


class JobTaskDbtTask(BaseModel):
    catalog: str | None = Field(
        None, description="The name of the catalog to use inside Unity Catalog"
    )
    commands: list[str] = Field(
        ...,
        description="(Array) Series of dbt commands to execute in sequence. Every command must start with 'dbt'",
    )
    profiles_directory: str | None = Field(
        None,
        description="The relative path to the directory in the repository specified by `git_source` where dbt should look in for the `profiles.yml` file. If not specified, defaults to the repository's root directory. Equivalent to passing `--profile-dir` to a dbt command",
    )
    project_directory: str | None = Field(
        None,
        description="The path where dbt should look for `dbt_project.yml`. Equivalent to passing `--project-dir` to the dbt CLI. * If `source` is `GIT`: Relative path to the directory in the repository specified in the `git_source` block. Defaults to the repository's root directory when not specified. * If `source` is `WORKSPACE`: Absolute path to the folder in the workspace",
    )
    schema_: str | None = Field(
        None,
        description="The name of the schema dbt should run in. Defaults to `default`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobTaskDependsOn(BaseModel):
    outcome: str | None = Field(
        None,
        description="Can only be specified on condition task dependencies. The outcome of the dependent task that must be met for this task to run. Possible values are `'true'` or `'false'`",
    )
    task_key: str = Field(..., description="The name of the task this task depends on")


class JobTaskEmailNotifications(BaseModel):
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )
    on_duration_warning_threshold_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
    )
    on_start: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
    )
    on_streaming_backlog_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
    )


class JobTaskForEachTaskTaskAlertTaskSubscribers(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskForEachTaskTaskAlertTask(BaseModel):
    alert_id: str | None = Field(
        None,
        description="(String) identifier of the Databricks Alert ([databricks_alert](alert.md))",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    workspace_path: str | None = Field(None)
    subscribers: list[JobTaskForEachTaskTaskAlertTaskSubscribers] | None = Field(None)


class JobTaskForEachTaskTaskCleanRoomsNotebookTask(BaseModel):
    clean_room_name: str = Field(...)
    etag: str | None = Field(None)
    notebook_base_parameters: dict[str, str] | None = Field(None)
    notebook_name: str = Field(...)


class JobTaskForEachTaskTaskCompute(BaseModel):
    hardware_accelerator: str | None = Field(
        None,
        description="Hardware accelerator configuration for Serverless GPU workloads. Supported values are: * `GPU_1xA10`: GPU_1xA10: Single A10 GPU configuration. * `GPU_8xH100`: GPU_8xH100: 8x H100 GPU configuration",
    )


class JobTaskForEachTaskTaskConditionTask(BaseModel):
    left: str = Field(
        ...,
        description="The left operand of the condition task. It could be a string value, job state, or a parameter reference",
    )
    op: str = Field(
        ...,
        description="string specifying the operation used to evaluate the given metric. The only supported operation is `GREATER_THAN`",
    )
    right: str = Field(
        ...,
        description="The right operand of the condition task. It could be a string value, job state, or parameter reference",
    )


class JobTaskForEachTaskTaskDashboardTaskSubscriptionSubscribers(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskForEachTaskTaskDashboardTaskSubscription(BaseModel):
    custom_subject: str | None = Field(
        None, description="string specifying a custom subject of email sent"
    )
    paused: bool | None = Field(None)
    subscribers: (
        list[JobTaskForEachTaskTaskDashboardTaskSubscriptionSubscribers] | None
    ) = Field(None)


class JobTaskForEachTaskTaskDashboardTask(BaseModel):
    dashboard_id: str | None = Field(
        None,
        description="(String) identifier of the Databricks SQL Dashboard [databricks_sql_dashboard](sql_dashboard.md)",
    )
    filters: dict[str, str] | None = Field(None)
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    subscription: JobTaskForEachTaskTaskDashboardTaskSubscription | None = Field(None)


class JobTaskForEachTaskTaskDbtCloudTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    dbt_cloud_job_id: int | None = Field(None)


class JobTaskForEachTaskTaskDbtPlatformTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    dbt_platform_job_id: str | None = Field(None)


class JobTaskForEachTaskTaskDbtTask(BaseModel):
    catalog: str | None = Field(
        None, description="The name of the catalog to use inside Unity Catalog"
    )
    commands: list[str] = Field(
        ...,
        description="(Array) Series of dbt commands to execute in sequence. Every command must start with 'dbt'",
    )
    profiles_directory: str | None = Field(
        None,
        description="The relative path to the directory in the repository specified by `git_source` where dbt should look in for the `profiles.yml` file. If not specified, defaults to the repository's root directory. Equivalent to passing `--profile-dir` to a dbt command",
    )
    project_directory: str | None = Field(
        None,
        description="The path where dbt should look for `dbt_project.yml`. Equivalent to passing `--project-dir` to the dbt CLI. * If `source` is `GIT`: Relative path to the directory in the repository specified in the `git_source` block. Defaults to the repository's root directory when not specified. * If `source` is `WORKSPACE`: Absolute path to the folder in the workspace",
    )
    schema_: str | None = Field(
        None,
        description="The name of the schema dbt should run in. Defaults to `default`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobTaskForEachTaskTaskDependsOn(BaseModel):
    outcome: str | None = Field(
        None,
        description="Can only be specified on condition task dependencies. The outcome of the dependent task that must be met for this task to run. Possible values are `'true'` or `'false'`",
    )
    task_key: str = Field(..., description="The name of the task this task depends on")


class JobTaskForEachTaskTaskEmailNotifications(BaseModel):
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )
    on_duration_warning_threshold_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
    )
    on_start: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
    )
    on_streaming_backlog_exceeded: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[str] | None = Field(
        None,
        description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
    )


class JobTaskForEachTaskTaskGenAiComputeTaskCompute(BaseModel):
    gpu_node_pool_id: str | None = Field(None)
    gpu_type: str | None = Field(None)
    num_gpus: int = Field(...)


class JobTaskForEachTaskTaskGenAiComputeTask(BaseModel):
    command: str | None = Field(None)
    dl_runtime_image: str = Field(...)
    mlflow_experiment_name: str | None = Field(None)
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    training_script_path: str | None = Field(None)
    yaml_parameters: str | None = Field(None)
    yaml_parameters_file_path: str | None = Field(None)
    compute: JobTaskForEachTaskTaskGenAiComputeTaskCompute | None = Field(
        None,
        description="Task level compute configuration. This block is [documented below](#compute-configuration-block)",
    )


class JobTaskForEachTaskTaskHealthRules(BaseModel):
    metric: str = Field(
        ...,
        description="string specifying the metric to check, like `RUN_DURATION_SECONDS`, `STREAMING_BACKLOG_FILES`, etc. - check the [Jobs REST API documentation](https://docs.databricks.com/api/workspace/jobs/create#health-rules-metric) for the full list of supported metrics",
    )
    op: str = Field(
        ...,
        description="string specifying the operation used to evaluate the given metric. The only supported operation is `GREATER_THAN`",
    )
    value: int = Field(
        ..., description="integer value used to compare to the given metric"
    )


class JobTaskForEachTaskTaskHealth(BaseModel):
    rules: list[JobTaskForEachTaskTaskHealthRules] | None = Field(
        None,
        description="(List) list of rules that are represented as objects with the following attributes:",
    )


class JobTaskForEachTaskTaskLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobTaskForEachTaskTaskLibraryCran | None = Field(None)
    maven: JobTaskForEachTaskTaskLibraryMaven | None = Field(None)
    pypi: JobTaskForEachTaskTaskLibraryPypi | None = Field(None)


class JobTaskForEachTaskTaskNewClusterAutoscale(BaseModel):
    max_workers: int | None = Field(None)
    min_workers: int | None = Field(None)


class JobTaskForEachTaskTaskNewClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: int | None = Field(None)
    ebs_volume_iops: int | None = Field(None)
    ebs_volume_size: int | None = Field(None)
    ebs_volume_throughput: int | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: int | None = Field(None)
    zone_id: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: (
        JobTaskForEachTaskTaskNewClusterAzureAttributesLogAnalyticsInfo | None
    ) = Field(None)


class JobTaskForEachTaskTaskNewClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterClusterLogConf(BaseModel):
    dbfs: JobTaskForEachTaskTaskNewClusterClusterLogConfDbfs | None = Field(None)
    s3: JobTaskForEachTaskTaskNewClusterClusterLogConfS3 | None = Field(None)
    volumes: JobTaskForEachTaskTaskNewClusterClusterLogConfVolumes | None = Field(None)


class JobTaskForEachTaskTaskNewClusterClusterMountInfoNetworkFilesystemInfo(BaseModel):
    mount_options: str | None = Field(None)
    server_address: str = Field(...)


class JobTaskForEachTaskTaskNewClusterClusterMountInfo(BaseModel):
    local_mount_dir_path: str = Field(...)
    remote_mount_dir_path: str | None = Field(None)
    network_filesystem_info: (
        JobTaskForEachTaskTaskNewClusterClusterMountInfoNetworkFilesystemInfo | None
    ) = Field(None)


class JobTaskForEachTaskTaskNewClusterDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class JobTaskForEachTaskTaskNewClusterDockerImage(BaseModel):
    url: str = Field(..., description="URL of the job on the given workspace")
    basic_auth: JobTaskForEachTaskTaskNewClusterDockerImageBasicAuth | None = Field(
        None
    )


class JobTaskForEachTaskTaskNewClusterDriverNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobTaskForEachTaskTaskNewClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    boot_disk_size: int | None = Field(None)
    first_on_demand: int | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: int | None = Field(None)
    use_preemptible_executors: bool | None = Field(None)
    zone_id: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class JobTaskForEachTaskTaskNewClusterInitScripts(BaseModel):
    abfss: JobTaskForEachTaskTaskNewClusterInitScriptsAbfss | None = Field(None)
    dbfs: JobTaskForEachTaskTaskNewClusterInitScriptsDbfs | None = Field(None)
    file: JobTaskForEachTaskTaskNewClusterInitScriptsFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    gcs: JobTaskForEachTaskTaskNewClusterInitScriptsGcs | None = Field(None)
    s3: JobTaskForEachTaskTaskNewClusterInitScriptsS3 | None = Field(None)
    volumes: JobTaskForEachTaskTaskNewClusterInitScriptsVolumes | None = Field(None)
    workspace: JobTaskForEachTaskTaskNewClusterInitScriptsWorkspace | None = Field(None)


class JobTaskForEachTaskTaskNewClusterLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskForEachTaskTaskNewClusterLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobTaskForEachTaskTaskNewClusterLibraryCran | None = Field(None)
    maven: JobTaskForEachTaskTaskNewClusterLibraryMaven | None = Field(None)
    pypi: JobTaskForEachTaskTaskNewClusterLibraryPypi | None = Field(None)


class JobTaskForEachTaskTaskNewClusterWorkerNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobTaskForEachTaskTaskNewClusterWorkloadTypeClients(BaseModel):
    jobs: bool | None = Field(None)
    notebooks: bool | None = Field(None)


class JobTaskForEachTaskTaskNewClusterWorkloadType(BaseModel):
    clients: JobTaskForEachTaskTaskNewClusterWorkloadTypeClients | None = Field(None)


class JobTaskForEachTaskTaskNewCluster(BaseModel):
    apply_policy_default_values: bool | None = Field(None)
    cluster_id: str | None = Field(None)
    cluster_name: str | None = Field(None)
    custom_tags: dict[str, str] | None = Field(None)
    data_security_mode: str | None = Field(None)
    driver_instance_pool_id: str | None = Field(None)
    driver_node_type_id: str | None = Field(None)
    enable_elastic_disk: bool | None = Field(None)
    enable_local_disk_encryption: bool | None = Field(None)
    idempotency_token: str | None = Field(None)
    instance_pool_id: str | None = Field(None)
    is_single_node: bool | None = Field(None)
    kind: str | None = Field(None)
    node_type_id: str | None = Field(None)
    num_workers: int | None = Field(None)
    policy_id: str | None = Field(None)
    remote_disk_throughput: int | None = Field(None)
    runtime_engine: str | None = Field(None)
    single_user_name: str | None = Field(None)
    spark_conf: dict[str, str] | None = Field(None)
    spark_env_vars: dict[str, str] | None = Field(None)
    spark_version: str | None = Field(None)
    ssh_public_keys: list[str] | None = Field(None)
    total_initial_remote_disk_size: int | None = Field(None)
    use_ml_runtime: bool | None = Field(None)
    autoscale: JobTaskForEachTaskTaskNewClusterAutoscale | None = Field(None)
    aws_attributes: JobTaskForEachTaskTaskNewClusterAwsAttributes | None = Field(None)
    azure_attributes: JobTaskForEachTaskTaskNewClusterAzureAttributes | None = Field(
        None
    )
    cluster_log_conf: JobTaskForEachTaskTaskNewClusterClusterLogConf | None = Field(
        None
    )
    cluster_mount_info: (
        list[JobTaskForEachTaskTaskNewClusterClusterMountInfo] | None
    ) = PluralField(None, plural="cluster_mount_infos")
    docker_image: JobTaskForEachTaskTaskNewClusterDockerImage | None = Field(None)
    driver_node_type_flexibility: (
        JobTaskForEachTaskTaskNewClusterDriverNodeTypeFlexibility | None
    ) = Field(None)
    gcp_attributes: JobTaskForEachTaskTaskNewClusterGcpAttributes | None = Field(None)
    init_scripts: list[JobTaskForEachTaskTaskNewClusterInitScripts] | None = Field(None)
    library: list[JobTaskForEachTaskTaskNewClusterLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    worker_node_type_flexibility: (
        JobTaskForEachTaskTaskNewClusterWorkerNodeTypeFlexibility | None
    ) = Field(None)
    workload_type: JobTaskForEachTaskTaskNewClusterWorkloadType | None = Field(
        None, description="isn't supported"
    )


class JobTaskForEachTaskTaskNotebookTask(BaseModel):
    base_parameters: dict[str, str] | None = Field(
        None,
        description="(Map) Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters and in run-now, the value from run-now will be used. If the notebook takes a parameter that is not specified in the job's base_parameters or the run-now override parameters, the default value from the notebook will be used. Retrieve these parameters in a notebook using `dbutils.widgets.get`",
    )
    notebook_path: str = Field(
        ...,
        description="The path of the [databricks_notebook](notebook.md#path) to be run in the Databricks workspace or remote repository. For notebooks stored in the Databricks workspace, the path must be absolute and begin with a slash. For notebooks stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobTaskForEachTaskTaskNotificationSettings(BaseModel):
    alert_on_last_attempt: bool | None = Field(
        None,
        description="(Bool) do not send notifications to recipients specified in `on_start` for the retried runs and do not send notifications to recipients specified in `on_failure` until the last retry of the run",
    )
    no_alert_for_canceled_runs: bool | None = Field(
        None, description="(Bool) don't send alert for cancelled runs"
    )
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )


class JobTaskForEachTaskTaskPipelineTask(BaseModel):
    full_refresh: bool | None = Field(
        None,
        description="(Bool) Specifies if there should be full refresh of the pipeline",
    )
    pipeline_id: str = Field(..., description="The pipeline's unique ID")


class JobTaskForEachTaskTaskPowerBiTaskPowerBiModel(BaseModel):
    authentication_method: str | None = Field(None)
    model_name: str | None = Field(None)
    overwrite_existing: bool | None = Field(None)
    storage_mode: str | None = Field(None)
    workspace_name: str | None = Field(None)


class JobTaskForEachTaskTaskPowerBiTaskTables(BaseModel):
    catalog: str | None = Field(
        None, description="The name of the catalog to use inside Unity Catalog"
    )
    name: str | None = Field(
        None,
        description="The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`",
    )
    schema_: str | None = Field(
        None,
        description="The name of the schema dbt should run in. Defaults to `default`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    storage_mode: str | None = Field(None)


class JobTaskForEachTaskTaskPowerBiTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    refresh_after_update: bool | None = Field(None)
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    power_bi_model: JobTaskForEachTaskTaskPowerBiTaskPowerBiModel | None = Field(None)
    tables: list[JobTaskForEachTaskTaskPowerBiTaskTables] | None = Field(None)


class JobTaskForEachTaskTaskPythonWheelTask(BaseModel):
    entry_point: str | None = Field(
        None, description="Python function as entry point for the task"
    )
    named_parameters: dict[str, str] | None = Field(
        None, description="Named parameters for the task"
    )
    package_name: str | None = Field(None, description="Name of Python package")
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobTaskForEachTaskTaskRunJobTaskPipelineParams(BaseModel):
    full_refresh: bool | None = Field(
        None,
        description="(Bool) Specifies if there should be full refresh of the pipeline",
    )


class JobTaskForEachTaskTaskRunJobTask(BaseModel):
    dbt_commands: list[str] | None = Field(None)
    jar_params: list[str] | None = Field(None)
    job_id: int = Field(..., description="(String) ID of the job")
    job_parameters: dict[str, str] | None = Field(
        None, description="(Map) Job parameters for the task"
    )
    notebook_params: dict[str, str] | None = Field(None)
    python_named_params: dict[str, str] | None = Field(None)
    python_params: list[str] | None = Field(None)
    spark_submit_params: list[str] | None = Field(None)
    sql_params: dict[str, str] | None = Field(None)
    pipeline_params: JobTaskForEachTaskTaskRunJobTaskPipelineParams | None = Field(None)


class JobTaskForEachTaskTaskSparkJarTask(BaseModel):
    jar_uri: str | None = Field(None)
    main_class_name: str | None = Field(
        None,
        description="The full name of the class containing the main method to be executed. This class must be contained in a JAR provided as a library. The code should use `SparkContext.getOrCreate` to obtain a Spark context; otherwise, runs of the job will fail",
    )
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    run_as_repl: bool | None = Field(None)


class JobTaskForEachTaskTaskSparkPythonTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    python_file: str = Field(
        ...,
        description="The URI of the Python file to be executed. Cloud file URIs (e.g. `s3:/`, `abfss:/`, `gs:/`), workspace paths and remote repository are supported. For Python files stored in the Databricks workspace, the path must be absolute and begin with `/`. For files stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )


class JobTaskForEachTaskTaskSparkSubmitTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobTaskForEachTaskTaskSqlTaskAlertSubscriptions(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskForEachTaskTaskSqlTaskAlert(BaseModel):
    alert_id: str = Field(
        ...,
        description="(String) identifier of the Databricks Alert ([databricks_alert](alert.md))",
    )
    pause_subscriptions: bool | None = Field(
        None, description="flag that specifies if subscriptions are paused or not"
    )
    subscriptions: list[JobTaskForEachTaskTaskSqlTaskAlertSubscriptions] | None = Field(
        None,
        description="a list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or `destination_id` - for Alert destination's identifier",
    )


class JobTaskForEachTaskTaskSqlTaskDashboardSubscriptions(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskForEachTaskTaskSqlTaskDashboard(BaseModel):
    custom_subject: str | None = Field(
        None, description="string specifying a custom subject of email sent"
    )
    dashboard_id: str = Field(
        ...,
        description="(String) identifier of the Databricks SQL Dashboard [databricks_sql_dashboard](sql_dashboard.md)",
    )
    pause_subscriptions: bool | None = Field(
        None, description="flag that specifies if subscriptions are paused or not"
    )
    subscriptions: list[JobTaskForEachTaskTaskSqlTaskDashboardSubscriptions] | None = (
        Field(
            None,
            description="a list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or `destination_id` - for Alert destination's identifier",
        )
    )


class JobTaskForEachTaskTaskSqlTaskFile(BaseModel):
    path: str = Field(
        ...,
        description="If `source` is `GIT`: Relative path to the file in the repository specified in the `git_source` block with SQL commands to execute. If `source` is `WORKSPACE`: Absolute path to the file in the workspace with SQL commands to execute",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )


class JobTaskForEachTaskTaskSqlTaskQuery(BaseModel):
    query_id: str = Field(...)


class JobTaskForEachTaskTaskSqlTask(BaseModel):
    parameters: dict[str, str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    warehouse_id: str = Field(
        ...,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    alert: JobTaskForEachTaskTaskSqlTaskAlert | None = Field(
        None, description="block consisting of following fields:"
    )
    dashboard: JobTaskForEachTaskTaskSqlTaskDashboard | None = Field(
        None, description="block consisting of following fields:"
    )
    file: JobTaskForEachTaskTaskSqlTaskFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    query: JobTaskForEachTaskTaskSqlTaskQuery | None = Field(
        None,
        description="block consisting of single string field: `query_id` - identifier of the Databricks Query ([databricks_query](query.md))",
    )


class JobTaskForEachTaskTaskWebhookNotificationsOnDurationWarningThresholdExceeded(
    BaseModel
):
    pass


class JobTaskForEachTaskTaskWebhookNotificationsOnFailure(BaseModel):
    pass


class JobTaskForEachTaskTaskWebhookNotificationsOnStart(BaseModel):
    pass


class JobTaskForEachTaskTaskWebhookNotificationsOnStreamingBacklogExceeded(BaseModel):
    pass


class JobTaskForEachTaskTaskWebhookNotificationsOnSuccess(BaseModel):
    pass


class JobTaskForEachTaskTaskWebhookNotifications(BaseModel):
    on_duration_warning_threshold_exceeded: (
        list[
            JobTaskForEachTaskTaskWebhookNotificationsOnDurationWarningThresholdExceeded
        ]
        | None
    ) = PluralField(
        None,
        plural="on_duration_warning_threshold_exceededs",
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[JobTaskForEachTaskTaskWebhookNotificationsOnFailure] | None = (
        PluralField(
            None,
            plural="on_failures",
            description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
        )
    )
    on_start: list[JobTaskForEachTaskTaskWebhookNotificationsOnStart] | None = (
        PluralField(
            None,
            plural="on_starts",
            description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
        )
    )
    on_streaming_backlog_exceeded: (
        list[JobTaskForEachTaskTaskWebhookNotificationsOnStreamingBacklogExceeded]
        | None
    ) = PluralField(
        None,
        plural="on_streaming_backlog_exceededs",
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[JobTaskForEachTaskTaskWebhookNotificationsOnSuccess] | None = (
        PluralField(
            None,
            plural="on_successes",
            description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
        )
    )


class JobTaskForEachTaskTask(BaseModel):
    description: str | None = Field(None, description="description for this task")
    disable_auto_optimization: bool | None = Field(
        None, description="A flag to disable auto optimization in serverless tasks"
    )
    disabled: bool | None = Field(None)
    environment_key: str | None = Field(
        None,
        description="an unique identifier of the Environment.  It will be referenced from `environment_key` attribute of corresponding task",
    )
    existing_cluster_id: str | None = Field(
        None,
        description="Identifier of the [interactive cluster](cluster.md) to run job on.  *Note: running tasks on interactive clusters may lead to increased costs!*",
    )
    job_cluster_key: str | None = Field(
        None,
        description="Identifier that can be referenced in `task` block, so that cluster is shared between tasks",
    )
    max_retries: int | None = Field(
        None,
        description="(Integer) An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it completes with a `FAILED` or `INTERNAL_ERROR` lifecycle state. The value -1 means to retry indefinitely and the value 0 means to never retry. The default behavior is to never retry. A run can have the following lifecycle state: `PENDING`, `RUNNING`, `TERMINATING`, `TERMINATED`, `SKIPPED` or `INTERNAL_ERROR`",
    )
    min_retry_interval_millis: int | None = Field(
        None,
        description="(Integer) An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run. The default behavior is that unsuccessful runs are immediately retried",
    )
    retry_on_timeout: bool | None = Field(
        None,
        description="(Bool) An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on timeout",
    )
    run_if: str | None = Field(
        None,
        description="An optional value indicating the condition that determines whether the task should be run once its dependencies have been completed. One of `ALL_SUCCESS`, `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, `ALL_DONE`, `AT_LEAST_ONE_FAILED` or `ALL_FAILED`. When omitted, defaults to `ALL_SUCCESS`",
    )
    task_key: str = Field(..., description="The name of the task this task depends on")
    timeout_seconds: int | None = Field(
        None,
        description="(Integer) An optional timeout applied to each run of this job. The default behavior is to have no timeout",
    )
    alert_task: JobTaskForEachTaskTaskAlertTask | None = Field(None)
    clean_rooms_notebook_task: JobTaskForEachTaskTaskCleanRoomsNotebookTask | None = (
        Field(None)
    )
    compute: JobTaskForEachTaskTaskCompute | None = Field(
        None,
        description="Task level compute configuration. This block is [documented below](#compute-configuration-block)",
    )
    condition_task: JobTaskForEachTaskTaskConditionTask | None = Field(None)
    dashboard_task: JobTaskForEachTaskTaskDashboardTask | None = Field(None)
    dbt_cloud_task: JobTaskForEachTaskTaskDbtCloudTask | None = Field(None)
    dbt_platform_task: JobTaskForEachTaskTaskDbtPlatformTask | None = Field(None)
    dbt_task: JobTaskForEachTaskTaskDbtTask | None = Field(None)
    depends_on: list[JobTaskForEachTaskTaskDependsOn] | None = PluralField(
        None,
        plural="depends_ons",
        description="block specifying dependency(-ies) for a given task",
    )
    email_notifications: JobTaskForEachTaskTaskEmailNotifications | None = Field(
        None,
        description="An optional block to specify a set of email addresses notified when this task begins, completes or fails. The default behavior is to not send any emails. This block is [documented below](#email_notifications-configuration-block)",
    )
    gen_ai_compute_task: JobTaskForEachTaskTaskGenAiComputeTask | None = Field(None)
    health: JobTaskForEachTaskTaskHealth | None = Field(
        None,
        description="block described below that specifies health conditions for a given task",
    )
    library: list[JobTaskForEachTaskTaskLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    new_cluster: JobTaskForEachTaskTaskNewCluster | None = Field(
        None,
        description="Block with almost the same set of parameters as for [databricks_cluster](cluster.md) resource, except following (check the [REST API documentation for full list of supported parameters](https://docs.databricks.com/api/workspace/jobs/create#job_clusters-new_cluster)):",
    )
    notebook_task: JobTaskForEachTaskTaskNotebookTask | None = Field(None)
    notification_settings: JobTaskForEachTaskTaskNotificationSettings | None = Field(
        None,
        description="An optional block controlling the notification settings on the job level [documented below](#notification_settings-configuration-block)",
    )
    pipeline_task: JobTaskForEachTaskTaskPipelineTask | None = Field(None)
    power_bi_task: JobTaskForEachTaskTaskPowerBiTask | None = Field(None)
    python_wheel_task: JobTaskForEachTaskTaskPythonWheelTask | None = Field(None)
    run_job_task: JobTaskForEachTaskTaskRunJobTask | None = Field(None)
    spark_jar_task: JobTaskForEachTaskTaskSparkJarTask | None = Field(None)
    spark_python_task: JobTaskForEachTaskTaskSparkPythonTask | None = Field(None)
    spark_submit_task: JobTaskForEachTaskTaskSparkSubmitTask | None = Field(None)
    sql_task: JobTaskForEachTaskTaskSqlTask | None = Field(None)
    webhook_notifications: JobTaskForEachTaskTaskWebhookNotifications | None = Field(
        None,
        description="(List) An optional set of system destinations (for example, webhook destinations or Slack) to be notified when runs of this task begins, completes or fails. The default behavior is to not send any notifications. This field is a block and is documented below",
    )


class JobTaskForEachTask(BaseModel):
    concurrency: int | None = Field(
        None,
        description="Controls the number of active iteration task runs. Default is 20, maximum allowed is 100",
    )
    inputs: str = Field(
        ...,
        description="(String) Array for task to iterate on. This can be a JSON string or a reference to an array parameter",
    )
    task: JobTaskForEachTaskTask | None = Field(
        None, description="Task to run against the `inputs` list"
    )


class JobTaskGenAiComputeTaskCompute(BaseModel):
    gpu_node_pool_id: str | None = Field(None)
    gpu_type: str | None = Field(None)
    num_gpus: int = Field(...)


class JobTaskGenAiComputeTask(BaseModel):
    command: str | None = Field(None)
    dl_runtime_image: str = Field(...)
    mlflow_experiment_name: str | None = Field(None)
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    training_script_path: str | None = Field(None)
    yaml_parameters: str | None = Field(None)
    yaml_parameters_file_path: str | None = Field(None)
    compute: JobTaskGenAiComputeTaskCompute | None = Field(
        None,
        description="Task level compute configuration. This block is [documented below](#compute-configuration-block)",
    )


class JobTaskHealthRules(BaseModel):
    metric: str = Field(
        ...,
        description="string specifying the metric to check, like `RUN_DURATION_SECONDS`, `STREAMING_BACKLOG_FILES`, etc. - check the [Jobs REST API documentation](https://docs.databricks.com/api/workspace/jobs/create#health-rules-metric) for the full list of supported metrics",
    )
    op: str = Field(
        ...,
        description="string specifying the operation used to evaluate the given metric. The only supported operation is `GREATER_THAN`",
    )
    value: int = Field(
        ..., description="integer value used to compare to the given metric"
    )


class JobTaskHealth(BaseModel):
    rules: list[JobTaskHealthRules] | None = Field(
        None,
        description="(List) list of rules that are represented as objects with the following attributes:",
    )


class JobTaskLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobTaskLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobTaskLibraryCran | None = Field(None)
    maven: JobTaskLibraryMaven | None = Field(None)
    pypi: JobTaskLibraryPypi | None = Field(None)


class JobTaskNewClusterAutoscale(BaseModel):
    max_workers: int | None = Field(None)
    min_workers: int | None = Field(None)


class JobTaskNewClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: int | None = Field(None)
    ebs_volume_iops: int | None = Field(None)
    ebs_volume_size: int | None = Field(None)
    ebs_volume_throughput: int | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: int | None = Field(None)
    zone_id: str | None = Field(None)


class JobTaskNewClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class JobTaskNewClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: int | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: JobTaskNewClusterAzureAttributesLogAnalyticsInfo | None = Field(
        None
    )


class JobTaskNewClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobTaskNewClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterClusterLogConf(BaseModel):
    dbfs: JobTaskNewClusterClusterLogConfDbfs | None = Field(None)
    s3: JobTaskNewClusterClusterLogConfS3 | None = Field(None)
    volumes: JobTaskNewClusterClusterLogConfVolumes | None = Field(None)


class JobTaskNewClusterClusterMountInfoNetworkFilesystemInfo(BaseModel):
    mount_options: str | None = Field(None)
    server_address: str = Field(...)


class JobTaskNewClusterClusterMountInfo(BaseModel):
    local_mount_dir_path: str = Field(...)
    remote_mount_dir_path: str | None = Field(None)
    network_filesystem_info: (
        JobTaskNewClusterClusterMountInfoNetworkFilesystemInfo | None
    ) = Field(None)


class JobTaskNewClusterDockerImageBasicAuth(BaseModel):
    password: str = Field(...)
    username: str = Field(...)


class JobTaskNewClusterDockerImage(BaseModel):
    url: str = Field(..., description="URL of the job on the given workspace")
    basic_auth: JobTaskNewClusterDockerImageBasicAuth | None = Field(None)


class JobTaskNewClusterDriverNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobTaskNewClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    boot_disk_size: int | None = Field(None)
    first_on_demand: int | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: int | None = Field(None)
    use_preemptible_executors: bool | None = Field(None)
    zone_id: str | None = Field(None)


class JobTaskNewClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class JobTaskNewClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class JobTaskNewClusterInitScripts(BaseModel):
    abfss: JobTaskNewClusterInitScriptsAbfss | None = Field(None)
    dbfs: JobTaskNewClusterInitScriptsDbfs | None = Field(None)
    file: JobTaskNewClusterInitScriptsFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    gcs: JobTaskNewClusterInitScriptsGcs | None = Field(None)
    s3: JobTaskNewClusterInitScriptsS3 | None = Field(None)
    volumes: JobTaskNewClusterInitScriptsVolumes | None = Field(None)
    workspace: JobTaskNewClusterInitScriptsWorkspace | None = Field(None)


class JobTaskNewClusterLibraryCran(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskNewClusterLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class JobTaskNewClusterLibraryPypi(BaseModel):
    package: str = Field(...)
    repo: str | None = Field(None)


class JobTaskNewClusterLibrary(BaseModel):
    egg: str | None = Field(None)
    jar: str | None = Field(None)
    requirements: str | None = Field(None)
    whl: str | None = Field(None)
    cran: JobTaskNewClusterLibraryCran | None = Field(None)
    maven: JobTaskNewClusterLibraryMaven | None = Field(None)
    pypi: JobTaskNewClusterLibraryPypi | None = Field(None)


class JobTaskNewClusterWorkerNodeTypeFlexibility(BaseModel):
    alternate_node_type_ids: list[str] | None = Field(None)


class JobTaskNewClusterWorkloadTypeClients(BaseModel):
    jobs: bool | None = Field(None)
    notebooks: bool | None = Field(None)


class JobTaskNewClusterWorkloadType(BaseModel):
    clients: JobTaskNewClusterWorkloadTypeClients | None = Field(None)


class JobTaskNewCluster(BaseModel):
    apply_policy_default_values: bool | None = Field(None)
    cluster_id: str | None = Field(None)
    cluster_name: str | None = Field(None)
    custom_tags: dict[str, str] | None = Field(None)
    data_security_mode: str | None = Field(None)
    driver_instance_pool_id: str | None = Field(None)
    driver_node_type_id: str | None = Field(None)
    enable_elastic_disk: bool | None = Field(None)
    enable_local_disk_encryption: bool | None = Field(None)
    idempotency_token: str | None = Field(None)
    instance_pool_id: str | None = Field(None)
    is_single_node: bool | None = Field(None)
    kind: str | None = Field(None)
    node_type_id: str | None = Field(None)
    num_workers: int | None = Field(None)
    policy_id: str | None = Field(None)
    remote_disk_throughput: int | None = Field(None)
    runtime_engine: str | None = Field(None)
    single_user_name: str | None = Field(None)
    spark_conf: dict[str, str] | None = Field(None)
    spark_env_vars: dict[str, str] | None = Field(None)
    spark_version: str | None = Field(None)
    ssh_public_keys: list[str] | None = Field(None)
    total_initial_remote_disk_size: int | None = Field(None)
    use_ml_runtime: bool | None = Field(None)
    autoscale: JobTaskNewClusterAutoscale | None = Field(None)
    aws_attributes: JobTaskNewClusterAwsAttributes | None = Field(None)
    azure_attributes: JobTaskNewClusterAzureAttributes | None = Field(None)
    cluster_log_conf: JobTaskNewClusterClusterLogConf | None = Field(None)
    cluster_mount_info: list[JobTaskNewClusterClusterMountInfo] | None = PluralField(
        None, plural="cluster_mount_infos"
    )
    docker_image: JobTaskNewClusterDockerImage | None = Field(None)
    driver_node_type_flexibility: JobTaskNewClusterDriverNodeTypeFlexibility | None = (
        Field(None)
    )
    gcp_attributes: JobTaskNewClusterGcpAttributes | None = Field(None)
    init_scripts: list[JobTaskNewClusterInitScripts] | None = Field(None)
    library: list[JobTaskNewClusterLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    worker_node_type_flexibility: JobTaskNewClusterWorkerNodeTypeFlexibility | None = (
        Field(None)
    )
    workload_type: JobTaskNewClusterWorkloadType | None = Field(
        None, description="isn't supported"
    )


class JobTaskNotebookTask(BaseModel):
    base_parameters: dict[str, str] | None = Field(
        None,
        description="(Map) Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters and in run-now, the value from run-now will be used. If the notebook takes a parameter that is not specified in the job's base_parameters or the run-now override parameters, the default value from the notebook will be used. Retrieve these parameters in a notebook using `dbutils.widgets.get`",
    )
    notebook_path: str = Field(
        ...,
        description="The path of the [databricks_notebook](notebook.md#path) to be run in the Databricks workspace or remote repository. For notebooks stored in the Databricks workspace, the path must be absolute and begin with a slash. For notebooks stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )


class JobTaskNotificationSettings(BaseModel):
    alert_on_last_attempt: bool | None = Field(
        None,
        description="(Bool) do not send notifications to recipients specified in `on_start` for the retried runs and do not send notifications to recipients specified in `on_failure` until the last retry of the run",
    )
    no_alert_for_canceled_runs: bool | None = Field(
        None, description="(Bool) don't send alert for cancelled runs"
    )
    no_alert_for_skipped_runs: bool | None = Field(
        None, description="(Bool) don't send alert for skipped runs"
    )


class JobTaskPipelineTask(BaseModel):
    full_refresh: bool | None = Field(
        None,
        description="(Bool) Specifies if there should be full refresh of the pipeline",
    )
    pipeline_id: str = Field(..., description="The pipeline's unique ID")


class JobTaskPowerBiTaskPowerBiModel(BaseModel):
    authentication_method: str | None = Field(None)
    model_name: str | None = Field(None)
    overwrite_existing: bool | None = Field(None)
    storage_mode: str | None = Field(None)
    workspace_name: str | None = Field(None)


class JobTaskPowerBiTaskTables(BaseModel):
    catalog: str | None = Field(
        None, description="The name of the catalog to use inside Unity Catalog"
    )
    name: str | None = Field(
        None,
        description="The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`",
    )
    schema_: str | None = Field(
        None,
        description="The name of the schema dbt should run in. Defaults to `default`",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    storage_mode: str | None = Field(None)


class JobTaskPowerBiTask(BaseModel):
    connection_resource_name: str | None = Field(None)
    refresh_after_update: bool | None = Field(None)
    warehouse_id: str | None = Field(
        None,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    power_bi_model: JobTaskPowerBiTaskPowerBiModel | None = Field(None)
    tables: list[JobTaskPowerBiTaskTables] | None = Field(None)


class JobTaskPythonWheelTask(BaseModel):
    entry_point: str | None = Field(
        None, description="Python function as entry point for the task"
    )
    named_parameters: dict[str, str] | None = Field(
        None, description="Named parameters for the task"
    )
    package_name: str | None = Field(None, description="Name of Python package")
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobTaskRunJobTaskPipelineParams(BaseModel):
    full_refresh: bool | None = Field(
        None,
        description="(Bool) Specifies if there should be full refresh of the pipeline",
    )


class JobTaskRunJobTask(BaseModel):
    dbt_commands: list[str] | None = Field(None)
    jar_params: list[str] | None = Field(None)
    job_id: int = Field(..., description="(String) ID of the job")
    job_parameters: dict[str, str] | None = Field(
        None, description="(Map) Job parameters for the task"
    )
    notebook_params: dict[str, str] | None = Field(None)
    python_named_params: dict[str, str] | None = Field(None)
    python_params: list[str] | None = Field(None)
    spark_submit_params: list[str] | None = Field(None)
    sql_params: dict[str, str] | None = Field(None)
    pipeline_params: JobTaskRunJobTaskPipelineParams | None = Field(None)


class JobTaskSparkJarTask(BaseModel):
    jar_uri: str | None = Field(None)
    main_class_name: str | None = Field(
        None,
        description="The full name of the class containing the main method to be executed. This class must be contained in a JAR provided as a library. The code should use `SparkContext.getOrCreate` to obtain a Spark context; otherwise, runs of the job will fail",
    )
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    run_as_repl: bool | None = Field(None)


class JobTaskSparkPythonTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    python_file: str = Field(
        ...,
        description="The URI of the Python file to be executed. Cloud file URIs (e.g. `s3:/`, `abfss:/`, `gs:/`), workspace paths and remote repository are supported. For Python files stored in the Databricks workspace, the path must be absolute and begin with `/`. For files stored in a remote repository, the path must be relative. This field is required",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )


class JobTaskSparkSubmitTask(BaseModel):
    parameters: list[str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )


class JobTaskSqlTaskAlertSubscriptions(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskSqlTaskAlert(BaseModel):
    alert_id: str = Field(
        ...,
        description="(String) identifier of the Databricks Alert ([databricks_alert](alert.md))",
    )
    pause_subscriptions: bool | None = Field(
        None, description="flag that specifies if subscriptions are paused or not"
    )
    subscriptions: list[JobTaskSqlTaskAlertSubscriptions] | None = Field(
        None,
        description="a list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or `destination_id` - for Alert destination's identifier",
    )


class JobTaskSqlTaskDashboardSubscriptions(BaseModel):
    destination_id: str | None = Field(None)
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class JobTaskSqlTaskDashboard(BaseModel):
    custom_subject: str | None = Field(
        None, description="string specifying a custom subject of email sent"
    )
    dashboard_id: str = Field(
        ...,
        description="(String) identifier of the Databricks SQL Dashboard [databricks_sql_dashboard](sql_dashboard.md)",
    )
    pause_subscriptions: bool | None = Field(
        None, description="flag that specifies if subscriptions are paused or not"
    )
    subscriptions: list[JobTaskSqlTaskDashboardSubscriptions] | None = Field(
        None,
        description="a list of subscription blocks consisting out of one of the required fields: `user_name` for user emails or `destination_id` - for Alert destination's identifier",
    )


class JobTaskSqlTaskFile(BaseModel):
    path: str = Field(
        ...,
        description="If `source` is `GIT`: Relative path to the file in the repository specified in the `git_source` block with SQL commands to execute. If `source` is `WORKSPACE`: Absolute path to the file in the workspace with SQL commands to execute",
    )
    source: str | None = Field(
        None,
        description="The source of the project. Possible values are `WORKSPACE` and `GIT`",
    )


class JobTaskSqlTaskQuery(BaseModel):
    query_id: str = Field(...)


class JobTaskSqlTask(BaseModel):
    parameters: dict[str, str] | None = Field(
        None,
        description="(Map) parameters to be used for each run of this task. The SQL alert task does not support custom parameters",
    )
    warehouse_id: str = Field(
        ...,
        description="ID of the (the [databricks_sql_endpoint](sql_endpoint.md)) that will be used to execute the task.  Only Serverless & Pro warehouses are supported right now",
    )
    alert: JobTaskSqlTaskAlert | None = Field(
        None, description="block consisting of following fields:"
    )
    dashboard: JobTaskSqlTaskDashboard | None = Field(
        None, description="block consisting of following fields:"
    )
    file: JobTaskSqlTaskFile | None = Field(
        None, description="block consisting of single string fields:"
    )
    query: JobTaskSqlTaskQuery | None = Field(
        None,
        description="block consisting of single string field: `query_id` - identifier of the Databricks Query ([databricks_query](query.md))",
    )


class JobTaskWebhookNotificationsOnDurationWarningThresholdExceeded(BaseModel):
    pass


class JobTaskWebhookNotificationsOnFailure(BaseModel):
    pass


class JobTaskWebhookNotificationsOnStart(BaseModel):
    pass


class JobTaskWebhookNotificationsOnStreamingBacklogExceeded(BaseModel):
    pass


class JobTaskWebhookNotificationsOnSuccess(BaseModel):
    pass


class JobTaskWebhookNotifications(BaseModel):
    on_duration_warning_threshold_exceeded: (
        list[JobTaskWebhookNotificationsOnDurationWarningThresholdExceeded] | None
    ) = PluralField(
        None,
        plural="on_duration_warning_threshold_exceededs",
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[JobTaskWebhookNotificationsOnFailure] | None = PluralField(
        None,
        plural="on_failures",
        description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
    )
    on_start: list[JobTaskWebhookNotificationsOnStart] | None = PluralField(
        None,
        plural="on_starts",
        description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
    )
    on_streaming_backlog_exceeded: (
        list[JobTaskWebhookNotificationsOnStreamingBacklogExceeded] | None
    ) = PluralField(
        None,
        plural="on_streaming_backlog_exceededs",
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[JobTaskWebhookNotificationsOnSuccess] | None = PluralField(
        None,
        plural="on_successes",
        description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
    )


class JobTask(BaseModel):
    description: str | None = Field(None, description="description for this task")
    disable_auto_optimization: bool | None = Field(
        None, description="A flag to disable auto optimization in serverless tasks"
    )
    disabled: bool | None = Field(None)
    environment_key: str | None = Field(
        None,
        description="an unique identifier of the Environment.  It will be referenced from `environment_key` attribute of corresponding task",
    )
    existing_cluster_id: str | None = Field(
        None,
        description="Identifier of the [interactive cluster](cluster.md) to run job on.  *Note: running tasks on interactive clusters may lead to increased costs!*",
    )
    job_cluster_key: str | None = Field(
        None,
        description="Identifier that can be referenced in `task` block, so that cluster is shared between tasks",
    )
    max_retries: int | None = Field(
        None,
        description="(Integer) An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it completes with a `FAILED` or `INTERNAL_ERROR` lifecycle state. The value -1 means to retry indefinitely and the value 0 means to never retry. The default behavior is to never retry. A run can have the following lifecycle state: `PENDING`, `RUNNING`, `TERMINATING`, `TERMINATED`, `SKIPPED` or `INTERNAL_ERROR`",
    )
    min_retry_interval_millis: int | None = Field(
        None,
        description="(Integer) An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run. The default behavior is that unsuccessful runs are immediately retried",
    )
    retry_on_timeout: bool | None = Field(
        None,
        description="(Bool) An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on timeout",
    )
    run_if: str | None = Field(
        None,
        description="An optional value indicating the condition that determines whether the task should be run once its dependencies have been completed. One of `ALL_SUCCESS`, `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, `ALL_DONE`, `AT_LEAST_ONE_FAILED` or `ALL_FAILED`. When omitted, defaults to `ALL_SUCCESS`",
    )
    task_key: str = Field(..., description="The name of the task this task depends on")
    timeout_seconds: int | None = Field(
        None,
        description="(Integer) An optional timeout applied to each run of this job. The default behavior is to have no timeout",
    )
    alert_task: JobTaskAlertTask | None = Field(None)
    clean_rooms_notebook_task: JobTaskCleanRoomsNotebookTask | None = Field(None)
    compute: JobTaskCompute | None = Field(
        None,
        description="Task level compute configuration. This block is [documented below](#compute-configuration-block)",
    )
    condition_task: JobTaskConditionTask | None = Field(None)
    dashboard_task: JobTaskDashboardTask | None = Field(None)
    dbt_cloud_task: JobTaskDbtCloudTask | None = Field(None)
    dbt_platform_task: JobTaskDbtPlatformTask | None = Field(None)
    dbt_task: JobTaskDbtTask | None = Field(None)
    depends_on: list[JobTaskDependsOn] | None = PluralField(
        None,
        plural="depends_ons",
        description="block specifying dependency(-ies) for a given task",
    )
    email_notifications: JobTaskEmailNotifications | None = Field(
        None,
        description="An optional block to specify a set of email addresses notified when this task begins, completes or fails. The default behavior is to not send any emails. This block is [documented below](#email_notifications-configuration-block)",
    )
    for_each_task: JobTaskForEachTask | None = Field(None)
    gen_ai_compute_task: JobTaskGenAiComputeTask | None = Field(None)
    health: JobTaskHealth | None = Field(
        None,
        description="block described below that specifies health conditions for a given task",
    )
    library: list[JobTaskLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    new_cluster: JobTaskNewCluster | None = Field(
        None,
        description="Block with almost the same set of parameters as for [databricks_cluster](cluster.md) resource, except following (check the [REST API documentation for full list of supported parameters](https://docs.databricks.com/api/workspace/jobs/create#job_clusters-new_cluster)):",
    )
    notebook_task: JobTaskNotebookTask | None = Field(None)
    notification_settings: JobTaskNotificationSettings | None = Field(
        None,
        description="An optional block controlling the notification settings on the job level [documented below](#notification_settings-configuration-block)",
    )
    pipeline_task: JobTaskPipelineTask | None = Field(None)
    power_bi_task: JobTaskPowerBiTask | None = Field(None)
    python_wheel_task: JobTaskPythonWheelTask | None = Field(None)
    run_job_task: JobTaskRunJobTask | None = Field(None)
    spark_jar_task: JobTaskSparkJarTask | None = Field(None)
    spark_python_task: JobTaskSparkPythonTask | None = Field(None)
    spark_submit_task: JobTaskSparkSubmitTask | None = Field(None)
    sql_task: JobTaskSqlTask | None = Field(None)
    webhook_notifications: JobTaskWebhookNotifications | None = Field(
        None,
        description="(List) An optional set of system destinations (for example, webhook destinations or Slack) to be notified when runs of this task begins, completes or fails. The default behavior is to not send any notifications. This field is a block and is documented below",
    )


class JobTimeouts(BaseModel):
    create: str | None = Field(None)
    update_: str | None = Field(
        None,
        serialization_alias="update",
        validation_alias=AliasChoices("update", "update_"),
    )


class JobTriggerFileArrival(BaseModel):
    min_time_between_triggers_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after the specified amount of time passed since the last time the trigger fired. The minimum allowed value is 60 seconds",
    )
    url: str = Field(..., description="URL of the job on the given workspace")
    wait_after_last_change_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after no file activity has occurred for the specified amount of time. This makes it possible to wait for a batch of incoming files to arrive before triggering a run. The minimum allowed value is 60 seconds",
    )


class JobTriggerModel(BaseModel):
    aliases: list[str] | None = Field(None)
    condition: str = Field(
        ...,
        description="The table(s) condition based on which to trigger a job run.  Possible values are `ANY_UPDATED`, `ALL_UPDATED`",
    )
    min_time_between_triggers_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after the specified amount of time passed since the last time the trigger fired. The minimum allowed value is 60 seconds",
    )
    securable_name: str | None = Field(None)
    wait_after_last_change_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after no file activity has occurred for the specified amount of time. This makes it possible to wait for a batch of incoming files to arrive before triggering a run. The minimum allowed value is 60 seconds",
    )


class JobTriggerPeriodic(BaseModel):
    interval: int = Field(
        ..., description="Specifies the interval at which the job should run"
    )
    unit: str = Field(
        ...,
        description="The unit of time for the interval.  Possible values are: `DAYS`, `HOURS`, `WEEKS`",
    )


class JobTriggerTableUpdate(BaseModel):
    condition: str | None = Field(
        None,
        description="The table(s) condition based on which to trigger a job run.  Possible values are `ANY_UPDATED`, `ALL_UPDATED`",
    )
    min_time_between_triggers_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after the specified amount of time passed since the last time the trigger fired. The minimum allowed value is 60 seconds",
    )
    table_names: list[str] = Field(
        ...,
        description="A non-empty list of tables to monitor for changes. The table name must be in the format `catalog_name.schema_name.table_name`",
    )
    wait_after_last_change_seconds: int | None = Field(
        None,
        description="If set, the trigger starts a run only after no file activity has occurred for the specified amount of time. This makes it possible to wait for a batch of incoming files to arrive before triggering a run. The minimum allowed value is 60 seconds",
    )


class JobTrigger(BaseModel):
    pause_status: str | None = Field(
        None,
        description="Indicate whether this trigger is paused or not. Either `PAUSED` or `UNPAUSED`. When the `pause_status` field is omitted in the block, the server will default to using `UNPAUSED` as a value for `pause_status`",
    )
    file_arrival: JobTriggerFileArrival | None = Field(
        None,
        description="configuration block to define a trigger for [File Arrival events](https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/file-arrival-triggers) consisting of following attributes:",
    )
    model: JobTriggerModel | None = Field(None)
    periodic: JobTriggerPeriodic | None = Field(
        None,
        description="configuration block to define a trigger for Periodic Triggers consisting of the following attributes:",
    )
    table_update: JobTriggerTableUpdate | None = Field(
        None,
        description="configuration block to define a trigger for [Table Updates](https://docs.databricks.com/aws/en/jobs/trigger-table-update) consisting of following attributes:",
    )


class JobWebhookNotificationsOnDurationWarningThresholdExceeded(BaseModel):
    pass


class JobWebhookNotificationsOnFailure(BaseModel):
    pass


class JobWebhookNotificationsOnStart(BaseModel):
    pass


class JobWebhookNotificationsOnStreamingBacklogExceeded(BaseModel):
    pass


class JobWebhookNotificationsOnSuccess(BaseModel):
    pass


class JobWebhookNotifications(BaseModel):
    on_duration_warning_threshold_exceeded: (
        list[JobWebhookNotificationsOnDurationWarningThresholdExceeded] | None
    ) = PluralField(
        None,
        plural="on_duration_warning_threshold_exceededs",
        description="(List) list of notification IDs to call when the duration of a run exceeds the threshold specified by the `RUN_DURATION_SECONDS` metric in the `health` block",
    )
    on_failure: list[JobWebhookNotificationsOnFailure] | None = PluralField(
        None,
        plural="on_failures",
        description="(List) list of notification IDs to call when the run fails. A maximum of 3 destinations can be specified",
    )
    on_start: list[JobWebhookNotificationsOnStart] | None = PluralField(
        None,
        plural="on_starts",
        description="(List) list of notification IDs to call when the run starts. A maximum of 3 destinations can be specified",
    )
    on_streaming_backlog_exceeded: (
        list[JobWebhookNotificationsOnStreamingBacklogExceeded] | None
    ) = PluralField(
        None,
        plural="on_streaming_backlog_exceededs",
        description="(List) list of notification IDs to call when any streaming backlog thresholds are exceeded for any stream",
    )
    on_success: list[JobWebhookNotificationsOnSuccess] | None = PluralField(
        None,
        plural="on_successes",
        description="(List) list of notification IDs to call when the run completes successfully. A maximum of 3 destinations can be specified",
    )


class JobBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_job`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    always_running: bool | None = Field(
        None,
        description="(Bool) Whenever the job is always running, like a Spark Streaming application, on every update restart the current active run or start it again, if nothing it is not running. False by default. Any job runs are started with `parameters` specified in `spark_jar_task` or `spark_submit_task` or `spark_python_task` or `notebook_task` blocks",
    )
    budget_policy_id: str | None = Field(
        None,
        description="The ID of the user-specified budget policy to use for this job. If not specified, a default budget policy may be applied when creating or modifying the job",
    )
    control_run_state: bool | None = Field(
        None,
        description="(Bool) If true, the Databricks provider will stop and start the job as needed to ensure that the active run for the job reflects the deployed configuration. For continuous jobs, the provider respects the `pause_status` by stopping the current active run. This flag cannot be set for non-continuous jobs",
    )
    description: str | None = Field(None, description="description for this task")
    edit_mode: str | None = Field(
        None,
        description="If `'UI_LOCKED'`, the user interface for the job will be locked. If `'EDITABLE'` (the default), the user interface will be editable",
    )
    existing_cluster_id: str | None = Field(
        None,
        description="Identifier of the [interactive cluster](cluster.md) to run job on.  *Note: running tasks on interactive clusters may lead to increased costs!*",
    )
    format: str | None = Field(None)
    max_concurrent_runs: int | None = Field(
        None,
        description="(Integer) An optional maximum allowed number of concurrent runs of the job. Defaults to *1*",
    )
    max_retries: int | None = Field(
        None,
        description="(Integer) An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it completes with a `FAILED` or `INTERNAL_ERROR` lifecycle state. The value -1 means to retry indefinitely and the value 0 means to never retry. The default behavior is to never retry. A run can have the following lifecycle state: `PENDING`, `RUNNING`, `TERMINATING`, `TERMINATED`, `SKIPPED` or `INTERNAL_ERROR`",
    )
    min_retry_interval_millis: int | None = Field(
        None,
        description="(Integer) An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run. The default behavior is that unsuccessful runs are immediately retried",
    )
    name: str | None = Field(
        None,
        description="The name of the defined parameter. May only contain alphanumeric characters, `_`, `-`, and `.`",
    )
    performance_target: str | None = Field(
        None,
        description="The performance mode on a serverless job. The performance target determines the level of compute performance or cost-efficiency for the run.  Supported values are: * `PERFORMANCE_OPTIMIZED`: (default value) Prioritizes fast startup and execution times through rapid scaling and optimized cluster performance. * `STANDARD`: Enables cost-efficient execution of serverless workloads",
    )
    retry_on_timeout: bool | None = Field(
        None,
        description="(Bool) An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on timeout",
    )
    tags: dict[str, str] | None = Field(
        None,
        description="An optional map of the tags associated with the job. See [tags Configuration Map](#tags-configuration-map)",
    )
    timeout_seconds: int | None = Field(
        None,
        description="(Integer) An optional timeout applied to each run of this job. The default behavior is to have no timeout",
    )
    usage_policy_id: str | None = Field(None)
    continuous: JobContinuous | None = Field(None)
    dbt_task: JobDbtTask | None = Field(None)
    deployment: JobDeployment | None = Field(None)
    email_notifications: JobEmailNotifications | None = Field(
        None,
        description="An optional block to specify a set of email addresses notified when this task begins, completes or fails. The default behavior is to not send any emails. This block is [documented below](#email_notifications-configuration-block)",
    )
    environment: list[JobEnvironment] | None = PluralField(None, plural="environments")
    git_source: JobGitSource | None = Field(
        None,
        description="Specifies the a Git repository for task source code. See [git_source Configuration Block](#git_source-configuration-block) below",
    )
    health: JobHealth | None = Field(
        None,
        description="block described below that specifies health conditions for a given task",
    )
    job_cluster: list[JobJobCluster] | None = PluralField(
        None,
        plural="job_clusters",
        description="A list of job [databricks_cluster](cluster.md) specifications that can be shared and reused by tasks of this job. Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in task settings. *Multi-task syntax*",
    )
    library: list[JobLibrary] | None = PluralField(
        None,
        plural="libraries",
        description="(Set) An optional list of libraries to be installed on the cluster that will execute the job",
    )
    new_cluster: JobNewCluster | None = Field(
        None,
        description="Block with almost the same set of parameters as for [databricks_cluster](cluster.md) resource, except following (check the [REST API documentation for full list of supported parameters](https://docs.databricks.com/api/workspace/jobs/create#job_clusters-new_cluster)):",
    )
    notebook_task: JobNotebookTask | None = Field(None)
    notification_settings: JobNotificationSettings | None = Field(
        None,
        description="An optional block controlling the notification settings on the job level [documented below](#notification_settings-configuration-block)",
    )
    parameter: list[JobParameter] | None = PluralField(
        None,
        plural="parameters",
        description="Specifies job parameter for the job. See [parameter Configuration Block](#parameter-configuration-block)",
    )
    pipeline_task: JobPipelineTask | None = Field(None)
    python_wheel_task: JobPythonWheelTask | None = Field(None)
    queue: JobQueue | None = Field(
        None,
        description="The queue status for the job. See [queue Configuration Block](#queue-configuration-block) below",
    )
    run_as: JobRunAs | None = Field(
        None,
        description="The user or the service principal the job runs as. See [run_as Configuration Block](#run_as-configuration-block) below",
    )
    run_job_task: JobRunJobTask | None = Field(None)
    schedule: JobSchedule | None = Field(
        None,
        description="An optional periodic schedule for this job. The default behavior is that the job runs when triggered by clicking Run Now in the Jobs UI or sending an API request to runNow. See [schedule Configuration Block](#schedule-configuration-block) below",
    )
    spark_jar_task: JobSparkJarTask | None = Field(None)
    spark_python_task: JobSparkPythonTask | None = Field(None)
    spark_submit_task: JobSparkSubmitTask | None = Field(None)
    task: list[JobTask] | None = PluralField(
        None, plural="tasks", description="Task to run against the `inputs` list"
    )
    timeouts: JobTimeouts | None = Field(None)
    trigger: JobTrigger | None = Field(
        None,
        description="The conditions that triggers the job to start. See [trigger Configuration Block](#trigger-configuration-block) below. * `continuous`- (Optional) Configuration block to configure pause status. See [continuous Configuration Block](#continuous-configuration-block)",
    )
    webhook_notifications: JobWebhookNotifications | None = Field(
        None,
        description="(List) An optional set of system destinations (for example, webhook destinations or Slack) to be notified when runs of this task begins, completes or fails. The default behavior is to not send any notifications. This field is a block and is documented below",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_job"


__all__ = [
    "JobContinuous",
    "JobDbtTask",
    "JobDeployment",
    "JobEmailNotifications",
    "JobEnvironment",
    "JobEnvironmentSpec",
    "JobGitSource",
    "JobGitSourceGitSnapshot",
    "JobGitSourceJobSource",
    "JobGitSourceSparseCheckout",
    "JobHealth",
    "JobHealthRules",
    "JobJobCluster",
    "JobJobClusterNewCluster",
    "JobJobClusterNewClusterAutoscale",
    "JobJobClusterNewClusterAwsAttributes",
    "JobJobClusterNewClusterAzureAttributes",
    "JobJobClusterNewClusterAzureAttributesLogAnalyticsInfo",
    "JobJobClusterNewClusterClusterLogConf",
    "JobJobClusterNewClusterClusterLogConfDbfs",
    "JobJobClusterNewClusterClusterLogConfS3",
    "JobJobClusterNewClusterClusterLogConfVolumes",
    "JobJobClusterNewClusterClusterMountInfo",
    "JobJobClusterNewClusterClusterMountInfoNetworkFilesystemInfo",
    "JobJobClusterNewClusterDockerImage",
    "JobJobClusterNewClusterDockerImageBasicAuth",
    "JobJobClusterNewClusterDriverNodeTypeFlexibility",
    "JobJobClusterNewClusterGcpAttributes",
    "JobJobClusterNewClusterInitScripts",
    "JobJobClusterNewClusterInitScriptsAbfss",
    "JobJobClusterNewClusterInitScriptsDbfs",
    "JobJobClusterNewClusterInitScriptsFile",
    "JobJobClusterNewClusterInitScriptsGcs",
    "JobJobClusterNewClusterInitScriptsS3",
    "JobJobClusterNewClusterInitScriptsVolumes",
    "JobJobClusterNewClusterInitScriptsWorkspace",
    "JobJobClusterNewClusterLibrary",
    "JobJobClusterNewClusterLibraryCran",
    "JobJobClusterNewClusterLibraryMaven",
    "JobJobClusterNewClusterLibraryPypi",
    "JobJobClusterNewClusterWorkerNodeTypeFlexibility",
    "JobJobClusterNewClusterWorkloadType",
    "JobJobClusterNewClusterWorkloadTypeClients",
    "JobLibrary",
    "JobLibraryCran",
    "JobLibraryMaven",
    "JobLibraryPypi",
    "JobNewCluster",
    "JobNewClusterAutoscale",
    "JobNewClusterAwsAttributes",
    "JobNewClusterAzureAttributes",
    "JobNewClusterAzureAttributesLogAnalyticsInfo",
    "JobNewClusterClusterLogConf",
    "JobNewClusterClusterLogConfDbfs",
    "JobNewClusterClusterLogConfS3",
    "JobNewClusterClusterLogConfVolumes",
    "JobNewClusterClusterMountInfo",
    "JobNewClusterClusterMountInfoNetworkFilesystemInfo",
    "JobNewClusterDockerImage",
    "JobNewClusterDockerImageBasicAuth",
    "JobNewClusterDriverNodeTypeFlexibility",
    "JobNewClusterGcpAttributes",
    "JobNewClusterInitScripts",
    "JobNewClusterInitScriptsAbfss",
    "JobNewClusterInitScriptsDbfs",
    "JobNewClusterInitScriptsFile",
    "JobNewClusterInitScriptsGcs",
    "JobNewClusterInitScriptsS3",
    "JobNewClusterInitScriptsVolumes",
    "JobNewClusterInitScriptsWorkspace",
    "JobNewClusterLibrary",
    "JobNewClusterLibraryCran",
    "JobNewClusterLibraryMaven",
    "JobNewClusterLibraryPypi",
    "JobNewClusterWorkerNodeTypeFlexibility",
    "JobNewClusterWorkloadType",
    "JobNewClusterWorkloadTypeClients",
    "JobNotebookTask",
    "JobNotificationSettings",
    "JobParameter",
    "JobPipelineTask",
    "JobPythonWheelTask",
    "JobQueue",
    "JobRunAs",
    "JobRunJobTask",
    "JobSchedule",
    "JobSparkJarTask",
    "JobSparkPythonTask",
    "JobSparkSubmitTask",
    "JobTask",
    "JobTaskAlertTask",
    "JobTaskAlertTaskSubscribers",
    "JobTaskCleanRoomsNotebookTask",
    "JobTaskCompute",
    "JobTaskConditionTask",
    "JobTaskDashboardTask",
    "JobTaskDashboardTaskSubscription",
    "JobTaskDashboardTaskSubscriptionSubscribers",
    "JobTaskDbtCloudTask",
    "JobTaskDbtPlatformTask",
    "JobTaskDbtTask",
    "JobTaskDependsOn",
    "JobTaskEmailNotifications",
    "JobTaskForEachTask",
    "JobTaskForEachTaskTask",
    "JobTaskForEachTaskTaskAlertTask",
    "JobTaskForEachTaskTaskAlertTaskSubscribers",
    "JobTaskForEachTaskTaskCleanRoomsNotebookTask",
    "JobTaskForEachTaskTaskCompute",
    "JobTaskForEachTaskTaskConditionTask",
    "JobTaskForEachTaskTaskDashboardTask",
    "JobTaskForEachTaskTaskDashboardTaskSubscription",
    "JobTaskForEachTaskTaskDashboardTaskSubscriptionSubscribers",
    "JobTaskForEachTaskTaskDbtCloudTask",
    "JobTaskForEachTaskTaskDbtPlatformTask",
    "JobTaskForEachTaskTaskDbtTask",
    "JobTaskForEachTaskTaskDependsOn",
    "JobTaskForEachTaskTaskEmailNotifications",
    "JobTaskForEachTaskTaskGenAiComputeTask",
    "JobTaskForEachTaskTaskGenAiComputeTaskCompute",
    "JobTaskForEachTaskTaskHealth",
    "JobTaskForEachTaskTaskHealthRules",
    "JobTaskForEachTaskTaskLibrary",
    "JobTaskForEachTaskTaskLibraryCran",
    "JobTaskForEachTaskTaskLibraryMaven",
    "JobTaskForEachTaskTaskLibraryPypi",
    "JobTaskForEachTaskTaskNewCluster",
    "JobTaskForEachTaskTaskNewClusterAutoscale",
    "JobTaskForEachTaskTaskNewClusterAwsAttributes",
    "JobTaskForEachTaskTaskNewClusterAzureAttributes",
    "JobTaskForEachTaskTaskNewClusterAzureAttributesLogAnalyticsInfo",
    "JobTaskForEachTaskTaskNewClusterClusterLogConf",
    "JobTaskForEachTaskTaskNewClusterClusterLogConfDbfs",
    "JobTaskForEachTaskTaskNewClusterClusterLogConfS3",
    "JobTaskForEachTaskTaskNewClusterClusterLogConfVolumes",
    "JobTaskForEachTaskTaskNewClusterClusterMountInfo",
    "JobTaskForEachTaskTaskNewClusterClusterMountInfoNetworkFilesystemInfo",
    "JobTaskForEachTaskTaskNewClusterDockerImage",
    "JobTaskForEachTaskTaskNewClusterDockerImageBasicAuth",
    "JobTaskForEachTaskTaskNewClusterDriverNodeTypeFlexibility",
    "JobTaskForEachTaskTaskNewClusterGcpAttributes",
    "JobTaskForEachTaskTaskNewClusterInitScripts",
    "JobTaskForEachTaskTaskNewClusterInitScriptsAbfss",
    "JobTaskForEachTaskTaskNewClusterInitScriptsDbfs",
    "JobTaskForEachTaskTaskNewClusterInitScriptsFile",
    "JobTaskForEachTaskTaskNewClusterInitScriptsGcs",
    "JobTaskForEachTaskTaskNewClusterInitScriptsS3",
    "JobTaskForEachTaskTaskNewClusterInitScriptsVolumes",
    "JobTaskForEachTaskTaskNewClusterInitScriptsWorkspace",
    "JobTaskForEachTaskTaskNewClusterLibrary",
    "JobTaskForEachTaskTaskNewClusterLibraryCran",
    "JobTaskForEachTaskTaskNewClusterLibraryMaven",
    "JobTaskForEachTaskTaskNewClusterLibraryPypi",
    "JobTaskForEachTaskTaskNewClusterWorkerNodeTypeFlexibility",
    "JobTaskForEachTaskTaskNewClusterWorkloadType",
    "JobTaskForEachTaskTaskNewClusterWorkloadTypeClients",
    "JobTaskForEachTaskTaskNotebookTask",
    "JobTaskForEachTaskTaskNotificationSettings",
    "JobTaskForEachTaskTaskPipelineTask",
    "JobTaskForEachTaskTaskPowerBiTask",
    "JobTaskForEachTaskTaskPowerBiTaskPowerBiModel",
    "JobTaskForEachTaskTaskPowerBiTaskTables",
    "JobTaskForEachTaskTaskPythonWheelTask",
    "JobTaskForEachTaskTaskRunJobTask",
    "JobTaskForEachTaskTaskRunJobTaskPipelineParams",
    "JobTaskForEachTaskTaskSparkJarTask",
    "JobTaskForEachTaskTaskSparkPythonTask",
    "JobTaskForEachTaskTaskSparkSubmitTask",
    "JobTaskForEachTaskTaskSqlTask",
    "JobTaskForEachTaskTaskSqlTaskAlert",
    "JobTaskForEachTaskTaskSqlTaskAlertSubscriptions",
    "JobTaskForEachTaskTaskSqlTaskDashboard",
    "JobTaskForEachTaskTaskSqlTaskDashboardSubscriptions",
    "JobTaskForEachTaskTaskSqlTaskFile",
    "JobTaskForEachTaskTaskSqlTaskQuery",
    "JobTaskForEachTaskTaskWebhookNotifications",
    "JobTaskForEachTaskTaskWebhookNotificationsOnDurationWarningThresholdExceeded",
    "JobTaskForEachTaskTaskWebhookNotificationsOnFailure",
    "JobTaskForEachTaskTaskWebhookNotificationsOnStart",
    "JobTaskForEachTaskTaskWebhookNotificationsOnStreamingBacklogExceeded",
    "JobTaskForEachTaskTaskWebhookNotificationsOnSuccess",
    "JobTaskGenAiComputeTask",
    "JobTaskGenAiComputeTaskCompute",
    "JobTaskHealth",
    "JobTaskHealthRules",
    "JobTaskLibrary",
    "JobTaskLibraryCran",
    "JobTaskLibraryMaven",
    "JobTaskLibraryPypi",
    "JobTaskNewCluster",
    "JobTaskNewClusterAutoscale",
    "JobTaskNewClusterAwsAttributes",
    "JobTaskNewClusterAzureAttributes",
    "JobTaskNewClusterAzureAttributesLogAnalyticsInfo",
    "JobTaskNewClusterClusterLogConf",
    "JobTaskNewClusterClusterLogConfDbfs",
    "JobTaskNewClusterClusterLogConfS3",
    "JobTaskNewClusterClusterLogConfVolumes",
    "JobTaskNewClusterClusterMountInfo",
    "JobTaskNewClusterClusterMountInfoNetworkFilesystemInfo",
    "JobTaskNewClusterDockerImage",
    "JobTaskNewClusterDockerImageBasicAuth",
    "JobTaskNewClusterDriverNodeTypeFlexibility",
    "JobTaskNewClusterGcpAttributes",
    "JobTaskNewClusterInitScripts",
    "JobTaskNewClusterInitScriptsAbfss",
    "JobTaskNewClusterInitScriptsDbfs",
    "JobTaskNewClusterInitScriptsFile",
    "JobTaskNewClusterInitScriptsGcs",
    "JobTaskNewClusterInitScriptsS3",
    "JobTaskNewClusterInitScriptsVolumes",
    "JobTaskNewClusterInitScriptsWorkspace",
    "JobTaskNewClusterLibrary",
    "JobTaskNewClusterLibraryCran",
    "JobTaskNewClusterLibraryMaven",
    "JobTaskNewClusterLibraryPypi",
    "JobTaskNewClusterWorkerNodeTypeFlexibility",
    "JobTaskNewClusterWorkloadType",
    "JobTaskNewClusterWorkloadTypeClients",
    "JobTaskNotebookTask",
    "JobTaskNotificationSettings",
    "JobTaskPipelineTask",
    "JobTaskPowerBiTask",
    "JobTaskPowerBiTaskPowerBiModel",
    "JobTaskPowerBiTaskTables",
    "JobTaskPythonWheelTask",
    "JobTaskRunJobTask",
    "JobTaskRunJobTaskPipelineParams",
    "JobTaskSparkJarTask",
    "JobTaskSparkPythonTask",
    "JobTaskSparkSubmitTask",
    "JobTaskSqlTask",
    "JobTaskSqlTaskAlert",
    "JobTaskSqlTaskAlertSubscriptions",
    "JobTaskSqlTaskDashboard",
    "JobTaskSqlTaskDashboardSubscriptions",
    "JobTaskSqlTaskFile",
    "JobTaskSqlTaskQuery",
    "JobTaskWebhookNotifications",
    "JobTaskWebhookNotificationsOnDurationWarningThresholdExceeded",
    "JobTaskWebhookNotificationsOnFailure",
    "JobTaskWebhookNotificationsOnStart",
    "JobTaskWebhookNotificationsOnStreamingBacklogExceeded",
    "JobTaskWebhookNotificationsOnSuccess",
    "JobTimeouts",
    "JobTrigger",
    "JobTriggerFileArrival",
    "JobTriggerModel",
    "JobTriggerPeriodic",
    "JobTriggerTableUpdate",
    "JobWebhookNotifications",
    "JobWebhookNotificationsOnDurationWarningThresholdExceeded",
    "JobWebhookNotificationsOnFailure",
    "JobWebhookNotificationsOnStart",
    "JobWebhookNotificationsOnStreamingBacklogExceeded",
    "JobWebhookNotificationsOnSuccess",
    "JobBase",
]
