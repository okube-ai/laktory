# GENERATED FILE — DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_pipeline
from __future__ import annotations

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class PipelineClusterAutoscale(BaseModel):
    max_workers: float = Field(...)
    min_workers: float = Field(...)
    mode: str | None = Field(None)


class PipelineClusterAwsAttributes(BaseModel):
    availability: str | None = Field(None)
    ebs_volume_count: float | None = Field(None)
    ebs_volume_iops: float | None = Field(None)
    ebs_volume_size: float | None = Field(None)
    ebs_volume_throughput: float | None = Field(None)
    ebs_volume_type: str | None = Field(None)
    first_on_demand: float | None = Field(None)
    instance_profile_arn: str | None = Field(None)
    spot_bid_price_percent: float | None = Field(None)
    zone_id: str | None = Field(None)


class PipelineClusterAzureAttributesLogAnalyticsInfo(BaseModel):
    log_analytics_primary_key: str | None = Field(None)
    log_analytics_workspace_id: str | None = Field(None)


class PipelineClusterAzureAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: float | None = Field(None)
    spot_bid_max_price: float | None = Field(None)
    log_analytics_info: PipelineClusterAzureAttributesLogAnalyticsInfo | None = Field(
        None
    )


class PipelineClusterClusterLogConfDbfs(BaseModel):
    destination: str = Field(...)


class PipelineClusterClusterLogConfS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class PipelineClusterClusterLogConfVolumes(BaseModel):
    destination: str = Field(...)


class PipelineClusterClusterLogConf(BaseModel):
    dbfs: PipelineClusterClusterLogConfDbfs | None = Field(None)
    s3: PipelineClusterClusterLogConfS3 | None = Field(None)
    volumes: PipelineClusterClusterLogConfVolumes | None = Field(None)


class PipelineClusterGcpAttributes(BaseModel):
    availability: str | None = Field(None)
    first_on_demand: float | None = Field(None)
    google_service_account: str | None = Field(None)
    local_ssd_count: float | None = Field(None)
    zone_id: str | None = Field(None)


class PipelineClusterInitScriptsAbfss(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScriptsDbfs(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScriptsFile(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScriptsGcs(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScriptsS3(BaseModel):
    canned_acl: str | None = Field(None)
    destination: str = Field(...)
    enable_encryption: bool | None = Field(None)
    encryption_type: str | None = Field(None)
    endpoint: str | None = Field(None)
    kms_key: str | None = Field(None)
    region: str | None = Field(None)


class PipelineClusterInitScriptsVolumes(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScriptsWorkspace(BaseModel):
    destination: str = Field(...)


class PipelineClusterInitScripts(BaseModel):
    abfss: PipelineClusterInitScriptsAbfss | None = Field(None)
    dbfs: PipelineClusterInitScriptsDbfs | None = Field(None)
    file: PipelineClusterInitScriptsFile | None = Field(
        None,
        description="specifies path to a file in Databricks Workspace to include as source. Actual path is specified as `path` attribute inside the block",
    )
    gcs: PipelineClusterInitScriptsGcs | None = Field(None)
    s3: PipelineClusterInitScriptsS3 | None = Field(None)
    volumes: PipelineClusterInitScriptsVolumes | None = Field(None)
    workspace: PipelineClusterInitScriptsWorkspace | None = Field(None)


class PipelineCluster(BaseModel):
    apply_policy_default_values: bool | None = Field(None)
    custom_tags: dict[str, str] | None = Field(None)
    driver_instance_pool_id: str | None = Field(None)
    driver_node_type_id: str | None = Field(None)
    enable_local_disk_encryption: bool | None = Field(None)
    instance_pool_id: str | None = Field(None)
    label: str | None = Field(None)
    node_type_id: str | None = Field(None)
    num_workers: float | None = Field(None)
    policy_id: str | None = Field(None)
    spark_conf: dict[str, str] | None = Field(None)
    spark_env_vars: dict[str, str] | None = Field(None)
    ssh_public_keys: list[str] | None = Field(None)
    autoscale: PipelineClusterAutoscale | None = Field(None)
    aws_attributes: PipelineClusterAwsAttributes | None = Field(None)
    azure_attributes: PipelineClusterAzureAttributes | None = Field(None)
    cluster_log_conf: PipelineClusterClusterLogConf | None = Field(None)
    gcp_attributes: PipelineClusterGcpAttributes | None = Field(None)
    init_scripts: list[PipelineClusterInitScripts] | None = PluralField(
        None, plural="init_scripts"
    )


class PipelineDeployment(BaseModel):
    kind: str = Field(
        ..., description="The deployment method that manages the pipeline"
    )
    metadata_file_path: str | None = Field(
        None,
        description="The path to the file containing metadata about the deployment",
    )


class PipelineEnvironment(BaseModel):
    dependencies: list[str] | None = Field(
        None,
        description="a list of pip dependencies, as supported by the version of pip in this environment. Each dependency is a [pip requirement file line](https://pip.pypa.io/en/stable/reference/requirements-file-format/).  See [API docs](https://docs.databricks.com/api/azure/workspace/pipelines/create#environment-dependencies) for more information",
    )
    environment_version: str | None = Field(None)


class PipelineEventLog(BaseModel):
    catalog: str | None = Field(
        None, description="The UC catalog the event log is published under"
    )
    name: str = Field(
        ..., description="The table name the event log is published to in UC"
    )
    schema_: str | None = Field(
        None,
        description="The UC schema the event log is published under",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )


class PipelineFilters(BaseModel):
    exclude: list[str] | None = Field(None, description="Paths to exclude")
    include: list[str] | None = Field(None, description="Paths to include")


class PipelineGatewayDefinitionConnectionParameters(BaseModel):
    source_catalog: str | None = Field(None)


class PipelineGatewayDefinition(BaseModel):
    connection_id: str | None = Field(
        None,
        description="Deprecated, Immutable. The Unity Catalog connection this gateway pipeline uses to communicate with the source. *Use `connection_name` instead!*",
    )
    connection_name: str = Field(
        ...,
        description="Immutable. The Unity Catalog connection this ingestion pipeline uses to communicate with the source. Specify either ingestion_gateway_id or connection_name",
    )
    gateway_storage_catalog: str = Field(
        ...,
        description="Required, Immutable. The name of the catalog for the gateway pipeline's storage location",
    )
    gateway_storage_name: str | None = Field(
        None,
        description="Required. The Unity Catalog-compatible naming for the gateway storage location. This is the destination to use for the data that is extracted by the gateway. Lakeflow Declarative Pipelines system will automatically create the storage location under the catalog and schema",
    )
    gateway_storage_schema: str = Field(
        ...,
        description="Required, Immutable. The name of the schema for the gateway pipelines's storage location",
    )
    connection_parameters: PipelineGatewayDefinitionConnectionParameters | None = Field(
        None
    )


class PipelineIngestionDefinitionDataStagingOptions(BaseModel):
    catalog_name: str = Field(...)
    schema_name: str = Field(...)
    volume_name: str | None = Field(None)


class PipelineIngestionDefinitionFullRefreshWindow(BaseModel):
    days_of_week: list[str] | None = Field(None)
    start_hour: float = Field(...)
    time_zone_id: str | None = Field(None)


class PipelineIngestionDefinitionObjectsReportTableConfigurationAutoFullRefreshPolicy(
    BaseModel
):
    enabled: bool = Field(...)
    min_interval_hours: float | None = Field(None)


class PipelineIngestionDefinitionObjectsReportTableConfigurationQueryBasedConnectorConfig(
    BaseModel
):
    cursor_columns: list[str] | None = Field(None)
    deletion_condition: str | None = Field(None)
    hard_deletion_sync_min_interval_in_seconds: float | None = Field(None)


class PipelineIngestionDefinitionObjectsReportTableConfigurationWorkdayReportParametersReportParameters(
    BaseModel
):
    key: str | None = Field(None)
    value: str | None = Field(None)


class PipelineIngestionDefinitionObjectsReportTableConfigurationWorkdayReportParameters(
    BaseModel
):
    incremental: bool | None = Field(None)
    parameters: dict[str, str] | None = Field(None)
    report_parameters: (
        list[
            PipelineIngestionDefinitionObjectsReportTableConfigurationWorkdayReportParametersReportParameters
        ]
        | None
    ) = PluralField(None, plural="report_parameterss")


class PipelineIngestionDefinitionObjectsReportTableConfiguration(BaseModel):
    exclude_columns: list[str] | None = Field(None)
    include_columns: list[str] | None = Field(None)
    primary_keys: list[str] | None = Field(None)
    row_filter: str | None = Field(None)
    salesforce_include_formula_fields: bool | None = Field(None)
    scd_type: str | None = Field(None)
    sequence_by: list[str] | None = Field(None)
    auto_full_refresh_policy: (
        PipelineIngestionDefinitionObjectsReportTableConfigurationAutoFullRefreshPolicy
        | None
    ) = Field(None)
    query_based_connector_config: (
        PipelineIngestionDefinitionObjectsReportTableConfigurationQueryBasedConnectorConfig
        | None
    ) = Field(None)
    workday_report_parameters: (
        PipelineIngestionDefinitionObjectsReportTableConfigurationWorkdayReportParameters
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsReport(BaseModel):
    destination_catalog: str = Field(...)
    destination_schema: str = Field(...)
    destination_table: str | None = Field(None)
    source_url: str = Field(...)
    table_configuration: (
        PipelineIngestionDefinitionObjectsReportTableConfiguration | None
    ) = Field(
        None,
        description="Configuration settings to control the ingestion of tables. These settings are applied to all tables in the pipeline",
    )


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptionsFileIngestionOptionsFileFilters(
    BaseModel
):
    modified_after: str | None = Field(None)
    modified_before: str | None = Field(None)
    path_filter: str | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptionsFileIngestionOptions(
    BaseModel
):
    corrupt_record_column: str | None = Field(None)
    format: str | None = Field(None)
    format_options: dict[str, str] | None = Field(None)
    ignore_corrupt_files: bool | None = Field(None)
    infer_column_types: bool | None = Field(None)
    reader_case_sensitive: bool | None = Field(None)
    rescued_data_column: str | None = Field(None)
    schema_evolution_mode: str | None = Field(None)
    schema_hints: str | None = Field(None)
    single_variant_column: str | None = Field(None)
    file_filters: (
        list[
            PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptionsFileIngestionOptionsFileFilters
        ]
        | None
    ) = PluralField(None, plural="file_filterss")


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptions(BaseModel):
    entity_type: str | None = Field(None)
    url: str | None = Field(
        None,
        description="URL of the Lakeflow Declarative Pipeline on the given workspace",
    )
    file_ingestion_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptionsFileIngestionOptions
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGoogleAdsOptions(
    BaseModel
):
    lookback_window_days: float | None = Field(None)
    manager_account_id: str = Field(...)
    sync_start_date: str | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptionsFileIngestionOptionsFileFilters(
    BaseModel
):
    modified_after: str | None = Field(None)
    modified_before: str | None = Field(None)
    path_filter: str | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptionsFileIngestionOptions(
    BaseModel
):
    corrupt_record_column: str | None = Field(None)
    format: str | None = Field(None)
    format_options: dict[str, str] | None = Field(None)
    ignore_corrupt_files: bool | None = Field(None)
    infer_column_types: bool | None = Field(None)
    reader_case_sensitive: bool | None = Field(None)
    rescued_data_column: str | None = Field(None)
    schema_evolution_mode: str | None = Field(None)
    schema_hints: str | None = Field(None)
    single_variant_column: str | None = Field(None)
    file_filters: (
        list[
            PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptionsFileIngestionOptionsFileFilters
        ]
        | None
    ) = PluralField(None, plural="file_filterss")


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptions(
    BaseModel
):
    entity_type: str | None = Field(None)
    url: str | None = Field(
        None,
        description="URL of the Lakeflow Declarative Pipeline on the given workspace",
    )
    file_ingestion_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptionsFileIngestionOptions
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptionsTiktokAdsOptions(
    BaseModel
):
    data_level: str | None = Field(None)
    dimensions: list[str] | None = Field(None)
    lookback_window_days: float | None = Field(None)
    metrics: list[str] | None = Field(None)
    query_lifetime: bool | None = Field(None)
    report_type: str | None = Field(None)
    sync_start_date: str | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaConnectorOptions(BaseModel):
    gdrive_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGdriveOptions | None
    ) = Field(None)
    google_ads_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsGoogleAdsOptions | None
    ) = Field(None)
    sharepoint_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsSharepointOptions | None
    ) = Field(None)
    tiktok_ads_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptionsTiktokAdsOptions | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsSchemaTableConfigurationAutoFullRefreshPolicy(
    BaseModel
):
    enabled: bool = Field(...)
    min_interval_hours: float | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaTableConfigurationQueryBasedConnectorConfig(
    BaseModel
):
    cursor_columns: list[str] | None = Field(None)
    deletion_condition: str | None = Field(None)
    hard_deletion_sync_min_interval_in_seconds: float | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaTableConfigurationWorkdayReportParametersReportParameters(
    BaseModel
):
    key: str | None = Field(None)
    value: str | None = Field(None)


class PipelineIngestionDefinitionObjectsSchemaTableConfigurationWorkdayReportParameters(
    BaseModel
):
    incremental: bool | None = Field(None)
    parameters: dict[str, str] | None = Field(None)
    report_parameters: (
        list[
            PipelineIngestionDefinitionObjectsSchemaTableConfigurationWorkdayReportParametersReportParameters
        ]
        | None
    ) = PluralField(None, plural="report_parameterss")


class PipelineIngestionDefinitionObjectsSchemaTableConfiguration(BaseModel):
    exclude_columns: list[str] | None = Field(None)
    include_columns: list[str] | None = Field(None)
    primary_keys: list[str] | None = Field(None)
    row_filter: str | None = Field(None)
    salesforce_include_formula_fields: bool | None = Field(None)
    scd_type: str | None = Field(None)
    sequence_by: list[str] | None = Field(None)
    auto_full_refresh_policy: (
        PipelineIngestionDefinitionObjectsSchemaTableConfigurationAutoFullRefreshPolicy
        | None
    ) = Field(None)
    query_based_connector_config: (
        PipelineIngestionDefinitionObjectsSchemaTableConfigurationQueryBasedConnectorConfig
        | None
    ) = Field(None)
    workday_report_parameters: (
        PipelineIngestionDefinitionObjectsSchemaTableConfigurationWorkdayReportParameters
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsSchema(BaseModel):
    destination_catalog: str = Field(...)
    destination_schema: str = Field(...)
    source_catalog: str | None = Field(None)
    source_schema: str = Field(...)
    connector_options: (
        PipelineIngestionDefinitionObjectsSchemaConnectorOptions | None
    ) = Field(None)
    table_configuration: (
        PipelineIngestionDefinitionObjectsSchemaTableConfiguration | None
    ) = Field(
        None,
        description="Configuration settings to control the ingestion of tables. These settings are applied to all tables in the pipeline",
    )


class PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptionsFileIngestionOptionsFileFilters(
    BaseModel
):
    modified_after: str | None = Field(None)
    modified_before: str | None = Field(None)
    path_filter: str | None = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptionsFileIngestionOptions(
    BaseModel
):
    corrupt_record_column: str | None = Field(None)
    format: str | None = Field(None)
    format_options: dict[str, str] | None = Field(None)
    ignore_corrupt_files: bool | None = Field(None)
    infer_column_types: bool | None = Field(None)
    reader_case_sensitive: bool | None = Field(None)
    rescued_data_column: str | None = Field(None)
    schema_evolution_mode: str | None = Field(None)
    schema_hints: str | None = Field(None)
    single_variant_column: str | None = Field(None)
    file_filters: (
        list[
            PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptionsFileIngestionOptionsFileFilters
        ]
        | None
    ) = PluralField(None, plural="file_filterss")


class PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptions(BaseModel):
    entity_type: str | None = Field(None)
    url: str | None = Field(
        None,
        description="URL of the Lakeflow Declarative Pipeline on the given workspace",
    )
    file_ingestion_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptionsFileIngestionOptions
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptionsGoogleAdsOptions(
    BaseModel
):
    lookback_window_days: float | None = Field(None)
    manager_account_id: str = Field(...)
    sync_start_date: str | None = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptionsFileIngestionOptionsFileFilters(
    BaseModel
):
    modified_after: str | None = Field(None)
    modified_before: str | None = Field(None)
    path_filter: str | None = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptionsFileIngestionOptions(
    BaseModel
):
    corrupt_record_column: str | None = Field(None)
    format: str | None = Field(None)
    format_options: dict[str, str] | None = Field(None)
    ignore_corrupt_files: bool | None = Field(None)
    infer_column_types: bool | None = Field(None)
    reader_case_sensitive: bool | None = Field(None)
    rescued_data_column: str | None = Field(None)
    schema_evolution_mode: str | None = Field(None)
    schema_hints: str | None = Field(None)
    single_variant_column: str | None = Field(None)
    file_filters: (
        list[
            PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptionsFileIngestionOptionsFileFilters
        ]
        | None
    ) = PluralField(None, plural="file_filterss")


class PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptions(
    BaseModel
):
    entity_type: str | None = Field(None)
    url: str | None = Field(
        None,
        description="URL of the Lakeflow Declarative Pipeline on the given workspace",
    )
    file_ingestion_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptionsFileIngestionOptions
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptionsTiktokAdsOptions(
    BaseModel
):
    data_level: str | None = Field(None)
    dimensions: list[str] | None = Field(None)
    lookback_window_days: float | None = Field(None)
    metrics: list[str] | None = Field(None)
    query_lifetime: bool | None = Field(None)
    report_type: str | None = Field(None)
    sync_start_date: str | None = Field(None)


class PipelineIngestionDefinitionObjectsTableConnectorOptions(BaseModel):
    gdrive_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsGdriveOptions | None
    ) = Field(None)
    google_ads_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsGoogleAdsOptions | None
    ) = Field(None)
    sharepoint_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsSharepointOptions | None
    ) = Field(None)
    tiktok_ads_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptionsTiktokAdsOptions | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsTableTableConfigurationAutoFullRefreshPolicy(
    BaseModel
):
    enabled: bool = Field(...)
    min_interval_hours: float | None = Field(None)


class PipelineIngestionDefinitionObjectsTableTableConfigurationQueryBasedConnectorConfig(
    BaseModel
):
    cursor_columns: list[str] | None = Field(None)
    deletion_condition: str | None = Field(None)
    hard_deletion_sync_min_interval_in_seconds: float | None = Field(None)


class PipelineIngestionDefinitionObjectsTableTableConfigurationWorkdayReportParametersReportParameters(
    BaseModel
):
    key: str | None = Field(None)
    value: str | None = Field(None)


class PipelineIngestionDefinitionObjectsTableTableConfigurationWorkdayReportParameters(
    BaseModel
):
    incremental: bool | None = Field(None)
    parameters: dict[str, str] | None = Field(None)
    report_parameters: (
        list[
            PipelineIngestionDefinitionObjectsTableTableConfigurationWorkdayReportParametersReportParameters
        ]
        | None
    ) = PluralField(None, plural="report_parameterss")


class PipelineIngestionDefinitionObjectsTableTableConfiguration(BaseModel):
    exclude_columns: list[str] | None = Field(None)
    include_columns: list[str] | None = Field(None)
    primary_keys: list[str] | None = Field(None)
    row_filter: str | None = Field(None)
    salesforce_include_formula_fields: bool | None = Field(None)
    scd_type: str | None = Field(None)
    sequence_by: list[str] | None = Field(None)
    auto_full_refresh_policy: (
        PipelineIngestionDefinitionObjectsTableTableConfigurationAutoFullRefreshPolicy
        | None
    ) = Field(None)
    query_based_connector_config: (
        PipelineIngestionDefinitionObjectsTableTableConfigurationQueryBasedConnectorConfig
        | None
    ) = Field(None)
    workday_report_parameters: (
        PipelineIngestionDefinitionObjectsTableTableConfigurationWorkdayReportParameters
        | None
    ) = Field(None)


class PipelineIngestionDefinitionObjectsTable(BaseModel):
    destination_catalog: str = Field(...)
    destination_schema: str = Field(...)
    destination_table: str | None = Field(None)
    source_catalog: str | None = Field(None)
    source_schema: str | None = Field(None)
    source_table: str = Field(...)
    connector_options: (
        PipelineIngestionDefinitionObjectsTableConnectorOptions | None
    ) = Field(None)
    table_configuration: (
        PipelineIngestionDefinitionObjectsTableTableConfiguration | None
    ) = Field(
        None,
        description="Configuration settings to control the ingestion of tables. These settings are applied to all tables in the pipeline",
    )


class PipelineIngestionDefinitionObjects(BaseModel):
    report: PipelineIngestionDefinitionObjectsReport | None = Field(None)
    schema_: PipelineIngestionDefinitionObjectsSchema | None = Field(
        None, description="The UC schema the event log is published under"
    )
    table: PipelineIngestionDefinitionObjectsTable | None = Field(None)


class PipelineIngestionDefinitionSourceConfigurationsCatalogPostgresSlotConfig(
    BaseModel
):
    publication_name: str | None = Field(None)
    slot_name: str | None = Field(None)


class PipelineIngestionDefinitionSourceConfigurationsCatalogPostgres(BaseModel):
    slot_config: (
        PipelineIngestionDefinitionSourceConfigurationsCatalogPostgresSlotConfig | None
    ) = Field(None)


class PipelineIngestionDefinitionSourceConfigurationsCatalog(BaseModel):
    source_catalog: str | None = Field(None)
    postgres: PipelineIngestionDefinitionSourceConfigurationsCatalogPostgres | None = (
        Field(None)
    )


class PipelineIngestionDefinitionSourceConfigurations(BaseModel):
    catalog: PipelineIngestionDefinitionSourceConfigurationsCatalog | None = Field(
        None, description="The UC catalog the event log is published under"
    )


class PipelineIngestionDefinitionTableConfigurationAutoFullRefreshPolicy(BaseModel):
    enabled: bool = Field(...)
    min_interval_hours: float | None = Field(None)


class PipelineIngestionDefinitionTableConfigurationQueryBasedConnectorConfig(BaseModel):
    cursor_columns: list[str] | None = Field(None)
    deletion_condition: str | None = Field(None)
    hard_deletion_sync_min_interval_in_seconds: float | None = Field(None)


class PipelineIngestionDefinitionTableConfigurationWorkdayReportParametersReportParameters(
    BaseModel
):
    key: str | None = Field(None)
    value: str | None = Field(None)


class PipelineIngestionDefinitionTableConfigurationWorkdayReportParameters(BaseModel):
    incremental: bool | None = Field(None)
    parameters: dict[str, str] | None = Field(None)
    report_parameters: (
        list[
            PipelineIngestionDefinitionTableConfigurationWorkdayReportParametersReportParameters
        ]
        | None
    ) = PluralField(None, plural="report_parameterss")


class PipelineIngestionDefinitionTableConfiguration(BaseModel):
    exclude_columns: list[str] | None = Field(None)
    include_columns: list[str] | None = Field(None)
    primary_keys: list[str] | None = Field(None)
    row_filter: str | None = Field(None)
    salesforce_include_formula_fields: bool | None = Field(None)
    scd_type: str | None = Field(None)
    sequence_by: list[str] | None = Field(None)
    auto_full_refresh_policy: (
        PipelineIngestionDefinitionTableConfigurationAutoFullRefreshPolicy | None
    ) = Field(None)
    query_based_connector_config: (
        PipelineIngestionDefinitionTableConfigurationQueryBasedConnectorConfig | None
    ) = Field(None)
    workday_report_parameters: (
        PipelineIngestionDefinitionTableConfigurationWorkdayReportParameters | None
    ) = Field(None)


class PipelineIngestionDefinition(BaseModel):
    connection_name: str | None = Field(
        None,
        description="Immutable. The Unity Catalog connection this ingestion pipeline uses to communicate with the source. Specify either ingestion_gateway_id or connection_name",
    )
    connector_type: str | None = Field(None)
    ingest_from_uc_foreign_catalog: bool | None = Field(None)
    ingestion_gateway_id: str | None = Field(
        None,
        description="Immutable. Identifier for the ingestion gateway used by this ingestion pipeline to communicate with the source. Specify either ingestion_gateway_id or connection_name",
    )
    netsuite_jar_path: str | None = Field(None)
    source_type: str | None = Field(None)
    data_staging_options: PipelineIngestionDefinitionDataStagingOptions | None = Field(
        None
    )
    full_refresh_window: PipelineIngestionDefinitionFullRefreshWindow | None = Field(
        None
    )
    objects: list[PipelineIngestionDefinitionObjects] | None = PluralField(
        None,
        plural="objectss",
        description="Required. Settings specifying tables to replicate and the destination for the replicated tables",
    )
    source_configurations: (
        list[PipelineIngestionDefinitionSourceConfigurations] | None
    ) = PluralField(
        None,
        plural="source_configurationss",
        description="Array of objects describing top-level source configurations. See the [REST API docs](https://docs.databricks.com/api/workspace/pipelines/create#ingestion_definition-source_configurations) for reference",
    )
    table_configuration: PipelineIngestionDefinitionTableConfiguration | None = Field(
        None,
        description="Configuration settings to control the ingestion of tables. These settings are applied to all tables in the pipeline",
    )


class PipelineLatestUpdates(BaseModel):
    creation_time: str | None = Field(None)
    state: str | None = Field(None)
    update_id: str | None = Field(None)


class PipelineLibraryFile(BaseModel):
    path: str = Field(...)


class PipelineLibraryGlob(BaseModel):
    include: str = Field(..., description="Paths to include")


class PipelineLibraryMaven(BaseModel):
    coordinates: str = Field(...)
    exclusions: list[str] | None = Field(None)
    repo: str | None = Field(None)


class PipelineLibraryNotebook(BaseModel):
    path: str = Field(...)


class PipelineLibrary(BaseModel):
    jar: str | None = Field(None)
    whl: str | None = Field(None)
    file: PipelineLibraryFile | None = Field(
        None,
        description="specifies path to a file in Databricks Workspace to include as source. Actual path is specified as `path` attribute inside the block",
    )
    glob: PipelineLibraryGlob | None = Field(
        None,
        description="The unified field to include source code. Each entry should have the `include` attribute that can specify a notebook path, a file path, or a folder path that ends `/**` (to include everything from that folder). This field cannot be used together with `notebook` or `file`",
    )
    maven: PipelineLibraryMaven | None = Field(None)
    notebook: PipelineLibraryNotebook | None = Field(
        None,
        description="specifies path to a Databricks Notebook to include as source. Actual path is specified as `path` attribute inside the block",
    )


class PipelineNotification(BaseModel):
    alerts: list[str] | None = Field(None)
    email_recipients: list[str] | None = Field(None)


class PipelineRestartWindow(BaseModel):
    days_of_week: list[str] | None = Field(None)
    start_hour: float = Field(...)
    time_zone_id: str | None = Field(None)


class PipelineRunAs(BaseModel):
    service_principal_name: str | None = Field(
        None,
        description="The application ID of an active service principal. Setting this field requires the `servicePrincipal/user` role",
    )
    user_name: str | None = Field(
        None,
        description="The email of an active workspace user. Non-admin users can only set this field to their own email",
    )


class PipelineTimeouts(BaseModel):
    default: str | None = Field(None)


class PipelineTriggerCron(BaseModel):
    quartz_cron_schedule: str | None = Field(None)
    timezone_id: str | None = Field(None)


class PipelineTriggerManual(BaseModel):
    pass


class PipelineTrigger(BaseModel):
    cron: PipelineTriggerCron | None = Field(None)
    manual: PipelineTriggerManual | None = Field(None)


class PipelineBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_pipeline`.
    DO NOT EDIT — regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    allow_duplicate_names: bool | None = Field(
        None,
        description="Optional boolean flag. If false, deployment will fail if name conflicts with that of another pipeline. default is `false`",
    )
    budget_policy_id: str | None = Field(
        None,
        description="optional string specifying ID of the budget policy for this Lakeflow Declarative Pipeline",
    )
    catalog: str | None = Field(
        None, description="The UC catalog the event log is published under"
    )
    cause: str | None = Field(None)
    channel: str | None = Field(
        None,
        description="optional name of the release channel for Spark version used by Lakeflow Declarative Pipeline.  Supported values are: `CURRENT` (default) and `PREVIEW`",
    )
    cluster_id: str | None = Field(None)
    configuration: dict[str, str] | None = Field(
        None,
        description="An optional list of values to apply to the entire pipeline. Elements must be formatted as key:value pairs. * `library` blocks - Specifies pipeline code",
    )
    continuous: bool | None = Field(
        None,
        description="A flag indicating whether to run the pipeline continuously. The default value is `false`",
    )
    creator_user_name: str | None = Field(None)
    development: bool | None = Field(
        None,
        description="A flag indicating whether to run the pipeline in development mode. The default value is `false`",
    )
    edition: str | None = Field(
        None,
        description="optional name of the [product edition](https://docs.databricks.com/aws/en/dlt/configure-pipeline#choose-a-product-edition). Supported values are: `CORE`, `PRO`, `ADVANCED` (default).  Not required when `serverless` is set to `true`",
    )
    expected_last_modified: float | None = Field(None)
    health: str | None = Field(None)
    last_modified: float | None = Field(None)
    name: str | None = Field(
        None, description="The table name the event log is published to in UC"
    )
    photon: bool | None = Field(
        None,
        description="A flag indicating whether to use Photon engine. The default value is `false`",
    )
    root_path: str | None = Field(
        None,
        description="An optional string specifying the root path for this pipeline. This is used as the root directory when editing the pipeline in the Databricks user interface and it is added to `sys.path` when executing Python sources during pipeline execution. * `cluster` blocks - [Clusters](cluster.md) to run the pipeline. If none is specified, pipelines will automatically select a default cluster configuration for the pipeline. *Please note that Lakeflow Declarative Pipeline clusters are supporting only subset of attributes as described in [documentation](https://docs.databricks.com/api/workspace/pipelines/create#clusters).*  Also, note that `autoscale` block is extended with the `mode` parameter that controls the autoscaling algorithm (possible values are `ENHANCED` for new, enhanced autoscaling algorithm, or `LEGACY` for old algorithm)",
    )
    run_as_user_name: str | None = Field(None)
    schema_: str | None = Field(
        None,
        description="The UC schema the event log is published under",
        serialization_alias="schema",
        validation_alias=AliasChoices("schema", "schema_"),
    )
    serverless: bool | None = Field(
        None,
        description="An optional flag indicating if serverless compute should be used for this Lakeflow Declarative Pipeline.  Requires `catalog` to be set, as it could be used only with Unity Catalog",
    )
    state: str | None = Field(None)
    storage: str | None = Field(
        None,
        description="A location on cloud storage where output data and metadata required for pipeline execution are stored. By default, tables are stored in a subdirectory of this location. *Change of this parameter forces recreation of the pipeline.* (Conflicts with `catalog`)",
    )
    tags: dict[str, str] | None = Field(
        None,
        description="A map of tags associated with the pipeline. These are forwarded to the cluster as cluster tags, and are therefore subject to the same limitations. A maximum of 25 tags can be added to the pipeline",
    )
    target: str | None = Field(
        None,
        description="The name of a database (in either the Hive metastore or in a UC catalog) for persisting pipeline output data. Configuring the target setting allows you to view and query the pipeline output data from the Databricks UI",
    )
    url: str | None = Field(
        None,
        description="URL of the Lakeflow Declarative Pipeline on the given workspace",
    )
    usage_policy_id: str | None = Field(None)
    cluster: list[PipelineCluster] | None = PluralField(None, plural="clusters")
    deployment: PipelineDeployment | None = Field(
        None,
        description="Deployment type of this pipeline. Supports following attributes:",
    )
    environment: PipelineEnvironment | None = Field(None)
    event_log: PipelineEventLog | None = Field(
        None,
        description="an optional block specifying a table where LDP Event Log will be stored.  Consists of the following fields:",
    )
    filters: PipelineFilters | None = Field(
        None,
        description="Filters on which Pipeline packages to include in the deployed graph.  This block consists of following attributes:",
    )
    gateway_definition: PipelineGatewayDefinition | None = Field(
        None,
        description="The definition of a gateway pipeline to support CDC. Consists of following attributes:",
    )
    ingestion_definition: PipelineIngestionDefinition | None = Field(None)
    latest_updates: list[PipelineLatestUpdates] | None = PluralField(
        None, plural="latest_updatess"
    )
    library: list[PipelineLibrary] | None = PluralField(None, plural="libraries")
    notification: list[PipelineNotification] | None = PluralField(
        None, plural="notifications"
    )
    restart_window: PipelineRestartWindow | None = Field(None)
    run_as: PipelineRunAs | None = Field(
        None,
        description="The user or the service principal the pipeline runs as. See [run_as Configuration Block](#run_as-configuration-block) below",
    )
    timeouts: PipelineTimeouts | None = Field(None)
    trigger: PipelineTrigger | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_pipeline"
