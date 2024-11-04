# Release History

## [0.5.0] - Unreleased
### Added
* DataFrameColumnExpression model
* Data Quality Expectations
* Data Quality Checks
* Support for multiple sinks per pipeline node
* Support for quarantine sink
* Root path for laktory, pipelines and pipeline nodes
### Fixed
* Stream writer `FileDataSink` 
* Support for `null` value in `JobTaskSQLTask` queries
* Singularized attribute names in `JobEmailNotifications` for Terraform [[#276](https://github.com/okube-ai/laktory/issues/276)]
* Added missing `source` attribute in `JobTaskSqlTaskFile` [[#275](https://github.com/okube-ai/laktory/issues/275)]
### Updated
* `Job` to automatically alphabetically sort `tasks` [[#286](https://github.com/okube-ai/laktory/issues/286)]
* `Job` now supports `description` [[#277](https://github.com/okube-ai/laktory/issues/277)]
* `JobTaskNotebookTask` now allows `warehouse_id` for compute [[#265](https://github.com/okube-ai/laktory/issues/265)]
* `JobTaskSQLTask` updated to support `null` for queries [[#274](https://github.com/okube-ai/laktory/issues/274)]
### Breaking changes
* Renamed `sql_expr` to `expr` to enable both SQL and DataFrame expressions with auto-detection
* Updated DLT Expectation action "ALLOW" to "WARN"
* Prefixed `dlt_` to `warning_expectations` properties in pipeline nodes
* Refactored default paths for `WorkspaceFile` and `DBFSFile` models for improved target location control [[#263](https://github.com/okube-ai/laktory/issues/263)]
* Refactored Polars reader to read as LazyFrame
* Renamed `PipelineNode` attribute `sink` to `sinks` 

## [0.4.14] - 2024-10-08
### Updated
* Workflows quickstart to include debug script
* Workflows quickstart to better align DLT and job pipeline.

## [0.4.13] - 2024-10-01
### Added
* Grants resources to Stack
* `no_wait` option for Cluster resources
* Polars quickstart
### Fixed
* Missing dependencies when deploying grants and data access with Metastore
### Updated
* Added SQL expression to logs when processing Polars DataFrame
### Breaking changes
* Renamed workspace provider to grants provider in Metastore resource

## [0.4.12] - 2024-09-18
### Added
* Support for multi-segments (semi-column ; separated) SQL statements 
* Support for Databricks Lakeview Dashboard resource
* CLI `destroy` command
* `unity-catalog`, `workspace` and `workflows` template choices for CLI `quickstart`
### Updated
* Better feedback when terraform is not installed
* Added SQL query to pipeline node transformer logs
### Breaking changes
* Removed `backend` and `organization` arguments for CLI
* Combined CLI `pulumi-options` and `terraform-options` into `options`

## [0.4.11] - 2024-08-16
### Added
* `VectorSearchIndex` Databricks resource
* `VectorSearchEndpoint` Databricks resource
* `purge` method for data sink
* `full_refresh` option for pipeline and pipeline node
### Fixed
* Checkpoint location of `TableDataSink`

## [0.4.10] - 2024-07-20
### Fixed
* mergeSchema and overwriteSchema default options in DataSink writers

## [0.4.9] - 2024-07-20
### Added
* Support for models yaml merge
* MwsNccBinding databricks resource
* MwsNetworkConnectivityConfig databricks resource
* Support for Databricks Table `storage_credential_name` and `storage_location` properties 
* Support for `BINARYFILE` (PDF) format in `FileDataSource` with Spark 
### Fixed
* DLT Debug mode when source is streaming and node is not
* DataFrame type propagation when models are used as inputs to other models
* Terraform auto-approve when other options are used
* `show_version_info()` method to display correct packages version

## [0.4.8] - 2024-07-03
### Added
* Support for referencing nodes in SQL queries
* Support for looking up existing resources
* Support for terraform alias providers
* `laktory` namespace to spark.sql.connect
### Fixed
* Parametrized SQL expressions used in the context of DLT
### Updated
* Support for Polars 1.0

## [0.4.7] - 2024-06-27
### Fixed
* Support for parametrized queries when DLT module is loaded

## [0.4.6] - 2024-06-27
### Added
* Support for parametrized queries when DLT module is loaded
### Fixed
* Issue with getting environment stack on null properties

## [0.4.5] - 2024-06-25
### Updated
* `with_column` transformer node method to allow for `None` type

## [0.4.4] - 2024-06-25
### Added
* WorkspaceFile attribute to Pipeline class to customize access controls
### Fixed
* Spark dependencies
* Fixed encoding when reading from yaml files
### Updated
* Changed pipeline JSON file permission from `account users` to `users`
* Smart join to support coalesce of columns outside of the join


## [0.4.3] - 2024-06-12
### Updated
* Dataframe type propagation through all pipeline children
### Fixed
* Reading pipeline node data source is isolation mode

## [0.4.2] - 2024-06-11
### Fixed
* Creation of the same column multiple times in a transformer node

## [0.4.1] - 2024-06-11
### Fixed
* Accessing custom DataFrame functions in custom namespace in SparkChainNode execution

## [0.4.0] - 2024-06-11
### Added
* Support for Polars with FileDataSource
* Support for Polars with FileDataSink
* Support for PolarsChain transformer
* Polars DataFrame extension
* Polars Expressions extension
### Breaking changes
* Refactored column creation inside a transformer node
* Moved laktory Spark dataframe custom functions under a laktory namespace.

## [0.3.3] - 2024-05-30
### Added
* Support for SQL expression in SparkChain node
* Limit option to Data Sources
* Sample option to Data Sources
* Display method for Spark DataFrames
### Updated
* Stack model environments to support overwrite of individual list element
### Fixed
* Pipeline Node data source read with DLT in debug mode

## [0.3.2] - 2024-05-28
### Updated
* Install instructions
### Breaking changes
* Re-organized optional dependencies
* Remove support for Pulumi python

## [0.3.1] - 2024-05-28
### Fixed
* Updated ReadMe
* Stack Validator unit test

## [0.3.0] - 2024-05-28
### Added
* `Pipeline` model, the new central component for building ETL pipelines
* `PipelineNode` model, the `Pipeline` sub-component defining each dataframe in a pipeline
* `FileDataSink` and `TableDataSink` sinks models
* `PipelineNodeDataSource` and `MemoryDataSource` sources model
* Future support for Polars and other types of dataframe
### Updated
* Enabled CDC support for both `FileDataSource` and `TableDataSource`
### Breaking changes
* Merged `DataEventHeader` into `DataEvent` model
* Renamed `EventDataSource` model to `FileDataSource`
* Renamed `name` attribute to `table_name` in `TableDataSource` model
* Removed `SparkChain` support in DataSources
* Renamed `Pipeline` model to `DLTPipeline` model
* Cloud resources moved under models.resources.{provider}.{resource_class} to avoid collisions with future classes.
* Removed `TableBuilder`. `PipelineNode` should be used instead

## [0.2.1] - 2024-05-07
### Added
* Support for spark chain for a data source
* Support for broadcasting in a data source
* YAML model dump for all base models
### Fixed
* Function selection for pyspark connect dataframes

## [0.2.0] - 2024-05-02
### Added
* `SparkChain` a high level class allowing to declare and execute spark operations on a dataframe 
* `SparkColumnNode` the node of a `SparkChain` that builds a new column
* `SparkTableNode` the node of a `SparkChain` that returns a new dataframe
* Moved `filter`, `selects` and `watermarks` properties to `models.BaseDataSource` so that it can be used for all source types
* `models.BaseDataSource` `renames` attribute for renaming columns of the source table
* `models.BaseDataSource` `drops` attribute for dropping columns of the source table
* spark DataFrame `watermark` returns the watermark column and threshold if any 
* spark DataFrame `smart_join` joins, cleans duplicated columns and supports watermarking 
* spark DataFrame `groupby_and_agg` groupby and aggregates in a single function 
* spark DataFrame `window_filter` takes the first n rows over a window 
### Updated
* n/a
### Breaking changes
* Refactored table builder to use SparkChain instead of direct definitions of joins, unions, columns building, etc.

## [0.1.10] - 2024-04-23
### Added
* CLI `run` command to execute remote jobs and pipelines and monitor errors until completion
* `Dispatcher` class to manage and run remote jobs
* `JobRunner` class to run remote jobs
* `PipelineRunner` class to run remote pipelines
* datetime utilities
### Updated
* Environment variables `DATABRICKS_SDK_UPSTREAM` and `DATABRICKS_SDK_UPSTREAM_VERSION` to track laktory metrics as a Databricks partner
### Fixed
* Permissions resource dependencies on `DbfsFile` and `WorkspaceFile`.

## [0.1.9] - 2024-04-17
### Added
* Support for table unions in table builder
* New column property `raise_missing_arg_exception` to allow for some spark function inputs to be missing
* `add`, `sub`, `mul` and `div` spark functions

### Breaking Change
* Renamed `power` spark function to `scaled_power` to prevent conflict with native spark function 

## [0.1.8] - 2024-03-25
### Added
* `quickstart` CLI command to initialize a sample Laktory stack.
* Databricks DBFS file model

## [0.1.7] - 2024-03-15
### Added
* show_version_info() method for bugs reporting
* Git issues templates 

## [0.1.6] - 2024-02-23
### Updated
* Website branding

## [0.1.5] - 2024-02-14
### Added
* Support for DLT views
* Support for providing table builder `drop_duplicates` with a list of columns to consider for the drop duplicates. 
### Fixed
* Propagation of stack variables to resources

## [0.1.4] - 2024-02-12
### Added
* Support for custom join sql expression in `TableJoin` model

## [0.1.3] - 2024-02-10
### Added
* Support for explicit path in `TableDataSource` model

## [0.1.2] - 2024-02-05
### Added
* `Metastore`, `MetastoreAssignment`, `MetastoreDataAccess`, `MwsPermissionAssignment` and `ExternalLocation` models
* `workspace_permission_assginments` field to `Group` model
* `StackValidator` class for testing deployment in both Pulumi and Terraform
### Updated
* Pipeline model supports null catalog (hive metastore)
* Event Data Source supports custom event root path
* Event Data Source supports custom schema location path
* Refactored `core_resources` property to automatically propagate provider and dependencies to sub-resources.
### Breaking Changes
* Refactored default resource name to remove illegal characters, resolve variables and remove resource tags.
## [0.1.1] - 2024-01-28
### Added
* General support for Terraform IaC backend
* AWS Provider
* Azure Provider
* Azure Pulumi (Native) Provider
### Updated
* `BaseModel` `inject_vars` method regular expressions support 
### Breaking changes
* Replaced CLI argument `--stack` with `--org` and `--dev` for a more consistent experience between pulumi and terraform backends

## [0.1.0] - 2024-01-12
### Added
* Automatic creation of resources output variables that can be used in configuration files
* Custom model serialization allowing conversion of keys to camel case
* Laktory CLI
* Stack model to define and deploy a complete collection of resources from yaml files only and manage environments
* Support for cross-references in yaml files. A yaml configuration file can include another.
* `BaseResource` and `PulumiResource` models with all methods required to deploy through pulumi
* `Grants` model
* `GroupMember` model
* `Permissions` model
* `ServicePrincipalRole` model
* `UserRole` model
* `resources` object to a `BaseResource` instance to define and deploy all the associated resources 
### Updated
* `events_root` field of   `DataEventHeader` and `DataEvent` models is now a property for the default value to dynamically account for settings
* `inject_vars` method to support multiple targets (`pulumi_yaml`, `pulumi_py`, etc.)
### Breaking changes
* Modified `groups` field for `Users` and `ServicePrincipal` models to accept group id instead of group name
* Modified `resource_key` for `WorkspaceFile`, `Notebook` and `Directory`
* Removal of Laktory Resources Component (will trigger replacement of all resources unless aliases are used)
* Removal of resources engines classes
* Renamed `permissions` field to `access_controls` in multiple models to be consistent with Databricks API
* Renamed `vars` object to `variables`
* Resources deployment method `deploy()` and `deploy_with_pulumi()` renamed to `to_pulumi()`

## [0.0.29] - 2023-12-20
### Fixed
* Forced newlines character to eliminate discrepancies between Windows and Linux environment when writing pipeline files. 

## [0.0.28] - 2023-12-17
### Updated
* job.continuous.pause_status to allow for arbitrary string (allow variable)
* job.email_notifications.pause_status to allow for arbitrary string (allow variable)
* job.task_condition.pause_status to allow for arbitrary string (allow variable)
* warehouse.channel_name to allow for arbitrary string (allow variable)
* warehouse.spot_instance_policy to allow for arbitrary string (allow variable)
* warehouse.warehouse_type to allow for arbitrary string (allow variable)

## [0.0.27] - 2023-12-16
### Added
* Support for DLT tables expectations

## [0.0.26] - 2023-12-16
### Added
* GitHub releases
* Units conversion spark function

## [0.0.25] - 2023-12-12
### Added
* compare spark function
* API Reference
* doc tests
### Updated
* SparkFuncArgs model to allow constant value
### Breaking changes
* Renamed Producer model to DataProducer
* Renamed Resource model to BaseResource
* Renamed user and permissions resources
* Renamed group and permissions resources
* Renamed pipeline resources

## [0.0.24] - 2023-12-05
### Fixed
* Null values in joining columns with outer join
### Updated
* Variable injection to support Pulumi Output as part of a string
* Column builder requires all inputs available to build a column

## [0.0.23] - 2023-12-01
### Added
* Databricks directory model
* SQL Query model
* Table resource

## [0.0.22] - 2023-11-29
### Added
* Git tag for each release
* Automatic version bump after each release
* Automatic documentation publishing after each release 

## [0.0.21] - 2023-11-27
### Breaking Changes
* Renamed `table.builder.zone` to `table.builder.layer` to be consistent with industry standards. 

## [0.0.20] - 2023-11-27
### Added
* `header` option when reading CSV event data source 
* `read_options` option when reading event data source
* `aggregation` feature for table builder
* `window_filter` feature for table builder

## [0.0.19] - 2023-11-23
### Fixed
* Gold zone columns in table builder
### Added
* `drop_columns` option in table builder
* `template` property to table builder, allowing to select a template, independent of the zone.  
### Breaking Changes
* Renamed `models.sql.column.Column.to_column` to `models.sql.column.Column.is_column` to clarify that the provided value is a column name.

## [0.0.18] - 2023-11-14
### Added
* Support for externally managed users and groups

## [0.0.17] - 2023-11-13
### Fixed
* Data Event model to support timestamp from string
### Added
* Option to exclude timestamp from data event filepath
* Selects, filter, watermark options for TableDataSource
* Support for joins in Table model
* Silver Star builder
### Breaking Changes
* Refactored Table to move all building configuration into a `TableBuilder` model

## [0.0.16] - 2023-11-08
### Added
* UDFs property to pipeline model
### Breaking Changes
* Refactored InitScript model into the more general WorkspaceFile

## [0.0.15] - 2023-11-07
### Added
* Support for CDC table data source
* Support for SCD table
### Updated
* Automatic catalog and schema assignation to source table from table and pipeline

## [0.0.14] - 2023-11-06
### Fixed
* Pyspark imports when pyspark not installed

## [0.0.13] - 2023-11-06
### Fixed
* Column `spark_func_args` parsing
* Column `spark_func_kwargs` parsing
* Support for `_any` column type
### Added
* `schema_flat` and `has_column` DataFrame extensions for spark connect
### Updated
* Table Data Source to support tables external to the DLT pipeline

## [0.0.12] - 2023-11-06
### Fixed
* Pyspark imports when pyspark not installed

## [0.0.11] - 2023-11-05
### Fixed
* Deployment of pipeline configuration file
### Added
* Spark optional dependencies
* Spark unit tests
* Functions library
* Support for custom functions in silver processing
### Breaking Changes
* Changed API for table columns definition
* Removed databricks-sdk as dependency

## [0.0.10] - 2023-10-31
### Fixed
* `df_has_column` handling of arrays when column name contains a digit

## [0.0.9] - 2023-10-27
### Updated
* df_has_column support for column names with backticks (`)

## [0.0.8] - 2023-10-24
### Added
* Model pulumi dump method
* Support for variables in yaml files

### Breaking Changes
* Deprecated metadata SQL methods

## [0.0.7] - 2023-10-20
### Updated
* Bronze template notebook to leverage configuration file
* Silver template notebook to leverage configuration file
### Added
* compute.Cluster model
* compute.InitScript model
* compute.Job model
* compute.Notebook model
* compute.Pipeline model
* compute.Warehouse model
* secrets.Secret model
* secrets.SecretScope model
* Pipeline configuration file management
### Breaking Changes
* Refactored landing mount to landing root and changed default configuration to volumes

## [0.0.6] - 2023-10-10
### Added
* User, service principal and users group models
* Grants models
* Pulumi resources engine for user, group, catalog, schema, volume and associated grants
### Breaking Changes
* Renamed database objects to schema to be aligned with Databricks recommendations

## [0.0.5] - 2023-09-28
### Added
* Processing method for silver tables
* Silver DLT pipeline template notebook
* `df_schema_flat` function
* `_any` as as supported type for Spark functions
### Updated
* Excluded fields for `DataEvent` model_dump
* `df_hascolumn` function to support Spark 3.5
### Fixed
* Table Metadata included derived properties for the columns

## [0.0.4] - 2023-09-27
### Updated
* Refactored dlt module to support DBR < 13 and clusters in shared access mode
### Fixed
* model_validate_yaml() for Pipeline model
* table data insert when no data is available
### Breaking Changes
* Removed spark from required dependencies

## [0.0.3] - 2023-09-25
### Added
* Data Event cloud storage writers (Azure, AWS and Databricks mount)

## [0.0.2] - 2023-09-24
### Added
* Data Event class
* Data Source classes
* Pipeline class
* Support for BRONZE transformations

## [0.0.1] - 2023-07-13
### Added
* Initial pypi release
