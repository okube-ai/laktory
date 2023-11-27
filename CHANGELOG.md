# Release History
## [0.0.21] - 2023-11-27
###Breaking Changes
* Renamed `table.builder.zone` to `table.builder.layer` to be consistent with industry standards. 

## [0.0.20] - 2023-11-27
###Added
* `header` option when reading CSV event data source 
* `read_options` option when reading event data source
* `aggregation` feature for table builder
* `window_filter` feature for table builder

## [0.0.19] - 2023-11-23
###Fixed
* Gold zone columns in table builder
###Added
* `drop_columns` option in table builder
* `template` property to table builder, allowing to select a template, independent of the zone.  
###Breaking Changes
* Renamed `models.sql.column.Column.to_column` to `models.sql.column.Column.is_column` to clarify that the provided value is a column name.

## [0.0.18] - 2023-11-14
###Added
* Support for externally managed users and groups

## [0.0.17] - 2023-11-13
###Fixed
* Data Event model to support timestamp from string
###Added
* Option to exclude timestamp from data event filepath
* Selects, filter, watermark options for TableDataSource
* Support for joins in Table model
* Silver Star builder
###Breaking Changes
* Refactored Table to move all building configuration into a `TableBuilder` model

## [0.0.16] - 2023-11-08
###Added
* UDFs property to pipeline model
###Breaking Changes
* Refactored InitScript model into the more general WorkspaceFile

## [0.0.15] - 2023-11-07
###Added
* Support for CDC table data source
* Support for SCD table
###Updated
* Automatic catalog and schema assignation to source table from table and pipeline

## [0.0.14] - 2023-11-06
###Fixed
* Pyspark imports when pyspark not installed

## [0.0.13] - 2023-11-06
###Fixed
* Column `spark_func_args` parsing
* Column `spark_func_kwargs` parsing
* Support for `_any` column type
###Added
* `schema_flat` and `has_column` DataFrame extensions for spark connect
###Updated
* Table Data Source to support tables external to the DLT pipeline

## [0.0.12] - 2023-11-06
###Fixed
* Pyspark imports when pyspark not installed

## [0.0.11] - 2023-11-05
###Fixed
* Deployment of pipeline configuration file
###Added
* Spark optional dependencies
* Spark unit tests
* Functions library
* Support for custom functions in silver processing
###Breaking Changes
* Changed API for table columns definition
* Removed databricks-sdk as dependency

## [0.0.10] - 2023-10-31
###Fixed
* `df_has_column` handling of arrays when column name contains a digit

## [0.0.9] - 2023-10-27
###Updated
* df_has_column support for column names with backticks (`)

## [0.0.8] - 2023-10-24
###Added
* Model pulumi dump method
* Support for variables in yaml files

###Breaking Changes
* Deprecated metadata SQL methods

## [0.0.7] - 2023-10-20
###Updated
* Bronze template notebook to leverage configuration file
* Silver template notebook to leverage configuration file
###Added
* compute.Cluster model
* compute.InitScript model
* compute.Job model
* compute.Notebook model
* compute.Pipeline model
* compute.Warehouse model
* secrets.Secret model
* secrets.SecretScope model
* Pipeline configuration file management
###Breaking Changes
* Refactored landing mount to landing root and changed default configuration to volumes

## [0.0.6] - 2023-10-10
###Added
* User, service principal and users group models
* Grants models
* Pulumi resources engine for user, group, catalog, schema, volume and associated grants
###Breaking Changes
* Renamed database objects to schema to be aligned with Databricks recommendations

## [0.0.5] - 2023-09-28
###Added
* Processing method for silver tables
* Silver DLT pipeline template notebook
* `df_schema_flat` function
* `_any` as as supported type for Spark functions
###Updated
* Excluded fields for `DataEvent` model_dump
* `df_hascolumn` function to support Spark 3.5
###Fixed
* Table Metadata included derived properties for the columns

## [0.0.4] - 2023-09-27
###Updated
* Refactored dlt module to support DBR < 13 and clusters in shared access mode
###Fixed
* model_validate_yaml() for Pipeline model
* table data insert when no data is available
###Breaking Changes
* Removed spark from required dependencies

## [0.0.3] - 2023-09-25
###Added
* Data Event cloud storage writers (Azure, AWS and Databricks mount)

## [0.0.2] - 2023-09-24
###Added
* Data Event class
* Data Source classes
* Pipeline class
* Support for BRONZE transformations

## [0.0.1] - 2023-07-13
###Added
* Initial pypi release
