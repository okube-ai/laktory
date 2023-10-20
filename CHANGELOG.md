# Release History

## [0.0.7] - 2023-10-20

**Updated**
- Bronze template notebook to leverage configuration file
- Silver template notebook to leverage configuration file

**Added**
- compute.Cluster model
- compute.InitScript model
- compute.Job model
- compute.Notebook model
- compute.Pipeline model
- compute.Warehouse model
- secrets.Secret model
- secrets.SecretScope model
- Pipeline configuration file management

**Breaking Change**
- Refactored landing mount to landing root and changed default configuration to volumes


## [0.0.6] - 2023-10-10
**Added**
- User, service principal and users group models
- Grants models
- Pulumi resources engine for user, group, catalog, schema, volume and associated grants

**Breaking Change**
- Renamed database objects to schema to be aligned with Databricks recommendations

## [0.0.5] - 2023-09-28
**Added**
- Processing method for silver tables
- Silver DLT pipeline template notebook
- `df_schema_flat` function
- `_any` as as supported type for Spark functions

**Updated**
- Excluded fields for `DataEvent` model_dump
- `df_hascolumn` function to support Spark 3.5

**Fixed**
- Table Metadata included derived properties for the columns

## [0.0.4] - 2023-09-27
**Updated**
- Refactored dlt module to support DBR < 13 and clusters in shared access mode

**Fixed**
- model_validate_yaml() for Pipeline model
- table data insert when no data is available

**Breaking Change**
- Removed spark from required dependencies

## [0.0.3] - 2023-09-25
**Added**
- Data Event cloud storage writers (Azure, AWS and Databricks mount)

## [0.0.2] - 2023-09-24
**Added**
- Data Event class
- Data Source classes
- Pipeline class
- Support for BRONZE transformations

## [0.0.1] - 2023-07-13
**Added**
- Initial pypi release
