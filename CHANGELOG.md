# Release History

## [0.0.6]
**Added**
- User, service principal and users group models
- Grants models
- Pulumi resrouces for user, service principal and group models

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
