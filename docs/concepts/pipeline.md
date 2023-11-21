Out of all the models, the `Pipeline` is most likely the most critical one for building a Lakehouse.

## Delta Live Tables (DLT)
The main purpose of the `Pipeline` model is to declare a [Databricks Delta Live Tables](https://www.databricks.com/product/delta-live-tables) object, a declarative ETL framework that helps simplify streaming and batch transformations.
Simply define the transformations to perform on your data and let DLT pipelines automatically manage task orchestration, cluster management, monitoring, data quality and error handling.
Delta Live Tables are immensely powerful and greatly simplify the process of brining raw data into actionable analytics.

![dlt](../images/delta_live_tables.png)

## 