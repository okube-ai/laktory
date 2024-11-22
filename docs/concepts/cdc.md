??? "API Documentation"
    [`laktory.models.DataSinkMergeCDCOptions`][laktory.models.DataSinkMergeCDCOptions]<br>

Laktory streamlines Change Data Capture (CDC) processing by supporting a `merge` mode for data sinks.

Many data providers deliver row-level changes, such as inserts, updates, and deletes, via Change Data Capture (CDC). 
While this enables efficient synchronization between systems and reduces the need for full data reloads, the process of
merging data from a source to a target table can be complex. It typically involves using the Delta Table 
[merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) function (or `MERGE INTO` SQL 
statement), defining conditions for each operation, and implementing sophisticated logic to handle out-of-order records.

Laktory simplifies this process by providing configurable CDC merge options, supporting both SCD Types 1 and 2 for target 
tables:

- **SCD Type 1**: Updates records directly, without retaining a history of previous values.
- **SCD Type 2**: Retains a complete history of changes, using start and end indexes to track each version of a record.

## Key Features

Laktory's CDC integration is designed to simplify the incorporation of change data feeds into data pipelines. Its key
features include:

- Compatibility with static batches and streaming micro-batches (with the latter often being more suitable for CDC).
- Automated handling of initial ingestion when the target table does not yet exist.
- Automatic creation of history-tracking columns.
- Support for primary keys and column hash computations to optimize performance.
- YAML-based configuration, consistent with other Laktory models.
- Robust management of out-of-sequence records.
- Options to ignore changes involving `NULL` values.

## Examples
### SCD Type 1

Consider a stock prices table with the following initial data:

| `date`     | `symbol` | `open` | `close` |
|------------|----------|--------|---------|
| 2024-11-02 | APPL     | 0.57   | 0.49    |
| 2024-11-02 | MSFT     | 0.14   | 0.45    |
| 2024-11-03 | APPL     | 0.09   | 0.97    |
| 2024-11-03 | MSFT     | 0.57   | 0.96    |


You receive the following stream of updates: 1 row update, 1 row deletion, and 2 new rows.

| `date`     | `symbol` | `open` | `close` | `_is_deleted` |
|------------|----------|--------|---------|---------------|
| 2024-11-03 | APPL     | 0.99   | 0.97    | False         |
| 2024-11-03 | MSFT     | 0.57   | 0.96    | True          | 
| 2024-11-04 | APPL     | 0.29   | 0.36    | False         |
| 2024-11-04 | MSFT     | 0.25   | 0.39    | False         |

To merge these updates into the target table, define a data sink as follows:

```yaml title="sink.yaml"
path: "stock_prices/"
mode: "MERGE"
merge_cdc_options:
  primary_keys:
  - symbol
  - date
  delete_where: "_is_deleted = true"
  exclude_columns:
  - is_deleted
```

This configuration ensures the following:

- Rows matching the `primary_keys` will be updated. If no match is found, the row will be appended.
- Rows where the `delete_where` expression evaluates to true will be deleted from the target.

Final result:

| `date`     | `symbol` | `open` | `close` |
|------------|----------|--------|---------|
| 2024-11-02 | APPL     | 0.57   | 0.49    |
| 2024-11-02 | MSFT     | 0.14   | 0.45    |
| 2024-11-03 | APPL     | 0.99   | 0.97    |
| 2024-11-04 | APPL     | 0.29   | 0.36    |
| 2024-11-04 | MSFT     | 0.25   | 0.39    |


Additionally, an optional `order_by` attribute can be set to de-duplicate the
source using an explicit sequence.


### SCD Type 2

To use cases requiring to track history of changes, consider this initial 
dataset:

| `date`     | `symbol` | `open` | `close` | `_is_deleted` | `effective_date` | 
|------------|----------|--------|---------|---------------|------------------| 
| 2024-11-02 | APPL     | 0.57   | 0.49    | False         | 2024-11-02       | 
| 2024-11-02 | MSFT     | 0.14   | 0.45    | False         | 2024-11-02       | 
| 2024-11-03 | APPL     | 0.09   | 0.97    | False         | 2024-11-03       | 
| 2024-11-03 | MSFT     | 0.57   | 0.96    | False         | 2024-11-03       | 

and this update:

| `date`     | `symbol` | `open` | `close` | `_is_deleted` | `effective_date`  |
|------------|----------|--------|---------|---------------|-------------------|
| 2024-11-03 | APPL     | 0.99   | 0.97    | False         | 2024-11-04        |
| 2024-11-03 | MSFT     | 0.57   | 0.96    | True          | 2024-11-04        |
| 2024-11-04 | APPL     | 0.29   | 0.36    | False         | 2024-11-04        |
| 2024-11-04 | MSFT     | 0.25   | 0.39    | False         | 2024-11-04        |

In this case, the sink needs to be defined as:

```yaml title="sink.yaml"
path: "stock_prices/"
mode: "MERGE"
merge_cdc_options:
  scd_type: 2
  primary_keys:
  - symbol
  - date
  delete_where: "_is_deleted = true"
  order_by: effective_date
  exclude_columns:
  - is_deleted
  - effective_date
```

Key differences from SCD Type 1 include:

- Setting `scd_type` to 2 to retain a history of all changes.
- Introducing the `order_by` attribute to determine the sequence of updates, ensuring idempotency and handling out-of-sequence records.

Final result:

| `date`     | `symbol` | `open` | `close` | `__start_at` | `__end_at` |
|------------|----------|--------|---------|--------------|------------|
| 2024-11-02 | APPL     | 0.57   | 0.49    | 2024-11-02   | NULL       |
| 2024-11-02 | MSFT     | 0.14   | 0.45    | 2024-11-02   | NULL       |
| 2024-11-03 | APPL     | 0.09   | 0.97    | 2024-11-03   | 2024-11-04 |
| 2024-11-03 | APPL     | 0.99   | 0.97    | 2024-11-03   | NULL       |
| 2024-11-03 | MSFT     | 0.57   | 0.96    | 2024-11-03   | 2024-11-04 |
| 2024-11-04 | APPL     | 0.29   | 0.36    | 2024-11-04   | NULL       |
| 2024-11-04 | MSFT     | 0.25   | 0.39    | 2024-11-04   | NULL       |


Laktory will automatically build `__start_at` and `__end_at` columns to track when a given value was active.

A `NULL` value in the `__end_at` column indicates the record is still active. To retrieve the current state, filter rows 
with a `NULL` value in this column.

## Multi-sinks

Laktory enables the creation of both SCD Type 1 and Type 2 tables from the same source and transformations using the 
following configuration:

```yaml title="pipeline-node.yaml"
name: slv_stock_prices
source:
  node_name: brz_stock_prices
transformer:
  ...
sinks:
- path: "stock_prices_scd1/"
  mode: "MERGE"
  merge_cdc_options:
    scd_type: 1
    primary_keys:
    - symbol
    - date
    order_by: effective_date
    exclude_columns:
    - effective_date
- path: "stock_prices_scd2/"
  mode: "MERGE"
  merge_cdc_options:
    scd_type: 2
    primary_keys:
    - symbol
    - date
    order_by: effective_date
    exclude_columns:
    - effective_date
```
This configuration allows you to generate both SCD Type 1 and SCD Type 2 tables without duplicating transformation
definitions.