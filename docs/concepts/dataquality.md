## Data Quality
Laktory elevates data quality to a first-class citizen by integrating it
directly into the dataflow, ensuring issues are caught before data reaches
production. Instead of merely monitoring data quality post-factum—when it may
be too late—Laktory provides mechanisms to intercept and handle data quality 
issues during execution. Whether by failing a pipeline before corrupted data is 
written, or by quarantining invalid records, Laktory guarantees that only clean, 
reliable data enters your analytics.

<img src="/../../images/expectations_diagram.png" alt="node transformer" width="500"/>


### Expectations
??? "API Documentation"
    [`laktory.models.DataQualityExpectation`][laktory.models.DataQualityExpectation]<br>


<img src="/../../images/expectations_logo.png" alt="node transformer" width="100"/>

Data quality expectations can be assigned directly to pipeline nodes, as shown
in the example below:
```yaml
nodes:
  ...
  - name: slv_stock_prices
    source:
      node_name: brz_stock_prices
    transformer:
      ...
    expectations:
    - name: positive price
      expr: close > 0
      action: QUARANTINE
    - name: not emtpy
      type: AGGREGATE
      expr: COUNT(*) > 50
      action: FAIL
    sinks:
    - schema_name: finance
      table_name: slv_stock_prices
    - schema_name: finance
      table_name: slv_stock_prices_quarantine
      is_quarantine: True
  ...
```

#### Expression
Expectations are defined by expressions that evaluate to a boolean value, 
either at the row level (`ROW` `type`) or aggregate level (`AGGREGATE` `type`).
Laktory supports both SQL and DataFrame API functions, giving you flexibility in 
setting quality targets. Here are examples of valid expressions:

* `F.col('close') < 300` (pyspark)
* `close < 300`  (SQL)
* `COUNT(*) > 50`  (SQL)

#### Action

Unlike traditional monitoring solutions, Laktory uses expectations to actively 
control data flow. The following actions can be taken when an expectation is
not met:

* `WARN`: Logs a warning in the pipeline output
* `FAIL`: Raises an exception before data is written, preventing any potential corruption
* `DROP` (`ROW`-type only): Removes records that fail the expectation, preserving data integrity
* `QUARANTINE` (`ROW`-type only): Directs invalid records to designated quarantine sinks (`is_quarantine`), allowing for easy review and further analysis

To accommodate minor deviations, a tolerance (absolute or relative) can be configured. This allows a specified number 
of failures before triggering a warning or failure.

### Check
??? "API Documentation"
    [`laktory.models.DataQualityCheck`][laktory.models.DataQualityCheck]<br>

A data quality check evaluates each expectation at runtime for each node. It records the number and proportion of failed
rows and provides a summary status that considers both failures and any specified tolerances.

### Quarantine
As shown in the example above, a sink with the `is_quarantine` attribute set to `True` will receive only records that
fail expectations with the `QUARANTINE` action. This setup simplifies the process of isolating invalid data for later
review, protecting production data from contamination.

### Compatibility Matrix
Data Quality Expectations in Laktory are designed to be highly adaptable, but
not all configurations are compatible across different scenarios. The tables
below outline which combinations of expectation types, actions, and 
environments are supported.

#### Type vs Action
|              | `ROW` | `AGGREGATE` |
|--------------|-------|-------------|
| `WARN`       | yes   | yes         |
| `FAIL`       | yes   | yes         |
| `DROP`       | yes   | no          |
| `QUARANTINE` | yes   | no          |

#### DataFrame type vs Action
|              | `ROW` | `AGGREGATE` |
|--------------|-------|-------------|
| Batch        | yes   | yes         |
| Streaming    | yes   | no          |

### Orchestrator Limitations
Certain orchestrators impose restrictions on how expectations can be applied
to pipeline operations. Below is a summary of known limitations for supported
orchestrators:

* Databricks Delta Live Tables (DLT)
    * `WARN` expectations appear as `ALLOW` in the DLT Data Quality Tab.
    * `QUARANTINE` expectations appear as `DROP` in the DLT Data Quality Tab, though quarantine sinks remain fully supported.
    * `AGGREGATE` expectations do not appear in the DLT Data Quality Tab but can be accessed via logs.
    * Expectations defined with DataFrame expressions (instead of SQL) are not displayed in the DLT Data Quality Tab.
    * `WARN` and `FAIL` actions on streaming tables are supported only for SQL expressions. Static tables support both SQL and DataFrame expressions.

* Databricks Jobs
    * Expectations on streaming DataFrames using Serverless Compute are not supported
