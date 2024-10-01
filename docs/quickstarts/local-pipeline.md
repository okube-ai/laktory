The `local-pipeline` stack sets up a locally executable data pipeline using Polars as the DataFrame API. Unlike other 
setups, this stack creates a `pipeline.yaml` file but does not generate a `stack.yaml` file.

### Create Pipeline
To create the pipeline, use the following command:
```commandline
laktory quickstart -t local-pipeline
```

#### Files
After running the `quickstart` command, the following structure is created:

```bash
.
├── 00_explore_pipeline.py
├── 01_execute_node_bronze.py
├── 02_execute_node_silver.py
├── 03_execute_pipeline.py
├── 04_code_pipeline.py
├── data
│   ├── stock_metadata.json
│   └── stock_prices.json
└── pipeline.yaml

```

#### Data Directory
The data directory contains JSON files (`stock_metadata.json` and `stock_prices.json`) that are used as input for the
bronze tables in the pipeline.

#### Scripts
The `root` directory includes five scripts, each designed to help you explore how Laktory declares, builds, and executes
a data pipeline. These scripts expose low-level methods that are not commonly used in production but are valuable for
understanding the mechanics behind pipeline execution.

### Execution
No deployment is necessary for this stack. Simply select a script from the root directory (each script functions 
independently), read through the comments, and run it to experience Laktory's ETL capabilities firsthand.
