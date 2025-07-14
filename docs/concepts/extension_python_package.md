??? "API Documentation"
    [`laktory.models.resources.databricks.PythonPackage`][laktory.models.resources.databricks.PythonPackage]<br>

When developing production-grade code, it is generally recommended to use Python packages.
This promotes reusability, modularity, clean deployment, and better unit testing.

Laktory supports this approach through resources such as 
`models.resources.databricks.PyhonPackage`, which automates the building and deployment
of wheel files from local Python package source code.

Consider the following directory structure:
```terminal
.
├── lake
│   └── lake
│       ├── __init__.py
│       ├── _version.py
│       ├── dataframe_ext.py
│   └── pyproject.toml
│   └── README.md
├── notebooks
│   └── jobs
│       ├── job_hello.py
├── requirements.txt
├── resources
│   └── pl-stocks-job.yaml
│   └── pythonpackages.yaml
├── stack.yaml
```
In addition to the usual stack and resource files, this structure contains a Python package named `lake`,
defined by a `pyproject.toml` file.

Inside the `lake` package, a custom [Narwhals extension](extension_custom.md) is 
declared for data transformations:
```py title="dataframe_ext.py"
from datetime import datetime

import narwhals as nw

import laktory as lk


@lk.api.register_anyframe_namespace("lake")
class LakeNamespace:
    def __init__(self, _df):
        self._df = _df

    def with_last_modified(self):
        return self._df.with_columns(last_modified=nw.lit(datetime.now()))
```

The stack file declares:

- a `PythonPackage` databricks resource
- a variable `wheel_filepath` that defines the workspace path to which the wheel file will be deployed

```yaml title="stack.yaml"
name: workflows

resources:
  databricks_pythonpackages: !use resources/pythonpackages.yaml
  pipelines:
    pl-stocks-job: !use resources/pl-stocks-job.yaml
  variables:
    wheel_filepath: /Workspace${vars.workspace_laktory_root}wheels/lake-0.0.1-py3-none-any.whl

environments:
  dev:
    variables:
      env: dev
      is_dev: true
```
The `PythonPackage` resource declares:

- the name of the package
- the path to the pyproject.toml file
- the target directory in the Databricks workspace under the Laktory root

```yaml title="pythonpackages.yaml"
workspace-file-lake-package:
  package_name: lake
  config_filepath: ./lake/pyproject.toml
  dirpath: wheels/
  access_controls:
    - group_name: account users
      permission_level: CAN_READ
```

Finally, the pipeline references the wheel file as a dependency and uses the `lake` 
namespace  to apply a custom transformation. The dependencies section ensures the 
package is installed at runtime (in Databricks Jobs or Pipelines) and imported during 
execution.

```yaml title="pl-stocks-job.yaml"
name: pl-stocks-job

orchestrator:
  type: DATABRICKS_JOB

dependencies:
  - laktory==<laktory_version>
  - ${vars.wheel_filepath}

nodes:
    - name: slv_stock_prices
      source:
        table_name: brz_stock_prices
      sinks:
      - table_name: slv_stock_prices_job
        mode: OVERWRITE
      transformer:
        nodes:
        - func_name: lake.with_last_modified
```

When you run `laktory deploy`, the Python package is:

- built into a wheel file using the configuration in pyproject.toml
- deployed as a Databricks workspace file to ${vars.workspace_laktory_root}/wheels/

This workflow enables clean, repeatable deployment of custom transformation logic alongside your data pipelines.