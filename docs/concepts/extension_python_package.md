??? "API Documentation"
    [`laktory.models.resources.databricks.PythonPackage`][laktory.models.resources.databricks.PythonPackage]<br>

When developing production-grade code, it is generally recommended to build python 
package as it helps with re-usability, modularity, clean deployment and unit testing.

Some Laktory resources, such as the `models.resources.databricks.PyhonPackage` facilitates 
that process by dynamically building and deploying wheel files out of local python pacakge
source code.

Consider the following structure

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
in which, in addition the stack and resource files,
you also have a python package called `lake` with the required `pyproject.toml` 
configuration file to build the project.

The package itself declares a [Narwhals extension](extension_custom.md) that can be
used for data transformations
```py dataframe_ext.py
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

The stack includes the declaration of the python packages resources and declare the
workspace path to which the wheel file will be deployed as a variable.
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

The `PythonPackage` resource declares the name of the package, the local filepath of
the configuration file as well as the target directory in the workspace (inside laktory
root). 

```yaml title="pythonpackages.yaml"
workspace-file-lake-package:
  package_name: lake
  config_filepath: ./lake/pyproject.toml
  dirpath: wheels/
  access_controls:
    - group_name: account users
      permission_level: CAN_READ
```

Finally, the pipeline includes the wheel file as a dependency and uses the `lake`
namespace to declare a transformation. The `dependencies` declaration will ensure
that the package is installed when executed as a Databricks Job / Pipeline and imported
during the pipeline execution.

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


When calling `laktory deploy`, package is built and deployed as a Workspace file in the
{laktory_root}/wheels directory using infrastructure as code.