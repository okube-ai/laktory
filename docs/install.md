## Core Installation

We recommend [uv](https://github.com/astral-sh/uv) to manage your Python environment and packages, but 
[conda](https://docs.anaconda.com/miniconda/), 
[venv](https://docs.python.org/3/library/venv.html) or other manager can also be used.

=== "UV"

    First, ensure you have installed [UV](https://github.com/astral-sh/uv), and make sure you have [created and activated](https://docs.astral.sh/uv/pip/environments/#python-environments) a Python 3.9+ virtual environment.

[//]: # (    If you haven't, you can follow our [_setting up your environment_]&#40;https://github.com/narwhals-dev/narwhals/blob/main/CONTRIBUTING.md#option-1-use-uv-recommended&#41; guide.)
    Then, run:

    ```terminal
    uv pip install laktory
    ```
    
    You now have Laktory python package and CLI installed.

=== "Python's venv"

    First, ensure you have [created and activated](https://docs.python.org/3/library/venv.html) a Python 3.9+ virtual environment.

    Then, run:

    ```terminal
    pip install laktory
    ```

    You now have Laktory python package and CLI installed.

### Installation Validation

To verify the installation, open a terminal with the virtual environment activated and execute:

```terminal
>>> laktory --version
Laktory CLI version 0.5.13
```

## Optional Dependencies

In the spirit of having a package that is as lightweight as possible, only a
few core dependencies will be installed by default:

* [`networkx`](https://pypi.org/project/networkx/): Creation manipulation of networks for creating pipeline DAG.
* [`pydantic`](https://pypi.org/project/pydantic/): All laktory models derived from Pydantic `BaseModel`.
* [`typer`](https://pypi.org/project/typer/): Library for building CLI applications. 
 
This core only allows to declare data pipelines and stacks. For an execution
or a deployment, one of the optional dependencies must be installed.

### Dataframes

If you want to run your pipeline locally or test some of the transformations,
you will have to install the dataframe library used by your transformations.
Available options are `spark` and `polars`.

* Apache Spark
  ```cmd
  uv pip install laktory[spark]
  ```
  For running spark locally, you also need to follow instructions provided [here](https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/). 
  If you use homebrew to install java, your `JAVA_HOME` and `SPARK_HOME` environment variables should look something like:
    * `JAVA_HOME=/opt/homebrew/opt/java`
    * `SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec`

* Polars
  ```cmd
  uv pip install laktory[polars]
  ```

### Infrastructure as Code
If you use Pulumi as the IaC backend, you will want to run 

```cmd
uv pip install laktory[pulumi]
```

If you use `Terraform`, it has to be [installed manually](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) because it's not python-based and can't be installed from 
`pip`.

### Orchestrator
If you want to run your pipeline remotely using one of the supported 
orchestrator you will have to install their respective packages.

* Databricks
  ```cmd
  uv pip install laktory[databricks] databricks-connect
  ```

### Cloud Provider
The `DataEvent` class lets you write data events to various cloud storage 
accounts, but requires to install additional dependencies.

* Microsoft Azure: 
  ```terminal
  uv pip install laktory[azure]
  ```

* Amazon Web Services (AWS)
    ```terminal
    uv pip install laktory[aws]
    ```
  
[//]: # (* Google Cloud Platform &#40;GCP&#41;)
[//]: # (    ```terminal)
[//]: # (    pip install laktory[gcp])
[//]: # (    ```)

## Git-based installation
If you need or prefer installing Laktory from git, you can use:
```terminal
pip install git+https://github.com/okube-ai/laktory.git@main
```
