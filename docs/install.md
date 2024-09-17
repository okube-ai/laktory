For typical usage, a simple pip install will do the trick.

```bash
pip install laktory
```

In the spirit of having a package that is as lightweight as possible, only a
few core dependencies will be installed:

[//]: # (* [`databricks-sdk`]&#40;https://pypi.org/project/databricks-sdk/&#41;: The Databricks SDK for Python includes functionality to accelerate development with Python for the Databricks Lakehouse.)
[//]: # (* [`pulumi`]&#40;https://pypi.org/project/pulumi/&#41;: Infrastructure as code tool used to deploy resources.)
* [`networkx`](https://pypi.org/project/networkx/): Creation manipulation of networks for creating pipeline DAG.
* [`pydantic`](https://pypi.org/project/pydantic/): All laktory models derived from Pydantic `BaseModel`.
* [`settus`](https://pypi.org/project/settus/): Cloud-based settings management system.
* [`typer`](https://pypi.org/project/typer/): Library for building CLI applications. 
 
For configuration-specific or more advanced features like testing 
transformations with Spark, deploying using Pulumi or running a job on
Databricks, you will have to install some of the [optional dependencies](#optional-dependencies).

If you've got Python 3.8+ and `pip` installed, you're good to go. 
It is generally recommended to use a virtual environment for the installation. 

The pip installation not only install the python package, but also Laktory CLI that can be invoked with
```cmd
laktory --help
```

## IaC Backends
Laktory supports multiple IaC backends. You will need to install the CLI of your desired backend manually:

* [terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
* [pulumi](https://www.pulumi.com/docs/install/)


## Optional Dependencies

### Dataframes

If you want to run your pipeline locally or test some of the transformations,
you will have to install the dataframe library used by your transformations.
Available options are `spark` and `polars`.

* Apache Spark
  ```cmd
  pip install laktory[spark]
  ```
  For running spark locally, you also need to follow instructions provided [here](https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/). 
  If you use homebrew to install java, your `JAVA_HOME` and `SPARK_HOME` environment variables should look something like:
    * `JAVA_HOME=/opt/homebrew/opt/java`
    * `SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec`



* Polars
```cmd
pip install laktory[polars]
```

### Infrastructure as Code
If you are using Pulumi as the IaC backend, you will want to run 

```cmd
pip install laktory[pulumi]
```


### Orchestrator
If you want to run your pipeline remotely using one of the supported 
orchestrator you will have to install their respective packages.

* Databricks
```cmd
pip install laktory[databricks]
```

### Cloud Provider
If you plan on deployed cloud-specific resources or want to retreive values
from a secrets manager through `settus`, you will have to install 
cloud-specific packages.

* Microsoft Azure: 
  ```bash
  pip install laktory[azure]
  ```

* Amazon Web Services (AWS)
    ```bash
    pip install laktory[aws]
    ```

* Google Cloud Platform (GCP)
    ```bash
    pip install laktory[gcp]
    ```

## Git-based installation
If you need or prefer installing Laktory from git, you can use:
```bash
pip install git+https://github.com/okube-ai/laktory.git@main
```
