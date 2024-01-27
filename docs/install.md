For typical usage, a simple pip install will do the trick.

```bash
pip install laktory
```

The main dependencies are:

* [`pydantic`](https://pypi.org/project/pydantic/): All laktory models derived from Pydantic `BaseModel`.
* [`settus`](https://pypi.org/project/settus/): Cloud-based settings management system.
* [`pulumi`](https://pypi.org/project/pulumi/): Infrastructure as code tool used to deploy resources.
* [`typer`](https://pypi.org/project/typer/): Library for building CLI applications. 
 

If you've got Python 3.8+ and `pip` installed, you're good to go. 
It is generally recommended to use a virtual environment for the installation. 

The pip installation not only install the python package, but also Laktory CLI that can be invoked with
```cmd
laktory --help
```

## Cloud-specific installation
To benefit from all features, we recommend also installing your cloud provider-specific dependencies:

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

## IaC Backends
Laktory supports multiple IaC backends. You will need to install the CLI of your desired backend manually:

* [terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
* [pulumi](https://www.pulumi.com/docs/install/)

## Spark installation
Laktory facilitate building and deploying custom spark functions. 
If you wish to test these functions locally, we recommend also installing the spark optional dependency.

For running spark locally, you also need to follow instructions provided [here](https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/). 
If you use homebrew to install java, your `JAVA_HOME` and `SPARK_HOME` environment variables should look something like:
* `JAVA_HOME=/opt/homebrew/opt/java`
* `SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec`


```bash
pip install laktory[spark]
```

## Git-based installation
If you need or prefer installing Laktory from git, you can use:
```bash
pip install git+https://github.com/okube-ai/laktory.git@main
```
