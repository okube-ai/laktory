For typical usage, a simple pip install will do the trick.

```bash
pip install pydantic
```

The main dependencies are:

* [`pydantic`](https://pypi.org/project/pydantic/): All laktory models derived from Pydantic `BaseModel`.
* [`settus`](https://pypi.org/project/settus/): Cloud-based settings management system.
* [`pulumi`](https://pypi.org/project/pulumi/): Infrastructure as code tool used to deploy resources..
 

If you've got Python 3.8+ and `pip` installed, you're good to go. 
It is generally recommended to use a virtual environment for the installation. 


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


## Spark installation
Laktory facilitate building and deploying custom spark functions on the Lakehouse. 
If you wish to test these functions locally, we recommend also installing the spark optional dependency.

```bash
pip install laktory[spark]
```

## Git-based installation
If you need or prefer installing Laktory from git, you can use:
```bash
pip install git+https://github.com/okube-ai/laktory.git@main
```
