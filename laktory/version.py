import os
import platform
import sys
from laktory._version import VERSION


def show_version_info() -> str:

    from importlib.metadata import distributions

    package_names = {
        "azure-identity",
        "azure-storage-blob",
        "boto3",
        "pandas",
        "planck",
        "pulumi",
        "pulumi_aws",
        "pulumi_azure",
        "pulumi_azure_native",
        "pulumi_databricks",
        "pyarrow",
        "pydantic",
        "pyspark",
        "settus",
    }
    packages = {}

    for d in distributions():
        name = d.metadata["Name"]
        if name in package_names:
            packages[name] = d.version
    packages = dict(sorted(packages.items()))

    _packages = {
        "laktory": VERSION,
        "path": os.path.abspath(os.path.dirname(__file__)),
        "python": sys.version.replace("\n", " - "),
        "platform": platform.platform(),
        "": "-------------------",
    }

    packages = {**_packages, **packages}

    info = "Laktory version info:\n"
    info += "--------------------\n"
    info += "\n".join([f"{k}-{v}" for k, v in packages.items()])

    return info

