import os
import platform
import sys

from laktory._version import VERSION


def show_version_info() -> str:
    from importlib.metadata import version

    package_names = {
        "databricks-sdk",
        "delta-spark",
        "deltalake",
        "inflect",
        "mkdocs",
        "mkdocs-material",
        "mkdocs-video",
        "mkdocstrings",
        "narwhals",
        "networkx",
        "numpy",
        "pandas",
        "planck",
        "plotly",
        "polars",
        "prompt_toolkit",
        "pulumi",
        "pulumi_databricks",
        "pyarrow",
        "pydantic",
        "pyspark",
        "pytest",
        "pytest-cov",
        "pytest-examples",
        "python-dateutil",
        "pyyaml",
        "sqlglotsqlparse",
        "typer",
        "typing_extensions",
        "uv",
    }
    packages = {}

    # for d in distributions():
    #     name = d.metadata["Name"]
    #     if name in package_names:
    #         packages[name] = d.version
    # packages = dict(sorted(packages.items()))

    for name in package_names:
        try:
            packages[name] = version(name)
        except:  # noqa: E722
            packages[name] = "NOT FOUND"
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
