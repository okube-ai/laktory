import os
import platform
import sys

from laktory._version import VERSION


def show_version_info() -> str:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version

    package_names = {
        # core
        "inflect",
        "narwhals",
        "networkx",
        "planck",
        "platformdirs",
        "prompt_toolkit",
        "pydantic",
        "pydantic-settings",
        "pyyaml",
        "sqlglot",
        "sqlglotc",
        "typer",
        # polars extra
        "deltalake",
        "polars",
        "sqlparse",
        # pyspark extra
        "delta-spark",
        "pyarrow",
        "pyspark",
        # databricks extra
        "databricks-bundles",
        "databricks-sdk",
        # dev
        "mkdocs",
        "mkdocs-material",
        "mkdocs-video",
        "mkdocstrings",
        "pandas",
        "plotly",
        "pytest",
        "pytest-cov",
        "pytest-examples",
        "pytest-mock",
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
        except PackageNotFoundError:
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
