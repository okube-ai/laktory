# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_library
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import PluralField
from laktory.models.resources.terraformresource import TerraformResource


class LibraryCran(BaseModel):
    package: str = Field(..., description="The name of the CRAN package to install")
    repo: str | None = Field(
        None,
        description="The repository where the package can be found. If not specified, the default CRAN repo is used",
    )


class LibraryMaven(BaseModel):
    coordinates: str = Field(
        ...,
        description="Gradle-style Maven coordinates. For example: `org.jsoup:jsoup:1.7.2`",
    )
    exclusions: list[str] | None = Field(
        None,
        description="List of dependencies to exclude. For example: `['slf4j:slf4j', '*:hadoop-client']`. See [Maven dependency exclusions](https://maven.apache.org/guides/introduction/introduction-to-optional-and-excludes-dependencies.html) for more information",
    )
    repo: str | None = Field(
        None,
        description="The repository where the package can be found. If not specified, the default CRAN repo is used",
    )


class LibraryPypi(BaseModel):
    package: str = Field(..., description="The name of the CRAN package to install")
    repo: str | None = Field(
        None,
        description="The repository where the package can be found. If not specified, the default CRAN repo is used",
    )


class LibraryBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_library`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    cluster_id: str = Field(
        ..., description="ID of the databricks_cluster to install the library on"
    )
    egg: str | None = Field(
        None,
        description="Path to the EGG library. Installing Python egg files is deprecated and is not supported in Databricks Runtime 14.0 and above. Use `whl` or `pypi` instead",
    )
    jar: str | None = Field(
        None,
        description="Path to the JAR library. Supported URIs include Workspace paths, Unity Catalog Volumes paths, and S3 URIs. For example: `/Workspace/path/to/library.jar`, `/Volumes/path/to/library.jar` or `s3://my-bucket/library.jar`. If S3 is used, make sure the cluster has read access to the library. You may need to launch the cluster with an IAM role to access the S3 URI",
    )
    requirements: str | None = Field(
        None,
        description="Path to the requirements.txt file. Only Workspace paths and Unity Catalog Volumes paths are supported. For example: `/Workspace/path/to/requirements.txt` or `/Volumes/path/to/requirements.txt`. Requires a cluster with DBR 15.0+",
    )
    whl: str | None = Field(
        None,
        description="Path to the wheel library. Supported URIs include Workspace paths, Unity Catalog Volumes paths, and S3 URIs. For example: `/Workspace/path/to/library.whl`, `/Volumes/path/to/library.whl` or `s3://my-bucket/library.whl`. If S3 is used, make sure the cluster has read access to the library. You may need to launch the cluster with an IAM role to access the S3 URI",
    )
    cran: list[LibraryCran] | None = PluralField(
        None,
        plural="crans",
        description="Configuration block for a CRAN library. The block consists of the following fields:",
    )
    maven: list[LibraryMaven] | None = PluralField(
        None,
        plural="mavens",
        description="Configuration block for a Maven library. The block consists of the following fields:",
    )
    pypi: list[LibraryPypi] | None = PluralField(
        None,
        plural="pypis",
        description="Configuration block for a PyPI library. The block consists of the following fields:",
    )

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_library"


__all__ = ["LibraryCran", "LibraryMaven", "LibraryPypi", "LibraryBase"]
