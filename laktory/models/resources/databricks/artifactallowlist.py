from typing import Literal

from pydantic import Field

from laktory.models.resources.databricks.artifactallowlist_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.artifactallowlist_base import (
    ArtifactAllowlistBase,
)


class ArtifactAllowlist(ArtifactAllowlistBase):
    """
    Databricks Artifact Allowlist

    Libraries and init scripts allowed on compute with standard (shared)
    access mode. All the allowed artifacts of a given type must be declared in
    a single resource.

    Examples
    --------
    ```py
    import io

    from laktory import models

    allowlist_yaml = '''
    artifact_type: INIT_SCRIPT
    artifact_matchers:
    - artifact: /Volumes/dev/sources/init_scripts
      match_type: PREFIX_MATCH
    '''
    allowlist = models.resources.databricks.ArtifactAllowlist.model_validate_yaml(
        io.StringIO(allowlist_yaml)
    )
    ```

    References
    ----------

    * [Databricks Artifact Allowlist](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist)
    * [Terraform databricks_artifact_allowlist](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/artifact_allowlist)
    """

    # Narrow the type from base's plain str
    artifact_type: Literal["INIT_SCRIPT", "LIBRARY_JAR", "LIBRARY_MAVEN"] = Field(
        ...,
        description="The artifact type of the allowlist. Change forces creation of a new resource",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.artifact_type
