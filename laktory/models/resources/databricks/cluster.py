from typing import Union

from pydantic import AliasChoices
from pydantic import Field

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.cluster_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.cluster_base import ClusterBase
from laktory.models.resources.databricks.permissions import Permissions


class ClusterLookup(ResourceLookup):
    cluster_id: str = Field(
        serialization_alias="id", description="The id of the cluster"
    )


class Cluster(ClusterBase):
    """
    Databricks cluster

    Examples
    --------
    ```py
    import io

    from laktory import models

    cluster_yaml = '''
    name: default
    spark_version: 16.3.x-scala2.12
    data_security_mode: USER_ISOLATION
    node_type_id: Standard_DS3_v2
    autoscale:
      min_workers: 1
      max_workers: 4
    autotermination_minutes: 30
    libraries:
    - pypi:
        package: laktory==0.0.23
    access_controls:
    - group_name: role-engineers
      permission_level: CAN_RESTART
    is_pinned: true
    '''
    cluster = models.resources.databricks.Cluster.model_validate_yaml(
        io.StringIO(cluster_yaml)
    )
    ```

    References
    ----------

    * [Databricks Cluster](https://docs.databricks.com/en/compute/configure.html#autoscaling-local-storage-1)

    """

    access_controls: list[AccessControl] = Field(
        [], description="List of access controls"
    )
    cluster_name: str | None = Field(
        None,
        description="Cluster name, which doesn't have to be unique.",
        validation_alias=AliasChoices("cluster_name", "name"),
    )
    lookup_existing: ClusterLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    cluster_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]
        return resources

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls"]
