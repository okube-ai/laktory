from laktory.models.resources.databricks.instanceprofile_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.instanceprofile_base import InstanceProfileBase


class InstanceProfile(InstanceProfileBase):
    """
    Databricks Instance Profile

    Examples
    --------
    ```py
    import io

    from laktory import models

    profile_yaml = '''
    instance_profile_arn: arn:aws:iam::000000000000:instance-profile/my-role
    '''
    profile = models.resources.databricks.InstanceProfile.model_validate_yaml(
        io.StringIO(profile_yaml)
    )
    ```

    References
    ----------

    * [Databricks Instance Profile](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/instance_profile)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.instance_profile_arn.replace(":", "-")
