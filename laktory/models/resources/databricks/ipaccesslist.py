from laktory.models.resources.databricks.ipaccesslist_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.ipaccesslist_base import IpAccessListBase


class IpAccessList(IpAccessListBase):
    """
    Databricks IP access list

    Examples
    --------
    ```py
    import io

    from laktory import models

    ip_access_list_yaml = '''
    label: office
    list_type: ALLOW
    ip_addresses:
      - 1.2.3.4/32
    '''
    ip_access_list = models.resources.databricks.IpAccessList.model_validate_yaml(
        io.StringIO(ip_access_list_yaml)
    )
    ```

    References
    ----------

    * [Databricks IP Access List](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/ip_access_list)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
