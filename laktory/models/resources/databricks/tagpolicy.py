from laktory.models.resources.databricks.tagpolicy_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.tagpolicy_base import TagPolicyBase


class TagPolicy(TagPolicyBase):
    """
    Databricks Tag Policy

    Defines a governed tag: a tag key, with an optional list of allowed
    values, that can be assigned to Unity Catalog and workspace entities and
    used in attribute-based access control (ABAC) policies.

    Examples
    --------
    ```py
    import io

    from laktory import models

    tag_policy_yaml = '''
    tag_key: data_classification
    description: Sensitivity level of the data
    values:
    - name: public
    - name: confidential
    - name: pii
    '''
    tag_policy = models.resources.databricks.TagPolicy.model_validate_yaml(
        io.StringIO(tag_policy_yaml)
    )
    ```

    References
    ----------

    * [Databricks Governed Tags](https://docs.databricks.com/aws/en/admin/governed-tags/)
    * [Terraform databricks_tag_policy](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/tag_policy)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.tag_key
