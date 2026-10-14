from laktory.models.resources.databricks.systemschema_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.systemschema_base import SystemSchemaBase


class SystemSchema(SystemSchemaBase):
    """
    Databricks System Schema

    Enables a Unity Catalog system schema.

    Examples
    --------
    ```py
    import io

    from laktory import models

    system_schema_yaml = '''
    schema: access
    '''
    system_schema = models.resources.databricks.SystemSchema.model_validate_yaml(
        io.StringIO(system_schema_yaml)
    )
    ```

    References
    ----------

    * [Databricks System Schema](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/system_schema)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def resource_key(self) -> str:
        return self.schema_
