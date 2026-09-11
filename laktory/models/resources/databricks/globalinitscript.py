from laktory.models.resources.databricks.globalinitscript_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.globalinitscript_base import (
    GlobalInitScriptBase,
)


class GlobalInitScript(GlobalInitScriptBase):
    """
    Databricks Global Init Script

    Examples
    --------
    ```py
    import io

    from laktory import models

    script_yaml = '''
    name: my-init-script
    content_base64: ZWNobyAiaGVsbG8gd29ybGQi
    enabled: true
    '''
    script = models.resources.databricks.GlobalInitScript.model_validate_yaml(
        io.StringIO(script_yaml)
    )
    ```

    References
    ----------

    * [Databricks Global Init Script](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/global_init_script)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
