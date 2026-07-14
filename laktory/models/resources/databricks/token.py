from laktory.models.resources.databricks.token_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.token_base import TokenBase


class Token(TokenBase):
    """
    Databricks personal access token

    Examples
    --------
    ```py
    import io

    from laktory import models

    token_yaml = '''
    comment: laktory
    lifetime_seconds: 3600
    '''
    token = models.resources.databricks.Token.model_validate_yaml(
        io.StringIO(token_yaml)
    )
    ```

    References
    ----------

    * [Databricks Token](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/token)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
