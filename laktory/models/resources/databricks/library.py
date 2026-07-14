from laktory.models.resources.databricks.library_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.library_base import LibraryBase


class Library(LibraryBase):
    """
    Databricks library

    Examples
    --------
    ```py
    import io

    from laktory import models

    library_yaml = '''
    cluster_id: cluster-id
    pypi:
      - package: pandas
    '''
    library = models.resources.databricks.Library.model_validate_yaml(
        io.StringIO(library_yaml)
    )
    ```

    References
    ----------

    * [Databricks Library](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/library)
    """

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #
