import typer
from typing import Annotated
from laktory._logger import get_logger
from laktory.cli.app import app
from laktory._version import VERSION
from laktory.version import show_version_info

logger = get_logger(__name__)


@app.command()
def version():
    """
    Return installed laktory version and installed dependencies.

    Examples
    --------
    ```cmd
    laktory version
    ```
    """
    print(show_version_info())
