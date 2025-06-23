from ._version import VERSION

__version__ = VERSION

# Import first
from ._settings import settings
from ._useragent import set_databricks_sdk_upstream

set_databricks_sdk_upstream()

import laktory._parsers
import laktory.models
import laktory.spark
import laktory.typing
import laktory.yaml

from ._logger import get_logger
from ._settings import Settings
from .version import show_version_info
