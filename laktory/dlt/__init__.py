try:
    from dlt import *
except (ModuleNotFoundError, FileNotFoundError):
    from laktory._databricks_dlt import *
