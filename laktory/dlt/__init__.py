try:
    from dlt import *
except (ModuleNotFoundError, FileNotFoundError):
    try:
        from laktory._databricks_dlt import *
    except RuntimeError:
        pass
