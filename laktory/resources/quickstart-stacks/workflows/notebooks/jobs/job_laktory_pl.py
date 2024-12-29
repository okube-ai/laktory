# COMMAND ----------
dbutils.widgets.text("pipeline_name", "pl-stocks-job")
dbutils.widgets.text("node_name", "")
dbutils.widgets.text("full_refresh", "False")
dbutils.widgets.text("install_dependencies", "True")

# COMMAND ----------
install_dependencies = dbutils.widgets.get("install_dependencies").lower() == "true"
pl_name = dbutils.widgets.get("pipeline_name")

if install_dependencies:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    laktory_root = "/Workspace" + notebook_path.split("/jobs/")[0]
    filepath = f"{laktory_root}/pipelines/{pl_name}/requirements.txt"
    # MAGIC     %pip install -r $filepath
    # MAGIC     %restart_python

# COMMAND ----------
import importlib
import sys
import os
import pyspark.sql.functions as F

from laktory import models
from laktory import get_logger

logger = get_logger(__name__)
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

laktory_root = "/Workspace" + notebook_path.split("/jobs/")[0]
pl_name = dbutils.widgets.get("pipeline_name")
node_name = dbutils.widgets.get("node_name")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"
filepath = f"{laktory_root}/pipelines/{pl_name}/config.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())

# Import User Defined Functions
sys.path.append(f"{laktory_root}/pipelines/")
udfs = []
for udf in pl.udfs:
    if udf.module_path:
        sys.path.append(os.path.abspath(udf.module_path))
    module = importlib.import_module(udf.module_name)
    module = importlib.reload(module)
    globals()[udf.module_name] = module
    udfs += [getattr(module, udf.function_name)]

# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

if node_name:
    pl.nodes_dict[node_name].execute(spark=spark, udfs=udfs, full_refresh=full_refresh)
else:
    pl.execute(spark=spark, udfs=udfs, full_refresh=full_refresh)
