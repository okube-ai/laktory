# COMMAND ----------
dbutils.widgets.text("pipeline_name", "pl-stocks-job")
dbutils.widgets.text("node_name", "")
dbutils.widgets.text("full_refresh", "False")
dbutils.widgets.text("install_dependencies", "True")
dbutils.widgets.text("requirements", "")
dbutils.widgets.text("config_filepath", "")

# COMMAND ----------
install_dependencies = dbutils.widgets.get("install_dependencies").lower() == "true"

if install_dependencies:
    import json

    reqs = dbutils.widgets.get("requirements")
    reqs = " ".join(json.loads(reqs))
    # MAGIC %pip install $reqs
    # MAGIC %restart_python

# COMMAND ----------
import pyspark.sql.functions as F  # noqa: F401, E402

import laktory as lk  # noqa: E402

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

node_name = dbutils.widgets.get("node_name")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"
config_filepath = dbutils.widgets.get("config_filepath")
print(f"Reading pipeline at {config_filepath}")
with open(config_filepath, "r") as fp:
    pl = lk.models.Pipeline.model_validate_json(fp.read())


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

if node_name:
    pl.nodes_dict[node_name].execute(full_refresh=full_refresh)
else:
    pl.execute(full_refresh=full_refresh)
