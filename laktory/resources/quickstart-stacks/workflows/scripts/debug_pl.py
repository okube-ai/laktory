from databricks.connect import DatabricksSession

import laktory as lk

# TODO: import any custom modules that register Narwhals namespace(s)

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

stack_filepath = "../stack.yaml"

# Laktory root on DBFS, required to read checkpoints
lk.settings.runtime_root = "/laktory/"

# Get Remote Spark Session
# TODO: use your own profile
# https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config
spark = DatabricksSession.builder.profile("default").getOrCreate()

node_name = "brz_stock_prices"

# --------------------------------------------------------------------------- #
# Get Pipeline                                                                #
# --------------------------------------------------------------------------- #


with open(stack_filepath, "r") as fp:
    stack = lk.models.Stack.model_validate_yaml(fp)

pl = stack.get_env("dev").resources.pipelines["pl-stocks-job"]

# --------------------------------------------------------------------------- #
# Execute Pipeline                                                            #
# --------------------------------------------------------------------------- #

pl.execute(write_sinks=False, selects=node_name)

# --------------------------------------------------------------------------- #
# Display Results                                                             #
# --------------------------------------------------------------------------- #

df = pl.nodes_dict[node_name].output_df
df.laktory.display()
