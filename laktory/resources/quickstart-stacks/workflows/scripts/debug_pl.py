from databricks.connect import DatabricksSession

import laktory as lk

# TODO: import any custom modules that register Narwhals namespace(s)

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

stack_filepath = "../stack.yaml"

# Laktory root on DBFS, required to read checkpoints
lk.settings.laktory_root = "/laktory/"

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

if node_name:
    pl.nodes_dict[node_name].execute(write_sinks=False)
else:
    pl.execute(write_sinks=False)

# --------------------------------------------------------------------------- #
# Display Results                                                             #
# --------------------------------------------------------------------------- #

if node_name:
    df = pl.nodes_dict[node_name].output_df
else:
    df = pl.nodes[-1].output_df
df.laktory.display()
