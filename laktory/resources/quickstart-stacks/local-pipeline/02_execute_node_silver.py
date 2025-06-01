from laktory import models

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

with open("./pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

# --------------------------------------------------------------------------- #
# Select Node                                                                 #
# --------------------------------------------------------------------------- #

node_brz = pipeline.nodes_dict["brz_stock_prices"]
node_slv = pipeline.nodes_dict["slv_stock_prices"]

# --------------------------------------------------------------------------- #
# Execute Bronze                                                              #
# --------------------------------------------------------------------------- #

# Execute bronze node without writing the sink. This operation populates
# the output_dataframe property.
node_brz.execute(write_sinks=False)
print(f"Output Schema: {node_brz.output_df.schema}")
print("------------")

# --------------------------------------------------------------------------- #
# Execute Silver (low-level)                                                  #
# --------------------------------------------------------------------------- #

# The silver node reads from the bronze node as we see from its source
print(f"Silver node source type: {type(node_slv.source)}")
print(f"Silver node source node name: {node_slv.source.node_name}")

# For an execution outside of a pipeline, we can mock its read method by
# directly assigning it a dataframe.
node_slv.source._df = node_brz.output_df
df = node_slv.source.read()

# Next, we apply the transformer
df = node_slv.transformer.execute(df)

# From the logs, we can see that silver stock prices transformer was composed
# of two nodes. The first one is a SQL select statement, while the second
# is a call to Polars .unique() method. Laktory allows to conveniently mix
# and max SQL and DataFrame API calls.

# Printing the transformer output
print(f"Output Schema: {df.schema}")
print("------------")


# --------------------------------------------------------------------------- #
#  High-level Node Execution                                                  #
# --------------------------------------------------------------------------- #

# Again, the node would normally be executed using the execute method.
node_slv.execute()
