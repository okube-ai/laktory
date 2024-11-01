from laktory import models

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

with open("./pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

# --------------------------------------------------------------------------- #
#  Select Node                                                                #
# --------------------------------------------------------------------------- #

node = pipeline.nodes_dict["brz_stock_prices"]

# --------------------------------------------------------------------------- #
#  Low-level Node Execution                                                   #
# --------------------------------------------------------------------------- #

# Read source
source_df = node.source.read()
print(source_df)

# This node simply reads raw data from json files and output and consolidate
# them into a DataFrame which is written as a parquet file. As such, it
# does not have any transformer.
print(f"Transformer {node.transformer}")
print()

# Write to sink:
node.primary_sink.write(source_df)

# --------------------------------------------------------------------------- #
#  High-level Node Execution                                                  #
# --------------------------------------------------------------------------- #

# Generally, a node is executed using the execute method. It is the equivalent
# of calling the .source.read(), .transformer.execute() and sink.write()
# sequence
node.execute()
