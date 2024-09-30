from laktory import models

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

with open("./pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

# --------------------------------------------------------------------------- #
# Execute Pipeline                                                            #
# --------------------------------------------------------------------------- #

# In the previous examples, we showed that each node can be executed
# individually by passing the output of an upstream node to a downstream
# one. Although convenient for debugging and prototyping, a pipeline is
# generally executed simply by calling its execute method:
pipeline.execute()


# --------------------------------------------------------------------------- #
# Review Data                                                                 #
# --------------------------------------------------------------------------- #

# Once the pipeline is executed, the output dataframe of each node is
# available.
for node in pipeline.sorted_nodes:
    print(f"{node.name} schema | {node.output_df.schema}")
