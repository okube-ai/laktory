from laktory import models

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

with open("./pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

# --------------------------------------------------------------------------- #
# List Nodes Pipeline                                                         #
# --------------------------------------------------------------------------- #

print("Pipeline Nodes:")
for node_name in pipeline.sorted_node_names:
    print(f"   {node_name}")
print()

# --------------------------------------------------------------------------- #
# Visualize Pipeline DAG                                                      #
# --------------------------------------------------------------------------- #

fig = pipeline.dag_figure()
fig.write_html("./dag.html", auto_open=False)
