import great_expectations as gx
import pandas as pd

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

batch_name = "batch"
data_source_name = "pandas"
dataset_name = "trips"


# --------------------------------------------------------------------------- #
# Read Data                                                                   #
# --------------------------------------------------------------------------- #

df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

context = gx.get_context()
data_source = context.data_sources.add_pandas(data_source_name)
data_asset = data_source.add_dataframe_asset(name=dataset_name)

batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_name)
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# Create expectation
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count",
    min_value=1,
    max_value=3,
    result_format="COMPLETE",
)

# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

results = batch.validate(expectation)

# --------------------------------------------------------------------------- #
# Access Mechanics                                                            #
# --------------------------------------------------------------------------- #

r = results["result"]

print("Expectation:")
print(" cfg type", results["expectation_config"]["type"])
print(" cfg column", results["expectation_config"]["kwargs"]["column"])
print(" cfg min", results["expectation_config"]["kwargs"]["min_value"])
print(" cfg max", results["expectation_config"]["kwargs"]["max_value"])
print(" results:")
print("   elements count", r["element_count"])
print("   unexpected count", r["unexpected_count"])
print("   unexpected percent", r["unexpected_percent"])
print("   unexpected indexes", r["partial_unexpected_index_list"][0:3])
