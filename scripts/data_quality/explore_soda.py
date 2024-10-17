from soda.scan import Scan
import pandas as pd

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

# https://docs.soda.io/soda-cl/metrics-and-checks.html

data_source_name = "pandas"
dataset_name = "trips"
checks = f"""
checks for {dataset_name}:
    - row_count > 0
    - missing_count(trip_distance) = 0
    - max(trip_distance) between 1 and 3
"""

# --------------------------------------------------------------------------- #
# Read Data                                                                   #
# --------------------------------------------------------------------------- #

df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
).drop(
    [
        "pickup_datetime",
        "dropoff_datetime",
        "store_and_fwd_flag",
    ],
    axis=1,
)

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

# Initialize Soda Scan object
scan = Scan()
scan.set_data_source_name(data_source_name)
scan.add_pandas_dataframe(
    data_source_name=data_source_name, dataset_name=dataset_name, pandas_df=df
)

# Add the checks
scan.add_sodacl_yaml_str(checks)

# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

scan.set_verbose(True)
scan.execute()

# --------------------------------------------------------------------------- #
# Access Mechanics                                                            #
# --------------------------------------------------------------------------- #

# Queries
for _scan in scan._data_source_scans:
    data_source_name = _scan.data_source_scan_cfg.data_source_name
    for table in _scan.tables.values():
        table_name = table.table_name
        for p in table.partitions.values():
            partition_name = p.partition_name
            for q in p.collect_queries():
                print(f"{data_source_name}.{table_name}.{partition_name}:")
                print(f"  query: {''.join(q.sql.splitlines())}")
                print(f"  result: {q.row}")
print()

# Checks
for check in scan._checks:
    print(check.dict["name"])
    print("  type: ", check.dict["type"])
    print("  threshold", check.check_cfg.fail_threshold_cfg)
    print("  outcome: ", check.dict["outcome"])
    print("  metrics: ")
    for m in check.metrics.values():
        print(f"   {m.name}: {m.value}")
