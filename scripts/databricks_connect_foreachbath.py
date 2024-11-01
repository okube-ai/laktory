import os
from databricks.connect import DatabricksSession

# Setup
spark = DatabricksSession.builder.clusterId("0501-011833-vcux5w7j").getOrCreate()

# Execute
df = spark.readStream.table("brz_stock_prices_job")


def update_metrics(batch_df, batch_id):
    size = batch_df.count()
    print(f"Batch size: {size}")


writer = df.writeStream.foreachBatch(update_metrics).start()

writer.awaitTermination()
