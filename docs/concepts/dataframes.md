Laktory currently supports [Spark](https://spark.apache.org) (default) and [Polars](https://pola.rs) libraries for 
reading, transforming and writing data. While both can be used for local and remote cluster computations, Polars is
generally better suited for smaller workloads while Spark is ideal for very large datasets. A hands-on comparison of 
both libraries (and Pandas) is available [here](https://www.linkedin.com/pulse/dataframes-battle-royale-pandas-vs-polars-spark-olivier-soucy-3dmve/).

<img src="../../images/spark.png" alt="pulumi" width="192"/> 

Apache Spark excels in distributed data processing, making it ideal for massive
datasets spread across a cluster of machines. Its lazy execution model allows
for complex query optimization and efficient resource management. Spark's
ability to handle large-scale data processing with fault tolerance and
scalability makes it the go-to choice for big data environments, though it
requires more setup, overhead and resources compared to Pandas and Polars.

It's the ideal option for a running Laktory pipelines on a cloud-based
spark-enabled cluster.

<img src="../../images/polars.png" alt="pulumi" width="192"/>

Polars is built for speed and efficiency, utilizing Rust's performance 
capabilities and supporting multi-threaded execution. With its lazy execution
model, Polars can optimize the entire workflow, making it highly efficient for 
complex operations on larger datasets. It outperforms Pandas significantly in 
terms of speed and memory usage, especially when handling larger-than-memory
data.

It's the ideal option for a running Laktory pipelines locally with a 
python-only installation. However, some features are not currently supported
when using Polars:

- Reading/writing to datawarehouse tables
- Streaming operations

