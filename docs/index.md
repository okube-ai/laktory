
A DataOps framework for building Databricks lakehouse.

![what_is_laktory](images/what_is_laktory.png)

## What is it?
Laktory makes it possible to express and bring to life your data vision, from raw data to enriched analytics-ready datasets and finely tuned AI models, while adhering to basic DevOps best practices such as source control, code reviews and CI/CD.
By taking a declarative approach, you use configuration files or python code to instruct the desired outcome instead of detailing how to get there.
Such level of abstraction is made possible by the provided model methods, custom spark functions and templates.

Laktory is also your best friend when it comes to prototyping and debugging data pipelines. 
Within your workspace, you will have access to a custom `dlt` package allowing you to execute and test your notebook outside of a Delta Live Table execution.

Finally, Laktory got your testing and monitoring covered. 
A data pipeline built with Laktory is shipped with custom hooks enabling fine-grained monitoring of performance and failures [under development]    

## Who is it for?
Laktory is not web app or a UI that you can use to visually build your data pipeline. 
We don't promise that you will be up and running within a few clicks.
You need basic programming or DevOps experience to get started. 
What we do promise on the other hand is that once you are past the initial setup, you will be able to efficiently scale, deploying hundreds of datasets and models without compromising data governance.   

