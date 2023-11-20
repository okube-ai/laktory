A comprehensive template on how to deploy a lakehouse as code using Laktory is maintained [here](https://github.com/okube-ai/lakehouse-as-code).

In this template, 4 pulumi projects are used to:

* `{cloud_provider}_infra`: Deploy the required resources on your cloud provider
* `unity-catalog`: Setup users, groups, catalogs, schemas and manage grants
* `workspace-conf`: Setup secrets, clusters and warehouses
* `workspace`: The data workflows to build your lakehouse.
