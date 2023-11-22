Guides, API reference and sample code are great, but sometimes you need to see the big picture before trying to setup a scalable solution.
It's with that intent that a comprehensive template on how to deploy a lakehouse as code using Laktory has been created [here](https://github.com/okube-ai/lakehouse-as-code).

In this template, 4 pulumi projects are used to:

* `{cloud_provider}_infra`: Deploy the required resources on your cloud provider
* `unity-catalog`: Configure users, groups, catalogs, schemas and manage grants
* `workspace-conf`: Configure secrets, clusters and warehouses
* `workspace`: Configure data workflows to build your lakehouse.
