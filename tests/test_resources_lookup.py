from laktory import models


def test_lookup_inits():

    for M in [
        models.resources.databricks.Catalog,
        models.resources.databricks.Cluster,
        models.resources.databricks.DbfsFile,
        models.resources.databricks.Directory,
        models.resources.databricks.DLTPipeline,
        models.resources.databricks.Group,
        models.resources.databricks.Job,
        models.resources.databricks.Metastore,
        models.resources.databricks.Notebook,
        models.resources.databricks.Schema,
        models.resources.databricks.ServicePrincipal,
        models.resources.databricks.Table,
        models.resources.databricks.User,
        models.resources.databricks.Warehouse,
        models.resources.databricks.WorkspaceFile,
    ]:

        m = M(lookup_existing={"id": "0"})


if __name__ == "__main__":
    test_lookup_inits()
