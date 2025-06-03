from laktory import models


def test_catalog():
    _ = models.resources.databricks.Catalog(lookup_existing={"name": "0"})


def test_cluster():
    _ = models.resources.databricks.Cluster(lookup_existing={"cluster_id": "0"})


def test_dbfsfile():
    _ = models.resources.databricks.DbfsFile(lookup_existing={"path": "0"})


def test_directory():
    _ = models.resources.databricks.Directory(lookup_existing={"path": "0"})


def test_group():
    _ = models.resources.databricks.Group(lookup_existing={"display_name": "0"})


def test_job():
    _ = models.resources.databricks.Job(lookup_existing={"id": "0"})


def test_metastore():
    _ = models.resources.databricks.Metastore(lookup_existing={"metastore_id": "0"})


def test_notebook():
    _ = models.resources.databricks.Notebook(lookup_existing={"path": "0"})


def test_serviceprincipal():
    _ = models.resources.databricks.ServicePrincipal(
        lookup_existing={"application_id": "0"}
    )


def test_table():
    _ = models.resources.databricks.Table(lookup_existing={"name": "0"})


def test_user():
    _ = models.resources.databricks.User(lookup_existing={"user_id": "0"})


def test_warehouse():
    _ = models.resources.databricks.Warehouse(lookup_existing={"id": "0"})


if __name__ == "__main__":
    test_catalog()
    test_cluster()
    test_dbfsfile()
    test_directory()
    test_group()
    test_job()
    test_metastore()
    test_notebook()
    test_serviceprincipal()
    test_table()
    test_user()
    test_warehouse()
