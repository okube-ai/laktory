from laktory import models


def test_catalog():
    m = models.resources.databricks.Catalog(lookup_existing={"name": "0"})


def test_cluster():
    m = models.resources.databricks.Cluster(lookup_existing={"cluster_id": "0"})


def test_dbfsfile():
    m = models.resources.databricks.DbfsFile(lookup_existing={"path": "0"})


def test_directory():
    m = models.resources.databricks.Directory(lookup_existing={"path": "0"})


def test_group():
    m = models.resources.databricks.Group(lookup_existing={"display_name": "0"})


def test_job():
    m = models.resources.databricks.Job(lookup_existing={"id": "0"})


def test_metastore():
    m = models.resources.databricks.Metastore(lookup_existing={"metastore_id": "0"})


def test_notebook():
    m = models.resources.databricks.Notebook(lookup_existing={"path": "0"})


def test_serviceprincipal():
    m = models.resources.databricks.ServicePrincipal(
        lookup_existing={"application_id": "0"}
    )


def test_table():
    m = models.resources.databricks.Table(lookup_existing={"name": "0"})


def test_user():
    m = models.resources.databricks.User(lookup_existing={"user_id": "0"})


def test_warehouse():
    m = models.resources.databricks.Warehouse(lookup_existing={"id": "0"})


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
