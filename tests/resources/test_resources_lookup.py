from laktory import models


def test_catalog():
    _ = models.resources.databricks.Catalog(lookup_existing={"name": "0"})


def test_external_location():
    _ = models.resources.databricks.ExternalLocation(lookup_existing={"name": "0"})


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


def test_mlflow_experiment():
    _ = models.resources.databricks.MLflowExperiment(lookup_existing={"name": "0"})


def test_mlflow_model():
    _ = models.resources.databricks.MLflowModel(lookup_existing={"name": "0"})


def test_pipeline():
    _ = models.resources.databricks.Pipeline(lookup_existing={"pipeline_id": "0"})


def test_schema():
    _ = models.resources.databricks.Schema(lookup_existing={"name": "dev.engineering"})


def test_storage_credential():
    _ = models.resources.databricks.StorageCredential(lookup_existing={"name": "0"})


def test_user():
    _ = models.resources.databricks.User(lookup_existing={"user_id": "0"})


def test_volume():
    _ = models.resources.databricks.Volume(
        lookup_existing={"name": "dev.sources.landing"}
    )


def test_warehouse():
    _ = models.resources.databricks.Warehouse(lookup_existing={"id": "0"})
