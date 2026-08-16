from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Library

library = Library(
    cluster_id="cluster-id",
    pypi=[{"package": "pandas"}],
)


def test_library():
    assert library.cluster_id == "cluster-id"
    assert library.pypi[0].package == "pandas"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(library)
