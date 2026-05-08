import pytest

from laktory._testing import _get_stack_resource_key
from laktory.models.pipeline.pipeline import Pipeline
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.workspacefile import WorkspaceFile


def test_catalog():
    assert _get_stack_resource_key(Catalog(name="test")) == "databricks_catalogs"


def test_job():
    assert _get_stack_resource_key(Job()) == "databricks_jobs"


def test_pipeline():
    assert _get_stack_resource_key(Pipeline(name="test")) == "pipelines"


def test_union_value_type():
    # databricks_workspacefiles: dict[str, WorkspaceFile | PythonPackage]
    assert _get_stack_resource_key(WorkspaceFile()) == "databricks_workspacefiles"


def test_unknown_type_raises():
    class _Unregistered:
        pass

    with pytest.raises(ValueError, match="No StackResources key found"):
        _get_stack_resource_key(_Unregistered())
