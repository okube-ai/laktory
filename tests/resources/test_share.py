from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks.share import Share
from laktory.models.resources.databricks.share_base import ShareObject

share = Share(
    name="test-share",
    comment="Test share for Delta Sharing",
    owner="user@example.com",
)


def test_share_model():
    s = share
    assert s.name == "test-share"
    assert s.comment == "Test share for Delta Sharing"
    assert s.owner == "user@example.com"


def test_share_with_objects():
    obj = ShareObject(
        name="my_table",
        data_object_type="TABLE",
        shared_as="shared_table",
    )
    s = Share(
        name="test-share",
        comment="Test share with objects",
        objects=[obj],
    )
    assert len(s.object) == 1
    assert s.object[0].name == "my_table"
    assert s.object[0].data_object_type == "TABLE"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(share)
