import pytest

from laktory.models.resources.databricks.share import Share, ShareObject


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
    assert len(s.objects) == 1
    assert s.objects[0].name == "my_table"
    assert s.objects[0].data_object_type == "TABLE"


def test_share_pulumi_resource_type():
    s = share
    assert s.pulumi_resource_type == "databricks:Share"


def test_share_terraform_resource_type():
    s = share
    assert s.terraform_resource_type == "databricks_share"


def test_share_model_dump():
    s = share
    data = s.model_dump(exclude_unset=True)
    assert data["name"] == "test-share"
    assert data["comment"] == "Test share for Delta Sharing"
    assert data["owner"] == "user@example.com"