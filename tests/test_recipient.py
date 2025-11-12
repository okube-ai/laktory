import pytest

from laktory.models.resources.databricks.recipient import Recipient


def recipient():
    return Recipient(
        name="test-recipient",
        comment="Test recipient for Delta Sharing",
        authentication_type="TOKEN",
        owner="user@example.com",
    )


def test_recipient_model():
    r = recipient()
    assert r.name == "test-recipient"
    assert r.comment == "Test recipient for Delta Sharing"
    assert r.authentication_type == "TOKEN"
    assert r.owner == "user@example.com"


def test_recipient_pulumi_resource_type():
    r = recipient()
    assert r.pulumi_resource_type == "databricks:Recipient"


def test_recipient_terraform_resource_type():
    r = recipient()
    assert r.terraform_resource_type == "databricks_recipient"


def test_recipient_model_dump():
    r = recipient()
    data = r.model_dump(exclude_unset=True)
    assert data["name"] == "test-recipient"
    assert data["comment"] == "Test recipient for Delta Sharing"
    assert data["authentication_type"] == "TOKEN"