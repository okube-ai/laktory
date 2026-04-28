from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks.recipient import Recipient

recipient = Recipient(
    name="test-recipient",
    comment="Test recipient for Delta Sharing",
    authentication_type="TOKEN",
    owner="user@example.com",
)


def test_recipient_model():
    r = recipient
    assert r.name == "test-recipient"
    assert r.comment == "Test recipient for Delta Sharing"
    assert r.authentication_type == "TOKEN"
    assert r.owner == "user@example.com"


def test_recipient_terraform_resource_type():
    r = recipient
    assert r.terraform_resource_type == "databricks_recipient"


def test_recipient_model_dump():
    r = recipient
    data = r.model_dump(exclude_unset=True)
    assert data["name"] == "test-recipient"
    assert data["comment"] == "Test recipient for Delta Sharing"
    assert data["authentication_type"] == "TOKEN"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(recipient)
