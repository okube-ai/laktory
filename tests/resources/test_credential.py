from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Credential
from laktory.models.resources.databricks import Grant
from laktory.models.resources.databricks import Grants

credential = Credential(
    name="laktory-service-credential",
    purpose="SERVICE",
    azure_managed_identity={
        "access_connector_id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac"
    },
    comment="laktory unit test",
)


def test_credential():
    assert credential.resource_key == "laktory-service-credential"
    assert credential.azure_managed_identity.access_connector_id.endswith("/ac")
    assert credential.terraform_resource_type == "databricks_credential"


def test_credential_grants():
    cred = Credential.model_validate(
        {
            **credential.model_dump(exclude_unset=True),
            "grants": [{"principal": "role-engineers", "privileges": ["ACCESS"]}],
        }
    )
    (grants,) = cred.additional_core_resources
    assert isinstance(grants, Grants)
    assert grants.credential == f"${{resources.{cred.resource_name}.id}}"
    assert grants.grant[0].principal == "role-engineers"
    assert grants.grant[0].privileges == ["ACCESS"]

    cred = Credential.model_validate(
        {
            **credential.model_dump(exclude_unset=True),
            "grant": {"principal": "role-engineers", "privileges": ["MANAGE"]},
        }
    )
    (grant,) = cred.additional_core_resources
    assert isinstance(grant, Grant)
    assert grant.credential == f"${{resources.{cred.resource_name}.id}}"
    assert "grants" not in cred.terraform_properties


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(credential)
