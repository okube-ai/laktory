from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import AccountNetworkPolicy
from laktory.models.resources.databricks import WorkspaceNetworkOption

policy = AccountNetworkPolicy.model_validate(
    {
        "network_policy_id": "laktory-restricted",
        "egress": {
            "network_access": {
                "restriction_mode": "RESTRICTED_ACCESS",
                "allowed_internet_destinations": [
                    {"destination": "pypi.org", "internet_destination_type": "DNS_NAME"}
                ],
            }
        },
        "workspace_ids": [1234567890123456, 6543210987654321],
    }
)


def test_accountnetworkpolicy():
    assert policy.resource_name == "account-network-policy-laktory-restricted"
    assert "workspace_ids" not in policy.terraform_properties

    options = policy.additional_core_resources
    assert all(isinstance(o, WorkspaceNetworkOption) for o in options)
    assert [o.resource_name for o in options] == [
        "workspace-network-option-1234567890123456",
        "workspace-network-option-6543210987654321",
    ]
    assert {o.network_policy_id for o in options} == {
        "${resources.account-network-policy-laktory-restricted.network_policy_id}"
    }


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(policy.model_copy(update={"workspace_ids": None}))
    plan_resource(
        WorkspaceNetworkOption(
            workspace_id=1234567890123456, network_policy_id="laktory-restricted"
        )
    )
