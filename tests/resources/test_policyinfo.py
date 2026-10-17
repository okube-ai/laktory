from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import PolicyInfo

policy = PolicyInfo.model_validate(
    {
        "name": "mask_pii",
        "on_securable_type": "SCHEMA",
        "on_securable_fullname": "laktory.unit_tests",
        "policy_type": "POLICY_TYPE_COLUMN_MASK",
        "for_securable_type": "TABLE",
        "to_principals": ["account users"],
        "match_columns": [{"condition": "hasTag('pii')", "alias": "pii_col"}],
        "column_mask": {
            "function_name": "laktory.unit_tests.mask_string",
            "on_column": "pii_col",
        },
    }
)


def test_policyinfo():
    assert policy.column_mask.on_column == "pii_col"
    assert policy.resource_key == "laktory.unit_tests-mask_pii"
    assert policy.resource_name == "policy-info-laktory-unit_tests-mask_pii"
    assert policy.terraform_resource_type == "databricks_policy_info"
    assert (
        PolicyInfo(
            **{**policy.model_dump(), "on_securable_fullname": None}
        ).resource_key
        == "mask_pii"
    )


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(policy)
