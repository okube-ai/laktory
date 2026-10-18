import importlib.util
import json
from pathlib import Path

import pytest

from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources import databricks as dbks
from laktory.models.resources.databricks._settingv2mixin import (
    TYPED_VALUES_WITH_OPTIONAL_EFFECTIVE,
)

BUILD_DIR = Path(__file__).parents[2] / "scripts" / "build_resources"

settings = [
    (
        dbks.WorkspaceSettingV2,
        {"name": "workspace_label", "workspace_label": {"label": "dev"}},
        "workspace-setting-v2-workspace_label",
        "databricks_workspace_setting_v2",
    ),
    (
        dbks.AccountSettingV2,
        {"name": "llm_proxy_partner_powered", "boolean_val": {"value": False}},
        "account-setting-v2-llm_proxy_partner_powered",
        "databricks_account_setting_v2",
    ),
    (
        dbks.DefaultNamespaceSetting,
        {"namespace": {"value": "dev"}},
        "default-namespace-setting",
        "databricks_default_namespace_setting",
    ),
    (
        dbks.RestrictWorkspaceAdminsSetting,
        {"restrict_workspace_admins": {"status": "RESTRICT_TOKENS_AND_JOB_RUN_AS"}},
        "restrict-workspace-admins-setting",
        "databricks_restrict_workspace_admins_setting",
    ),
    (
        dbks.ComplianceSecurityProfileWorkspaceSetting,
        {
            "compliance_security_profile_workspace": {
                "is_enabled": True,
                "compliance_standards": ["HIPAA"],
            }
        },
        "compliance-security-profile-workspace-setting",
        "databricks_compliance_security_profile_workspace_setting",
    ),
    (
        dbks.EnhancedSecurityMonitoringWorkspaceSetting,
        {"enhanced_security_monitoring_workspace": {"is_enabled": True}},
        "enhanced-security-monitoring-workspace-setting",
        "databricks_enhanced_security_monitoring_workspace_setting",
    ),
    (
        dbks.DisableLegacyFeaturesSetting,
        {"disable_legacy_features": {"value": True}},
        "disable-legacy-features-setting",
        "databricks_disable_legacy_features_setting",
    ),
]


@pytest.mark.parametrize("cls,data,name,tf_type", settings)
def test_setting(cls, data, name, tf_type):
    setting = cls.model_validate(data)
    assert setting.resource_name == name
    assert setting.terraform_resource_type == tf_type


def test_setting_v2():
    with pytest.raises(ValueError):
        dbks.WorkspaceSettingV2(boolean_val={"value": True})
    # Effective values are outputs, not settable fields
    assert "effective_workspace_label" not in dbks.WorkspaceSettingV2.model_fields
    assert "effective_boolean_val" not in dbks.AccountSettingV2.model_fields


def test_setting_v2_effective_value():
    # Typed values are also sent as their effective value: the provider marks
    # it optional and fails the apply when Databricks returns it
    label = {"label": "dev", "color": "blue"}
    setting = dbks.WorkspaceSettingV2(name="workspace_label", workspace_label=label)
    assert setting.terraform_properties == {
        "name": "workspace_label",
        "workspace_label": label,
        "effective_workspace_label": label,
    }
    setting = dbks.AccountSettingV2(name="abac_on_views", boolean_val={"value": True})
    assert "effective_boolean_val" not in setting.terraform_properties


def test_setting_v2_effective_attributes_match_provider_schema():
    # The workaround and the generator skip list are written for the provider
    # schema: re-check them on each provider round trip
    # Load the generator constants module alone: no sys.path or sys.modules change
    spec = importlib.util.spec_from_file_location(
        "laktory_build_resources_config", BUILD_DIR / "_config.py"
    )
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)

    expected = {f"effective_{k}" for k in TYPED_VALUES_WITH_OPTIONAL_EFFECTIVE}
    assert config.SETTING_V2_EFFECTIVE_ATTRS == expected

    schema = json.loads((BUILD_DIR / "databricks_schema.json").read_text())
    resources = next(iter(schema["provider_schemas"].values()))["resource_schemas"]
    for name in ["databricks_workspace_setting_v2", "databricks_account_setting_v2"]:
        attributes = resources[name]["block"]["attributes"]
        optional_effective = {
            k
            for k, v in attributes.items()
            if k.startswith("effective_") and v.get("optional")
        }
        assert optional_effective == expected


def test_workspace_network_option_requires_workspace():
    with pytest.raises(ValueError):
        dbks.WorkspaceNetworkOption(network_policy_id="laktory-restricted")


@pytest.mark.parametrize("cls,data,name,tf_type", settings)
def test_terraform_plan(cls, data, name, tf_type):
    skip_terraform_plan()
    plan_resource(cls.model_validate(data))
