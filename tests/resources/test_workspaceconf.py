from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import SqlGlobalConfig
from laktory.models.resources.databricks import WorkspaceConf

conf = WorkspaceConf(custom_config={"maxTokenLifetimeDays": "90"})
sql_config = SqlGlobalConfig(
    security_policy="DATA_ACCESS_CONTROL", sql_config_params={"ANSI_MODE": "true"}
)


def test_workspaceconf():
    assert conf.resource_name == "workspace-conf"
    assert conf.terraform_properties == {
        "custom_config": {"maxTokenLifetimeDays": "90"}
    }
    assert sql_config.resource_name == "sql-global-config"
    assert sql_config.terraform_resource_type == "databricks_sql_global_config"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(conf)
    plan_resource(sql_config)
