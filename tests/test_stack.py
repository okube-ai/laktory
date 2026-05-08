from pathlib import Path

import pytest

import laktory as lk
from laktory import models
from laktory._settings import settings
from laktory._testing import skip_terraform_plan

root = Path(__file__).parent


@pytest.fixture
def stack():
    with open(root / "data/stack.yaml", "r") as fp:
        stack = models.Stack.model_validate_yaml(fp)

    return stack


def test_stack_model(stack):
    _ = stack.model_dump()


def test_stack_env_model(stack):
    # dev
    _stack = stack.get_env("dev")
    _stack = _stack.inject_vars()
    pl = _stack.resources.pipelines["pl-custom-name"]

    assert _stack.variables == {
        "business_unit": "laktory",
        "workflow_name": "UNDEFINED",
        "env": "dev",
        "is_dev": True,
        "node_type_id": "Standard_DS3_v2",
    }
    assert pl.orchestrator.development is None
    assert pl.nodes[0].dlt_template is None

    # prod
    _stack = stack.get_env("prod")
    pl = _stack.resources.pipelines["pl-custom-name"]
    assert _stack.variables == {
        "business_unit": "laktory",
        "workflow_name": "UNDEFINED",
        "env": "prod",
        "is_dev": False,
        "node_type_id": "Standard_DS4_v2",
    }
    assert not pl.orchestrator.development
    assert pl.nodes[0].dlt_template is None


def test_stack_resources_unique_name():
    with pytest.raises(ValueError):
        models.Stack(
            name="stack",
            organization="o3",
            resources=models.StackResources(
                databricks_schemas={"finance": {"name": "schema_finance"}},
                databricks_catalogs={
                    "finance": {
                        "name": "catalog_finance",
                    }
                },
            ),
        )


def test_build(monkeypatch, stack):
    monkeypatch.setattr(settings, "cli_raise_external_exceptions", True)
    stack.build(env_name="dev")


def test_terraform_stack(monkeypatch, stack):
    # Mock laktory version to account for dynamically changing value
    lk.__version__ = "<version>"

    # To prevent from exposing sensitive data, we overwrite some env vars
    monkeypatch.setenv("DATABRICKS_HOST", "my-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "my-token")
    monkeypatch.setattr(settings, "build_root", "/tmp/laktory/cache")

    data_default = stack.to_terraform().model_dump()
    print(data_default)
    assert data_default == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            }
        },
        "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "job_cluster": [
                        {
                            "job_cluster_key": "main",
                            "new_cluster": {
                                "node_type_id": "${vars.node_type_id}",
                                "spark_env_vars": {
                                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                    "LAKTORY_WORKSPACE_ENV": "${vars.env}",
                                },
                                "spark_version": "16.3.x-scala2.12",
                            },
                        }
                    ],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                        },
                        {
                            "task_key": "run-pipeline",
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                        },
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions_test": {
                    "access_control": [
                        {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                        {"permission_level": "CAN_RUN", "user_name": "user2"},
                    ],
                    "pipeline_id": "pipeline_123",
                },
                "permissions-warehouse-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-dlt-custom-name": {
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                    "provider": "databricks",
                },
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "configuration": {
                        "business_unit": "laktory",
                        "workflow_name": "pl-stock-prices-ut-stack",
                        "pipeline_name": "pl-stock-prices-ut-stack",
                        "requirements": '["laktory==<version>"]',
                        "config_filepath": "/Workspace/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    },
                    "name": "pl-stock-prices-ut-stack",
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "provider": "databricks",
                }
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "source": "/tmp/laktory/cache/pipelines/pl-stock-prices-ut-stack.json",
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
            },
        },
    }

    # Dev
    data = stack.to_terraform(env_name="dev").model_dump()
    print(data)
    assert data == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            }
        },
        "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "job_cluster": [
                        {
                            "job_cluster_key": "main",
                            "new_cluster": {
                                "node_type_id": "Standard_DS3_v2",
                                "spark_env_vars": {
                                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                    "LAKTORY_WORKSPACE_ENV": "dev",
                                },
                                "spark_version": "16.3.x-scala2.12",
                            },
                        }
                    ],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                        },
                        {
                            "task_key": "run-pipeline",
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                        },
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions_test": {
                    "access_control": [
                        {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                        {"permission_level": "CAN_RUN", "user_name": "user2"},
                    ],
                    "pipeline_id": "pipeline_123",
                },
                "permissions-warehouse-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-dlt-custom-name": {
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                    "provider": "databricks",
                },
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "configuration": {
                        "business_unit": "laktory",
                        "workflow_name": "pl-stock-prices-ut-stack",
                        "pipeline_name": "pl-stock-prices-ut-stack",
                        "requirements": '["laktory==<version>"]',
                        "config_filepath": "/Workspace/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    },
                    "name": "pl-stock-prices-ut-stack",
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "provider": "databricks",
                }
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "source": "/tmp/laktory/cache/pipelines/pl-stock-prices-ut-stack.json",
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
            },
        },
    }

    # Prod
    data = stack.to_terraform(env_name="prod").model_dump()
    print(data)
    assert data == {
        "terraform": {
            "required_providers": {
                "databricks": {"source": "databricks/databricks", "version": ">=1.49"}
            }
        },
        "provider": {"databricks": {"host": "my-host", "token": "my-token"}},
        "resource": {
            "databricks_job": {
                "job-stock-prices-ut-stack": {
                    "name": "job-stock-prices-ut-stack",
                    "job_cluster": [
                        {
                            "job_cluster_key": "main",
                            "new_cluster": {
                                "node_type_id": "Standard_DS4_v2",
                                "spark_env_vars": {
                                    "AZURE_TENANT_ID": "{{secrets/azure/tenant-id}}",
                                    "LAKTORY_WORKSPACE_ENV": "prod",
                                },
                                "spark_version": "16.3.x-scala2.12",
                            },
                        }
                    ],
                    "task": [
                        {
                            "job_cluster_key": "main",
                            "task_key": "ingest-metadata",
                            "library": [
                                {"pypi": {"package": "laktory==0.0.27"}},
                                {"pypi": {"package": "yfinance"}},
                            ],
                            "notebook_task": {
                                "notebook_path": "/jobs/ingest_stock_metadata.py"
                            },
                        },
                        {
                            "task_key": "run-pipeline",
                            "pipeline_task": {
                                "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}"
                            },
                        },
                    ],
                }
            },
            "databricks_permissions": {
                "permissions-notebook-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_READ"}
                    ],
                    "notebook_path": "${data.databricks_notebook.notebook-external.path}",
                    "depends_on": ["data.databricks_notebook.notebook-external"],
                },
                "permissions_test": {
                    "access_control": [
                        {"permission_level": "CAN_MANAGE", "user_name": "user1"},
                        {"permission_level": "CAN_RUN", "user_name": "user2"},
                    ],
                    "pipeline_id": "pipeline_123",
                },
                "permissions-warehouse-external": {
                    "access_control": [
                        {"group_name": "role-analysts", "permission_level": "CAN_USE"}
                    ],
                    "sql_endpoint_id": "${data.databricks_sql_warehouse.warehouse-external.id}",
                    "depends_on": ["data.databricks_sql_warehouse.warehouse-external"],
                },
                "permissions-dlt-custom-name": {
                    "access_control": [
                        {"group_name": "account users", "permission_level": "CAN_VIEW"},
                        {"group_name": "role-engineers", "permission_level": "CAN_RUN"},
                    ],
                    "pipeline_id": "${databricks_pipeline.dlt-custom-name.id}",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                },
                "permissions-workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "access_control": [
                        {"group_name": "users", "permission_level": "CAN_READ"}
                    ],
                    "workspace_file_path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": [
                        "databricks_workspace_file.workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json"
                    ],
                    "provider": "databricks",
                },
            },
            "databricks_pipeline": {
                "dlt-custom-name": {
                    "configuration": {
                        "business_unit": "laktory",
                        "workflow_name": "pl-stock-prices-ut-stack",
                        "pipeline_name": "pl-stock-prices-ut-stack",
                        "requirements": '["laktory==<version>"]',
                        "config_filepath": "/Workspace/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    },
                    "development": False,
                    "name": "pl-stock-prices-ut-stack",
                    "library": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}}
                    ],
                    "provider": "databricks",
                }
            },
            "databricks_workspace_file": {
                "workspace-file-laktory-pipelines-pl-stock-prices-ut-stack-json": {
                    "source": "/tmp/laktory/cache/pipelines/pl-stock-prices-ut-stack.json",
                    "path": "/.laktory/pipelines/pl-stock-prices-ut-stack.json",
                    "depends_on": ["databricks_pipeline.dlt-custom-name"],
                    "provider": "databricks",
                }
            },
        },
        "data": {
            "databricks_notebook": {
                "notebook-external": {"path": "/Workspace/external", "format": "SOURCE"}
            },
            "databricks_sql_warehouse": {
                "warehouse-external": {"id": "d2fa41bf94858c4b", "name": None}
            },
        },
    }


def test_substitute_terraform_refs():
    from laktory.models.stacks.terraformstack import _substitute_terraform_refs

    simple_map = {
        "${resources.my_cat}": "databricks_catalog.my_cat",
        "${resources.my.cat}": "databricks_catalog.my_dot_cat",
        "${resources.foo}": "databricks_schema.foo",
        "${resources.foobar}": "databricks_schema.foobar",
    }
    property_patterns = []
    for k0, k1 in [
        ("my_cat", "databricks_catalog.my_cat"),
        ("my.cat", "databricks_catalog.my_dot_cat"),
        ("foo", "databricks_schema.foo"),
        ("foobar", "databricks_schema.foobar"),
    ]:
        import re

        pattern = r"\$\{resources\." + re.escape(k0) + r"\.(.*?)\}"
        property_patterns.append((pattern, lambda m, k1=k1: f"${{{k1}.{m.group(1)}}}"))

    obj = {
        "simple": "${resources.my_cat}",
        "property": "${resources.my_cat.id}",
        "dot_name_simple": "${resources.my.cat}",
        "dot_name_property": "${resources.my.cat.name}",
        "prefix_simple": "${resources.foo}",
        "prefix_longer": "${resources.foobar}",
        "prefix_property": "${resources.foo.id}",
        "nested": {"inner": "${resources.my_cat.url}"},
        "list_val": ["${resources.foo}", "${resources.foobar.id}"],
        "unchanged": "no-substitution-needed",
    }

    result = _substitute_terraform_refs(obj, simple_map, property_patterns)

    assert result["simple"] == "databricks_catalog.my_cat"
    assert result["property"] == "${databricks_catalog.my_cat.id}"
    assert result["dot_name_simple"] == "databricks_catalog.my_dot_cat"
    assert result["dot_name_property"] == "${databricks_catalog.my_dot_cat.name}"
    # dot in resource name must NOT match the underscore variant
    assert result["dot_name_simple"] != "databricks_catalog.my_cat"
    # prefix collision: ${resources.foo} must not corrupt ${resources.foobar}
    assert result["prefix_simple"] == "databricks_schema.foo"
    assert result["prefix_longer"] == "databricks_schema.foobar"
    assert result["prefix_property"] == "${databricks_schema.foo.id}"
    assert result["nested"]["inner"] == "${databricks_catalog.my_cat.url}"
    assert result["list_val"] == [
        "databricks_schema.foo",
        "${databricks_schema.foobar.id}",
    ]
    assert result["unchanged"] == "no-substitution-needed"


def test_check_depends_on():
    from unittest.mock import patch

    from laktory.models.resources.baseresource import ResourceOptions

    class _R:
        def __init__(self, name, depends_on):
            self.resource_options = ResourceOptions(depends_on=depends_on)
            self.resource_options.name = name

    resources = {
        # valid ${resources.X} reference — should not warn
        "child": _R("child", ["${resources.parent}"]),
        # unknown ${resources.X} reference — should warn
        "orphan": _R("orphan", ["${resources.does-not-exist}"]),
        # non-${resources.X} string (Terraform data source) — should not warn
        "external": _R("external", ["data.databricks_notebook.foo"]),
    }
    providers = {"parent": _R("parent", [])}

    with patch("laktory.models.stacks.stack.logger") as mock_logger:
        models.Stack._check_depends_on(resources, providers)

    warned = [str(call) for call in mock_logger.warning.call_args_list]
    assert any("does-not-exist" in m for m in warned)
    assert not any("parent" in m for m in warned)
    assert not any("external" in m for m in warned)


def test_terraform_plan(monkeypatch, stack):
    monkeypatch.setattr(settings, "cli_raise_external_exceptions", True)
    skip_terraform_plan()

    tstack = stack.to_terraform(env_name="dev")
    tstack.init(flags=["-reconfigure"])
    tstack.plan()


def test_stack_settings(monkeypatch):
    custom_root = "/custom/path/"
    assert settings.runtime_root != custom_root

    monkeypatch.setattr(settings, "runtime_root", settings.runtime_root)
    _ = models.Stack(name="one_stack", settings={"runtime_root": custom_root})

    assert settings.runtime_root == custom_root


def test_get_env():
    stack = models.Stack(
        name="stack-${vars.v0}-${vars.v1}",
        variables={
            "v0": "value0",
            "v1": "value1",
        },
        environments={
            "dev": {
                "variables": {
                    "v1": "dev",
                }
            },
            "prd": {
                "variables": {
                    "v1": "prd",
                }
            },
        },
    )

    dev = stack.get_env("dev")
    assert dev.name == "stack-${vars.v0}-${vars.v1}"

    dev = stack.get_env("dev").inject_vars()
    assert dev.name == "stack-value0-dev"

    prd = stack.get_env("prd").inject_vars()
    assert prd.name == "stack-value0-prd"
