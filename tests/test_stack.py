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
    assert pl.nodes[0].ldp_template is None

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
    assert pl.nodes[0].ldp_template is None


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


def test_stack_to_terraform_cli_vars_override_stack_var(monkeypatch, stack):
    # CLI vars must override stack-level variables (workflow_name defaults to UNDEFINED).
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")

    # Verify baseline: stack-level workflow_name is UNDEFINED
    env_default = stack.get_env("dev")
    assert env_default.variables["workflow_name"] == "UNDEFINED"

    # After merging CLI vars, workflow_name on the env should be overridden
    env_with_cli = env_default.model_copy(
        update={"variables": {**env_default.variables, "workflow_name": "cli-wf"}}
    )
    env_injected = env_with_cli.inject_vars()
    # The variable dict itself should reflect the CLI value
    assert env_injected.variables["workflow_name"] == "cli-wf"


def test_stack_to_terraform_cli_vars_override_env_var(monkeypatch, stack):
    # CLI vars must win over env-level YAML variables (node_type_id is set per env).
    monkeypatch.setenv("DATABRICKS_HOST", "mock-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token")

    import json

    tf_stack_default = stack.to_terraform(env_name="dev")
    tf_stack_override = stack.to_terraform(
        env_name="dev", vars={"node_type_id": "Standard_DS5_v2"}
    )

    default_json = json.dumps(
        {k: v.model_dump() for k, v in tf_stack_default.resources.items()}
    )
    override_json = json.dumps(
        {k: v.model_dump() for k, v in tf_stack_override.resources.items()}
    )

    assert "Standard_DS3_v2" in default_json  # env-level default
    assert "Standard_DS5_v2" in override_json  # CLI override wins
    assert "Standard_DS3_v2" not in override_json


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
                },
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
                },
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
                },
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
    import re

    from laktory.models.stacks.terraformstack import _substitute_terraform_refs

    # Mirror the pattern+replacer construction that model_dump() uses.
    resource_map = {
        "my_cat": "databricks_catalog",
        "my.cat": "databricks_catalog",  # dot in name — must not match my_cat
        "foo": "databricks_schema",
        "foobar": "databricks_schema",  # prefix of "foo" — must not corrupt foo
    }
    tf_names = {
        "my_cat": "databricks_catalog.my_cat",
        "my.cat": "databricks_catalog.my_dot_cat",
        "foo": "databricks_schema.foo",
        "foobar": "databricks_schema.foobar",
    }
    # Sort longest-first, mirroring model_dump()'s alternation construction.
    names_alt = "|".join(
        re.escape(n) for n in sorted(resource_map, key=len, reverse=True)
    )
    pattern = re.compile(r"\$\{resources\.(" + names_alt + r")(?:\.([^}]*))?\}")

    def replacer(m):
        name, prop = m.group(1), m.group(2)
        base = tf_names[name]
        return f"${{{base}.{prop}}}" if prop is not None else base

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

    result = _substitute_terraform_refs(obj, pattern, replacer)

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


def test_substitute_terraform_refs_provider_alias():
    """
    When a stack has both 'databricks' and 'databricks.dev' providers,
    ${resources.databricks.dev} must resolve to bare 'databricks.dev' (provider
    ref), not '${databricks.dev}' (which would happen if the shorter name wins
    the regex alternation and '.dev' is misread as a property reference).
    """
    import re

    from laktory.models.stacks.terraformstack import _substitute_terraform_refs

    # Both the short base provider and an aliased workspace provider are present.
    provider_refs = {
        "databricks": "databricks",
        "databricks.dev": "databricks.dev",
    }
    names_alt = "|".join(
        re.escape(n) for n in sorted(provider_refs, key=len, reverse=True)
    )
    pattern = re.compile(r"\$\{resources\.(" + names_alt + r")(?:\.([^}]*))?\}")

    def replacer(m):
        name, prop = m.group(1), m.group(2)
        base = provider_refs[name]  # providers: base = bare name
        return f"${{{base}.{prop}}}" if prop is not None else base

    obj = {
        "base_provider": "${resources.databricks}",
        "aliased_provider": "${resources.databricks.dev}",
    }
    result = _substitute_terraform_refs(obj, pattern, replacer)

    assert result["base_provider"] == "databricks"
    # Must be bare 'databricks.dev', NOT '${databricks.dev}'
    assert result["aliased_provider"] == "databricks.dev"


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


_DB_PROVIDER = {
    "host": "https://adb-123.azuredatabricks.net",
    "token": "dapi123",
}


def _minimal_stack(backend, with_envs=True):
    envs = {"dev": {"variables": {}}, "prod": {"variables": {}}} if with_envs else {}
    return models.Stack(
        name="my-stack",
        environments=envs,
        resources={"providers": {"databricks": _DB_PROVIDER}},
        terraform={"backend": backend},
    )


def test_terraform_stack_workspace_state():
    from unittest.mock import MagicMock
    from unittest.mock import PropertyMock
    from unittest.mock import patch

    from laktory.models.resources.providers.databricksprovider import DatabricksProvider

    mock_wc = MagicMock()
    mock_wc.current_user.me.return_value.user_name = "user@test.com"

    with patch.object(
        DatabricksProvider, "workspace_client", new_callable=PropertyMock
    ) as mock_prop:
        mock_prop.return_value = mock_wc

        # true with PAT token → used directly (no token creation)
        ts = _minimal_stack({"databricks_workspace": True}).to_terraform(env_name="dev")
        backend = ts.terraform.backend["http"]
        assert backend["address"] == (
            "https://adb-123.azuredatabricks.net"
            "/api/2.0/workspace-files/Users/user@test.com"
            "/.laktory/my-stack/dev/state/terraform.tfstate"
        )
        assert backend["username"] == "token"
        assert backend["password"] == "dapi123"
        assert "lock_address" not in backend
        mock_wc.workspace.mkdirs.assert_called_once_with(
            path="/Users/user@test.com/.laktory/my-stack/dev/state"
        )
        mock_wc.tokens.create.assert_not_called()

        # SP auth (no token) → cached PAT returned by _get_cached_ws_token
        mock_wc.reset_mock()
        mock_wc.current_user.me.return_value.user_name = "user@test.com"
        sp_stack = models.Stack(
            name="my-stack",
            environments={"dev": {"variables": {}}},
            resources={
                "providers": {
                    "databricks": {
                        "host": "https://adb-123.azuredatabricks.net",
                        "azure_client_id": "client-id",
                        "azure_client_secret": "secret",
                        "azure_tenant_id": "tenant-id",
                    }
                }
            },
            terraform={"backend": {"databricks_workspace": True}},
        )
        import laktory.models.stacks.stack as _stack_module

        with patch.object(
            _stack_module, "_get_cached_ws_token", return_value="dapi-sp-cached"
        ):
            ts_sp = sp_stack.to_terraform(env_name="dev")
        assert ts_sp.terraform.backend["http"]["password"] == "dapi-sp-cached"

        # without env_name → state key is just stack name
        mock_wc.reset_mock()
        mock_wc.current_user.me.return_value.user_name = "user@test.com"
        ts_no_env = _minimal_stack(
            {"databricks_workspace": True}, with_envs=False
        ).to_terraform()
        assert ts_no_env.terraform.backend["http"]["address"].endswith(
            "/.laktory/my-stack/state/terraform.tfstate"
        )

    # other backend keys alongside databricks_workspace → those keys take precedence
    ts_explicit = _minimal_stack(
        {"databricks_workspace": True, "local": {}}
    ).to_terraform(env_name="dev")
    assert "local" in ts_explicit.terraform.backend
    assert "http" not in ts_explicit.terraform.backend
