from pathlib import Path

import pytest

from laktory import models
from laktory._version import VERSION
from laktory.dispatcher import Dispatcher

root = Path(__file__).parent


@pytest.fixture
def stack():
    with open(root / "data/stack.yaml", "r") as fp:
        stack = models.Stack.model_validate_yaml(fp)

    stack.terraform.backend = {
        "azurerm": {
            "resource_group_name": "o3-rg-laktory-dev",
            "storage_account_name": "o3stglaktorydev",
            "container_name": "unit-testing",
            "key": "terraform/dev.terraform.tfstate",
        }
    }

    return stack


def test_workspace_client(monkeypatch, stack):
    tstack = stack.model_copy()
    tstack.backend = "terraform"

    monkeypatch.setenv("DATABRICKS_HOST", "my-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "my-token")

    for _stack in [stack, tstack]:
        dispatcher = Dispatcher(stack=_stack)
        assert f"okube-laktory/{VERSION}" in dispatcher.wc.config.user_agent

        kwargs = dispatcher._workspace_arguments
        assert kwargs == {
            "host": "my-host",
            "token": "my-token",
        }
        assert dispatcher.wc is not None


def test_resources(stack):
    dispatcher = Dispatcher(stack)

    assert list(dispatcher.resources.keys()) == [
        "${vars.workflow_name}",
        "job-stock-prices-ut-stack",
    ]
    job = dispatcher.resources["job-stock-prices-ut-stack"]
    dlt = dispatcher.resources["${vars.workflow_name}"]

    assert job.model_dump() == {"name": "job-stock-prices-ut-stack", "id": None}
    assert dlt.model_dump() == {"name": "${vars.workflow_name}", "id": None}
