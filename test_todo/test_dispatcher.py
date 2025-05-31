from __future__ import annotations

from laktory import models
from laktory._testing import MonkeyPatch
from laktory._testing import Paths
from laktory._version import VERSION
from laktory.dispatcher import Dispatcher

paths = Paths(__file__)

with open(paths.data / "stack.yaml", "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)


tstack = stack.model_copy()
tstack.backend = "terraform"


def test_workspace_client(monkeypatch):
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

    # Test executed as script
    if isinstance(monkeypatch, MonkeyPatch):
        monkeypatch.cleanup()


def test_resources():
    dispatcher = Dispatcher(stack=stack)

    assert list(dispatcher.resources.keys()) == [
        "${vars.workflow_name}",
        "job-stock-prices-ut-stack",
    ]
    job = dispatcher.resources["job-stock-prices-ut-stack"]
    dlt = dispatcher.resources["${vars.workflow_name}"]

    assert job.model_dump() == {"name": "job-stock-prices-ut-stack", "id": None}
    assert dlt.model_dump() == {"name": "${vars.workflow_name}", "id": None}


if __name__ == "__main__":
    test_workspace_client(MonkeyPatch())
    test_resources()
