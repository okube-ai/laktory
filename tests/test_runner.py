import copy
import os

from laktory import models
from laktory import Runner

dirpath = os.path.dirname(__file__)

with open(os.path.join(dirpath, "stack.yaml"), "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)


tstack = stack.copy()
tstack.backend = "terraform"


def test_workspace_client():
    for _stack in [stack, tstack]:
        runner = Runner(stack=_stack)

        kwargs = runner.workspace_arguments

        if "token" in kwargs:
            kwargs["token"] = kwargs["token"][:6]
        assert kwargs == {
            "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
            "token": "dapic5",
        }
        assert runner.wc is not None


def test_resources():
    runner = Runner(stack=stack)

    assert runner.jobs == {
        "job-stock-prices-ut-stack": {"name": "job-stock-prices-ut-stack", "id": None}
    }
    assert runner.pipelines == {
        "pl-custom-name": {"name": "pl-stock-prices-ut-stack", "id": None}
    }


if __name__ == "__main__":
    test_workspace_client()
    test_resources()
