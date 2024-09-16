import os

from laktory import models
from laktory import Dispatcher
from laktory._version import VERSION
from laktory._testing import Paths

paths = Paths(__file__)

with open(os.path.join(paths.data, "stack.yaml"), "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)


tstack = stack.model_copy()
tstack.backend = "terraform"


def test_workspace_client():
    for _stack in [stack, tstack]:
        dispatcher = Dispatcher(stack=_stack)
        assert f"okube-laktory/{VERSION}" in dispatcher.wc.config.user_agent

        kwargs = dispatcher._workspace_arguments
        if "token" in kwargs:
            kwargs["token"] = kwargs["token"][:6]
        assert kwargs == {
            "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
            "token": "dapic5",
        }
        assert dispatcher.wc is not None


def test_resources():
    dispatcher = Dispatcher(stack=stack)

    print(dispatcher.resources.keys())

    assert list(dispatcher.resources.keys()) == [
        "pl-stock-prices-ut-stack",
        "job-stock-prices-ut-stack",
    ]
    job = dispatcher.resources["job-stock-prices-ut-stack"]
    dlt = dispatcher.resources["pl-stock-prices-ut-stack"]

    assert job.model_dump() == {"name": "job-stock-prices-ut-stack", "id": None}
    assert dlt.model_dump() == {"name": "pl-stock-prices-ut-stack", "id": None}


if __name__ == "__main__":
    test_workspace_client()
    # test_resources()
