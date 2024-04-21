import os

from laktory import models
from laktory import Dispatcher

dirpath = os.path.dirname(__file__)

with open(os.path.join(dirpath, "stack.yaml"), "r") as fp:
    stack = models.Stack.model_validate_yaml(fp)


tstack = stack.copy()
tstack.backend = "terraform"


def test_workspace_client():
    for _stack in [stack, tstack]:
        dispatcher = Dispatcher(stack=_stack)

        kwargs = dispatcher.workspace_arguments

        if "token" in kwargs:
            kwargs["token"] = kwargs["token"][:6]
        assert kwargs == {
            "host": "https://adb-2211091707396001.1.azuredatabricks.net/",
            "token": "dapic5",
        }
        assert dispatcher.wc is not None


def test_resources():
    dispatcher = Dispatcher(stack=stack)

    assert list(dispatcher.resources.keys()) == [
        "pl-custom-name",
        "job-stock-prices-ut-stack",
    ]
    job = dispatcher.resources["job-stock-prices-ut-stack"]
    pl = dispatcher.resources["pl-custom-name"]

    assert job.model_dump() == {"name": "job-stock-prices-ut-stack", "id": None}
    assert pl.model_dump() == {"name": "pl-stock-prices-ut-stack", "id": None}


if __name__ == "__main__":
    test_workspace_client()
    test_resources()
