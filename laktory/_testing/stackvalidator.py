import os
import shutil
import uuid

from laktory import models
from laktory._settings import settings
from laktory._testing import Paths

paths = Paths(__file__)


class StackValidator:
    def __init__(self, resources, providers=None):
        self.resources = resources
        self.providers = providers

    @property
    def stack(self):
        resources = {
            "providers": {
                "databricks": {
                    "host": "${vars.DATABRICKS_HOST}",
                    "token": "${vars.DATABRICKS_TOKEN}",
                }
            }
        }

        if self.providers:
            for k, v in self.providers.items():
                resources["providers"][k] = v

        for k, v in self.resources.items():
            resources[k] = {r.resource_name: r for r in v}

        return models.Stack(
            organization="okube",
            name="unit-testing",
            backend="pulumi",
            pulumi={
                "config": {
                    "databricks:host": "${vars.DATABRICKS_HOST}",
                    "databricks:token": "${vars.DATABRICKS_TOKEN}",
                }
            },
            resources=resources,
            environments={"dev": {}},
        )

    @property
    def pstack(self):
        return self.stack.to_pulumi("dev")

    @property
    def tstack(self):
        return self.stack.to_terraform("dev")

    def _pre_validate(self):
        c0 = settings.cli_raise_external_exceptions
        settings.cli_raise_external_exceptions = True
        cwd = os.getcwd()
        dirpath = paths.tmp / f"tmp_deploy_{uuid.uuid4()}"
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        os.chdir(dirpath)

        return cwd, dirpath, c0

    def _post_validate(self, cwd, dirpath, c0):
        os.chdir(cwd)
        shutil.rmtree(dirpath)
        settings.cli_raise_external_exceptions = c0

    def validate_pulumi(self):
        cwd, dirpath, c0 = self._pre_validate()
        self.pstack.preview(stack="okube/dev")
        self._post_validate(cwd, dirpath, c0)

    def validate_terraform(self):
        cwd, dirpath, c0 = self._pre_validate()
        self.tstack.init()
        self.tstack.plan()
        self._post_validate(cwd, dirpath, c0)
