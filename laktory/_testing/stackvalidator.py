import os
import shutil
import uuid
from laktory import models


class StackValidator:
    def __init__(self, resources):
        self.resources = resources

    @property
    def stack(self):
        resources = {"providers": {
            "databricks": {
                "host": "${vars.DATABRICKS_HOST}",
                "token": "${vars.DATABRICKS_TOKEN}",
            }
        }}

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
            environments={
                "dev": {
                }
            }
        )

    @property
    def pstack(self):
        return self.stack.to_pulumi("dev")

    @property
    def tstack(self):
        return self.stack.to_terraform("dev")

    def validate(self):
        cwd = os.getcwd()
        dirpath = f"./tmp_deploy_{uuid.uuid4()}"
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        os.chdir(dirpath)
        self.validate_pulumi()
        self.validate_terraform()
        os.chdir(cwd)
        shutil.rmtree(dirpath)

    def validate_pulumi(self):
        self.pstack.preview(stack="okube/dev")

    def validate_terraform(self):
        self.tstack.init()
        self.tstack.plan()
