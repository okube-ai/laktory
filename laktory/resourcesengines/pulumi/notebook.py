from typing import Union
import pulumi
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.compute.notebook import Notebook

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiNotebook(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    def __init__(
            self,
            name=None,
            notebook: Notebook = None,
            opts=None,
    ):
        if name is None:
            name = f"notebook-{notebook.key}"
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.notebook = databricks.Notebook(
                f"notebook-{notebook.key}",
                path=notebook.path,
                source=notebook.source,
                language=notebook.language,
                opts=opts,
            )

        access_controls = []
        for permission in notebook.permissions:
            access_controls += [
                databricks.PermissionsAccessControlArgs(
                    permission_level=permission.permission_level,
                    group_name=permission.group_name,
                    service_principal_name=permission.service_principal_name,
                    user_name=permission.user_name,
                )
            ]

        if access_controls:
            self.permissions = databricks.Permissions(
                f"permissions-file-{notebook.key}",
                access_controls=access_controls,
                notebook_path=self.notebook.path,
                opts=opts,
            )
