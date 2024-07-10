from __future__ import annotations
from typing import TYPE_CHECKING

from typing import Any
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class DispatcherRunner(BaseModel):
    """
    Base runner for jobs, pipelines and other executables.

    Attributes
    ----------
    name:
        Name of the resource attached to the runner
    id:
        ID of the deployed resource
    dispatcher:
        Dispatcher managing the runs
    """

    model_config = ConfigDict(extra="forbid")
    name: str = None
    id: str = None
    dispatcher: Any = Field(default=None, exclude=True)

    @property
    def wc(self) -> WorkspaceClient:
        """Databricks Workspace Client"""
        return self.dispatcher.wc

    def get_id(self) -> str:
        raise NotImplementedError()

    def run(self, wait=True):
        raise NotImplementedError()
