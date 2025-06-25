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
    """

    model_config = ConfigDict(extra="forbid")
    name: str = Field(None, description="Name of the resource attached to the runner")
    id: str = Field(None, description="ID of the deployed resource")
    dispatcher: Any = Field(
        default=None, exclude=True, description="Dispatcher managing the runs"
    )

    @property
    def wc(self) -> "WorkspaceClient":
        """Databricks Workspace Client"""
        return self.dispatcher.wc

    def get_id(self) -> str:
        raise NotImplementedError()

    def run(self, wait=True):
        raise NotImplementedError()
