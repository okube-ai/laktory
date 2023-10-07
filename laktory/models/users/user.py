from pydantic import Field
from laktory.models.base import BaseModel


class User(BaseModel):
    user_name: str
    display_name: str = None
    workspace_access: bool = True
    groups: list[str] = []
    roles: list[str] = []
