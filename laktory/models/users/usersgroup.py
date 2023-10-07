from laktory.models.base import BaseModel


class UsersGroup(BaseModel):
    display_name: str
    allow_cluster_create: bool = False
    workspace_access: bool = True
