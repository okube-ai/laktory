from laktory.models.base import BaseModel


class Permission(BaseModel):
    principal: str
    privileges: str = None
