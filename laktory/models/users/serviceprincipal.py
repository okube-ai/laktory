from pydantic import Field
from laktory.models.base import BaseModel


class ServicePrincipal(BaseModel):
    display_name: str
    application_id: str = None
    allow_cluster_create: bool = False
    groups: list[str] = []
    roles: list[str] = []

