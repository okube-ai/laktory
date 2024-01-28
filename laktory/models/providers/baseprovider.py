from pydantic import Field
from laktory.models.basemodel import BaseModel


class BaseProvider(BaseModel):
    source: str = Field(None, exclude=True)
    version: str = Field(None, exclude=True)

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_excludes(self):
        return ["source", "version"]

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return None
