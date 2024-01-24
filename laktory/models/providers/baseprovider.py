from laktory.models.basemodel import BaseModel


class BaseProvider(BaseModel):

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_resource_type(self) -> str:
        return None
