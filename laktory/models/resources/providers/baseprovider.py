import re
from pydantic import Field
from laktory.models.basemodel import BaseModel


class BaseProvider(BaseModel):
    alias: str = Field(None, exclude=False)
    source: str = Field(None, exclude=True)
    version: str = Field(None, exclude=True)

    @property
    def resource_name(self) -> str:
        """
        dots (.) are allowed to support terraform aliases
        """

        name = self.default_resource_name
        if self.resource_name_:
            name = self.resource_name_

        alias_pattern = ""
        if self.alias is not None:
            alias_pattern = "."
        pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9-_" + alias_pattern + "]*$")

        if self.alias:
            if not name.endswith(f".{self.alias}"):
                raise ValueError(
                    f"Resource name `{name}` is invalid. A name for alias provider must end with .{self.alias}"
                )

        if not pattern.match(name):
            raise ValueError(
                f"Resource name `{name}` is invalid. A name must start with a letter or underscore and may contain only letters, digits, underscores, dots (if alias), and dashes."
            )

        return name

    @property
    def resource_name_without_alias(self):
        if self.alias is None:
            return self.resource_name
        return self.resource_name.replace(f".{self.alias}", "")

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
