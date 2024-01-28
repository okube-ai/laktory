import re
from typing import Any
from pydantic import AliasChoices
from pydantic import Field
from pydantic import BaseModel as _BaseModel
from laktory.models.basemodel import BaseModel


class ResourceOptions(BaseModel):
    """
    Resource options for deployment.

    Attributes
    ----------
    depends_on:
        Explicit list of resources dependencies.
        Supported by both pulumi and terraform.
    provider:
        Explicit declaration of resources provider.
        Supported by both pulumi and terraform.
    aliases:
        Specify aliases for this resource, so that renaming or refactoring
        doesn’t replace it.
        Pulumi only.
    delete_before_replace:
        Override the default create-before-delete behavior when replacing a
        resource.
        Pulumi only.
    ignore_changes:
        Declare that changes to certain properties should be ignored during a
        diff.
        Pulumi only.
    import_:
        Bring an existing cloud resource into Pulumi.
        Pulumi only.
    parent:
        Establish a parent/child relationship between resources.
        Pulumi only.
    replace_on_changes:
        Declare that changes to certain properties should be treated as forcing
        a replacement.
        Pulumi only.
    """

    # pulumi + terraform
    depends_on: list[str] = []
    provider: str = None

    # pulumi only
    aliases: list[str] = None
    delete_before_replace: bool = True
    ignore_changes: list[str] = None
    import_: str = None
    parent: str = None
    replace_on_changes: list[str] = None


class BaseResource(_BaseModel):
    """
    Parent class for all Laktory models deployable as one or multiple cloud
    core resources. This `BaseResource` class is derived from
    `pydantic.BaseModel`.

    Attributes
    ----------
    resource_name:
        Name of the resource in the context of infrastructure as code. If None,
        `default_resource_name` will be used instead.
    options:
        Resources options specifications
    """

    resource_name_: str = Field(
        None,
        validation_alias=AliasChoices("resource_name_", "resource_name"),
        exclude=True,
    )
    options: ResourceOptions = Field(ResourceOptions(), exclude=True)
    _core_resources: list[Any] = None

    @property
    def resource_name(self) -> str:
        if self.resource_name_:
            return self.resource_name_
        return self.default_resource_name

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        Resource type id used to build default resource name. Equivalent to
        class name converted to kebab case. Ex.: SecretScope -> secret-scope
        """
        _id = type(self).__name__
        _id = re.sub(
            r"(?<!^)(?=[A-Z])", "-", _id
        ).lower()  # Convert CamelCase to kebab-case
        return _id

    @property
    def resource_key(self) -> str:
        """
        Resource key used to build default resource name. Equivalent to
        name properties if available. Otherwise, empty string.
        """
        return getattr(self, "name", "")

    @property
    def default_resource_name(self) -> str:
        """
        Resource default name constructed as
        `{self.resource_type_id}.{self.resource_key}`
        """

        if self.resource_type_id not in self.resource_key:
            name = f"{self.resource_type_id}-{self.resource_key}"
        else:
            name = f"{self.resource_key}"

        if name.endswith("-"):
            name = name[:-1]

        return name

    @property
    def core_resources(self):
        """
        List of core resources to be deployed with this laktory model:
        - class instance (self)
        """
        if self._core_resources is None:
            self._core_resources = [self]
        return self._core_resources
