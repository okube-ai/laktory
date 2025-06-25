import re
from typing import Any
from typing import Literal
from typing import get_args
from typing import get_origin

from pydantic import AliasChoices
from pydantic import BaseModel as _BaseModel
from pydantic import Field
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.basemodel import ModelMetaclass


class ResourceOptions(BaseModel):
    """
    Resource options for deployment.
    """

    # laktory
    is_enabled: bool = Field(
        True,
        description="""
        If `False`, resource is not passed to the IaC backend and is not deployed. May be used for deploying resources
        to specific stack environments only or for disabling resources when debugging.
        """,
    )

    # pulumi + terraform
    depends_on: list[str] = Field(
        [],
        description="Explicit list of resources dependencies. Supported by both pulumi and terraform.",
    )
    provider: str = Field(
        None,
        description="Explicit declaration of resources provider. Supported by both pulumi and terraform.",
    )
    ignore_changes: list[str] = Field(
        None,
        description="Declare that changes to certain properties should be ignored during a diff.",
    )

    # pulumi only
    aliases: list[str] = Field(
        None,
        description="Specify aliases for this resource, so that renaming or refactoring doesn’t replace it. Pulumi only.",
    )
    delete_before_replace: bool = Field(
        True,
        description="Override the default create-before-delete behavior when replacing a resource.Pulumi only.",
    )
    import_: str = Field(
        None, description="Bring an existing cloud resource into Pulumi. Pulumi only."
    )
    parent: str = None
    replace_on_changes: list[str] = Field(
        None,
        description="Declare that changes to certain properties should be treated as forcing a replacement. Pulumi only.",
    )

    # terraform only
    moved_from: str = Field(
        None,
        description="Establish a parent/child relationship between resources. Pulumi only.",
    )

    @property
    def pulumi_options(self) -> list[str]:
        return [
            "depends_on",
            "provider",
            "ignore_changes",
            "aliases",
            "delete_before_replace",
            "import_",
            "parent",
            "replace_on_changes",
        ]

    @property
    def terraform_options(self) -> list[str]:
        return [
            "depends_on",
            "provider",
            "ignore_changes",
            "moved_from",
        ]


class ResourceLookup(BaseModel):
    # model_config = ConfigDict(populate_by_name=True)
    def pulumi_dump(self, *args, **kwargs):
        kwargs["by_alias"] = kwargs.get("by_alias", True)
        kwargs["exclude_unset"] = kwargs.get("exclude_unset", True)
        return self.model_dump(*args, **kwargs)


class BaseResource(_BaseModel, metaclass=ModelMetaclass):
    """
    Parent class for all Laktory models deployable as one or multiple cloud
    core resources. This `BaseResource` class is derived from
    `pydantic.BaseModel`.
    """

    resource_name_: str = Field(
        None,
        validation_alias=AliasChoices("resource_name_", "resource_name"),
        exclude=True,
        description="""
        Name of the resource in the context of infrastructure as code. If None, `default_resource_name` will be used
        instead.
        """,
    )
    options: ResourceOptions = Field(
        ResourceOptions(), exclude=True, description="Resources options specifications"
    )
    lookup_existing: ResourceLookup = Field(
        None,
        exclude=True,
        frozen=False,
        description="Lookup resource instead of creating a new one.",
    )
    _core_resources: list[Any] = None

    @model_validator(mode="before")
    @classmethod
    def base_lookup(cls, data: Any) -> Any:
        if data is None:
            return data

        if not isinstance(data, dict):
            # TODO: Add support if data is a Base Resource instance
            return data

        lookup_existing = data.get("lookup_existing", None)
        if not lookup_existing:
            return data

        for fname, f in cls.model_fields.items():
            if f.is_required():
                # Since all field type hints include `var`, we need to isolate
                # intended type hint
                ann = get_args(f.annotation)[0]
                origin = get_origin(ann)
                args = get_args(ann)

                if ann == str:  # noqa: E721
                    data[fname] = ""
                elif origin == Literal:
                    data[fname] = args[0]
                elif str(f.annotation).startswith("list"):
                    data[fname] = []

        for k, v in cls.lookup_defaults().items():
            data[k] = v

        return data

    @model_validator(mode="after")
    def grants_validator(self) -> Any:
        grant = getattr(self, "grant", None)
        grants = getattr(self, "grants", None)
        if grants and grant:
            raise ValueError(
                "`grants` and `grant` are mutually exclusive. Only set one of them."
            )
        return self

    @classmethod
    def lookup_defaults(cls) -> dict:
        return {}

    @property
    def resource_name(self) -> str:
        pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9-_]*$")

        name = self.default_resource_name
        if self.resource_name_:
            name = self.resource_name_

        if not pattern.match(name):
            raise ValueError(
                f"Resource name `{name}` is invalid. A name must start with a letter or underscore and may contain only letters, digits, underscores, and dashes."
            )

        return name

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def get_grants_additional_resources(self, object, options=None):
        from laktory.models.resources.databricks.grant import Grant
        from laktory.models.resources.databricks.grants import Grants

        resources = []
        if options is None:
            options = {}

        if self.grants:
            resources += Grants(
                resource_name=f"grants-{self.resource_name}",
                grants=[
                    {"principal": g.principal, "privileges": g.privileges}
                    for g in self.grants
                ],
                options=options,
                **object,
            ).core_resources

        if self.grant:
            grant = self.grant
            if not isinstance(grant, list):
                grant = [grant]
            for g in grant:
                sanitized_principal = re.sub(
                    r"[_-]+",
                    "-",
                    re.sub(r"[^a-zA-Z0-9_-]", "-", re.sub(r"[ ()]", "_", g.principal)),
                ).strip("-")
                resources += Grant(
                    resource_name=f"grant-{self.resource_name}-{sanitized_principal}",
                    principal=g.principal,
                    privileges=g.privileges,
                    options=options,
                    **object,
                ).core_resources

        return resources

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def resource_type_id(self) -> str:
        """
        Resource type id used to build default resource name. Equivalent to
        class name converted to kebab case. e.g.: SecretScope -> secret-scope
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
        - `{self.resource_type_id}-{self.resource_key}`
        - removing ${resources....} tags
        - removing ${vars....} tags
        - Replacing special characters with - to avoid conflicts with resource properties
        """

        if self.resource_type_id not in self.resource_key:
            name = f"{self.resource_type_id}-{self.resource_key}"
        else:
            name = f"{self.resource_key}"

        if name.endswith("-"):
            name = name[:-1]

        # ${resources.x.property} -> x
        pattern = r"\$\{resources\.(.*?)\.(.*?)\}"
        name = re.sub(pattern, r"\1", name)

        # ${vars.x} -> x
        pattern = r"\$\{vars\.(.*?)\}"
        name = re.sub(pattern, r"\1", name)

        # Replace special characters
        chars = [".", "@", "{", "}", "[", "]", "$", "|"]
        for c in chars:
            name = name.replace(c, "-")

        return name

    @property
    def self_as_core_resources(self):
        """Flag set to `True` if self must be included in core resources"""
        return True

    @property
    def additional_core_resources(self):
        return []

    @property
    def core_resources(self):
        """
        List of core resources to be deployed with this laktory model:
        - class instance (self)
        """
        if self._core_resources is None:
            # Add self
            self._core_resources = []
            if self.self_as_core_resources and self.options.is_enabled:
                self._core_resources += [self]

            # Add additional
            def get_additional_resources(r):
                resources = []

                provider = r.options.provider
                k0 = f"${{resources.{r.resource_name}}}"

                for _r in r.additional_core_resources:
                    if not (r.options.is_enabled and _r.options.is_enabled):
                        continue

                    if provider:
                        if _r.options.provider is None:
                            _r.options.provider = provider

                    do = _r.options.depends_on
                    if r.self_as_core_resources and k0 not in do:
                        do += [k0]
                    _r.options.depends_on = do

                    if _r.self_as_core_resources:
                        resources += [_r]

                    for __r in get_additional_resources(_r):
                        resources += [__r]

                return resources

            self._core_resources += get_additional_resources(self)

        return self._core_resources
