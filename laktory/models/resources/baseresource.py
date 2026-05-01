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


def to_safe_name(name: str) -> str:
    """
    Resource default name constructed as
    - `{self.resource_type_id}-{self.resource_key}`
    - removing ${resources....} tags
    - preserving ${vars....} tags
    - Replacing special characters with - to avoid conflicts with resource properties
    """
    if name.endswith("-"):
        name = name[:-1]

    # ${resources.x.property} -> x
    pattern = r"\$\{resources\.(.*?)\.(.*?)\}"
    name = re.sub(pattern, r"\1", name)

    # Preserve ${vars...} tags
    pattern_vars = r"\$\{vars\.[^}]+\}"
    preserved_vars = re.findall(pattern_vars, name)

    # Temporarily replace preserved vars with placeholders
    for i, var in enumerate(preserved_vars):
        placeholder = f"__VAR_PLACEHOLDER_{i}__"
        name = name.replace(var, placeholder)

    # Replace special characters
    chars = [".", "@", "{", "}", "[", "]", "$", "|", "\\", "/", " "]
    for c in chars:
        name = name.replace(c, "-")

    # Restore preserved ${vars...} tags
    for i, var in enumerate(preserved_vars):
        placeholder = f"__VAR_PLACEHOLDER_{i}__"
        name = name.replace(placeholder, var)

    # Remove duplicate dashes
    while "--" in name:
        name = name.replace("--", "-")

    # Remove trailing dashes
    if name.startswith("-"):
        name = name[1:]

    # Remove leading dashes
    if name.endswith("-"):
        name = name[:-1]

    return name


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

    depends_on: list[str] = Field(
        [],
        description="Explicit list of resource dependencies.",
    )
    provider: str = Field(
        None,
        description="Explicit declaration of resource provider.",
    )
    ignore_changes: list[str] = Field(
        None,
        description="Declare that changes to certain properties should be ignored during a diff.",
    )
    import_: str = Field(
        None, description="Bring an existing cloud resource into Laktory management."
    )
    moved_from: str = Field(
        None,
        description="Declare that a resource was moved from another address.",
    )

    @property
    def terraform_options(self) -> list[str]:
        return [
            "depends_on",
            "provider",
            "ignore_changes",
            "moved_from",
            "import_",
        ]


class ResourceLookup(BaseModel):
    pass


class BaseResource(_BaseModel, metaclass=ModelMetaclass):
    """
    Parent class for all Laktory models deployable as one or multiple cloud
    core resources. This `BaseResource` class is derived from
    `pydantic.BaseModel`.
    """

    __doc_hide_base__ = True  # hide this class's fields and methods from child docs

    resource_name_: str = Field(
        None,
        validation_alias=AliasChoices("resource_name_", "resource_name"),
        exclude=True,
        description="""
        Name of the resource in the context of infrastructure as code. If None, `default_resource_name` will be used
        instead.
        """,
    )
    deployment_options: ResourceOptions = Field(
        ResourceOptions(),
        exclude=True,
        description="Resources deployment options specifications",
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
    def options_compat(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "options" not in data:
            return data
        # Only redirect "options" → "deployment_options" for classes that don't
        # have a native Terraform "options" field (detected via AliasChoices on options_)
        for fname, field_info in cls.model_fields.items():
            if fname == "deployment_options":
                continue
            alias = field_info.validation_alias
            if alias is None:
                continue
            choices = alias.choices if isinstance(alias, AliasChoices) else [alias]
            if any(c == "options" for c in choices if isinstance(c, str)):
                return data  # native options field present — leave as-is
        data["deployment_options"] = data.pop("options")
        return data

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
        if self.resource_name_:
            name = self.resource_name_
        else:
            name = self.resource_safe_key
            if name == "":
                name = self.resource_type_id
            elif self.resource_type_id not in self.resource_safe_key:
                name = f"{self.resource_type_id}-{name}"
            else:
                pass

        pattern = re.compile(
            r"^[a-zA-Z][a-zA-Z0-9-_]*(\$\{vars\.[a-zA-Z0-9_]+\}[a-zA-Z0-9-_]*)*$"
        )

        if not pattern.match(name):
            raise ValueError(
                f"Resource name `{name}` is invalid. A name must start with a letter or underscore, "
                "may contain only letters, digits, underscores, dashes or a variable (`${vars.some_var}`)."
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
                deployment_options=options,
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
                    deployment_options=options,
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
    def resource_safe_key(self) -> str:
        if self.resource_key is None:
            return ""
        return to_safe_name(self.resource_key)

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
            if self.self_as_core_resources and self.deployment_options.is_enabled:
                self._core_resources += [self]

            # Add additional
            def get_additional_resources(r):
                resources = []

                provider = r.deployment_options.provider
                k0 = f"${{resources.{r.resource_name}}}"

                for _r in r.additional_core_resources:
                    if not (
                        r.deployment_options.is_enabled
                        and _r.deployment_options.is_enabled
                    ):
                        continue

                    _options_updated = False
                    if provider:
                        if _r.deployment_options.provider is None:
                            _options_updated = True
                            _r.deployment_options.provider = provider

                    do = _r.deployment_options.depends_on
                    l0 = len(do)
                    if r.self_as_core_resources and k0 not in do:
                        do += [k0]
                    _r.deployment_options.depends_on = do
                    l1 = len(do)
                    if l1 != l0:
                        _options_updated = True

                    # This is to ensure deployment_options is flagged as set and part of
                    # model_fields_set when injecting variables.
                    if _options_updated:
                        _r.deployment_options = _r.deployment_options

                    if _r.self_as_core_resources:
                        resources += [_r]

                    for __r in get_additional_resources(_r):
                        resources += [__r]

                return resources

            self._core_resources += get_additional_resources(self)

        return self._core_resources
