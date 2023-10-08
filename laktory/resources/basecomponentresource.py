import pulumi


class BaseComponentResource(pulumi.ComponentResource):

    @property
    def t(self) -> str:
        return f"laktory:{self.provider}:{type(self.model).__name__}"

    @property
    def default_name(self) -> str:
        raise NotImplementedError()

    @property
    def provider(self) -> str:
        raise NotImplementedError()

    def __init__(self, model=None, name=None, opts=None, **kwargs):
        self.model = model
        if name is None:
            name = self.default_name
        super().__init__(self.t, name, {}, opts)

        kwargs["opts"] = kwargs.get("opts", pulumi.ResourceOptions())
        kwargs["opts"].parent = self
        self.set_resources(**kwargs)

    def set_resources(self, **kwargs):
        return NotImplementedError()
