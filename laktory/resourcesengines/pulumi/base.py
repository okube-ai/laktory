import pulumi


class PulumiResourcesEngine(pulumi.ComponentResource):

    @property
    def t(self) -> str:
        return f"laktory:{self.provider}:{type(self).__name__.replace('Pulumi', '')}"

    # @property
    # def default_name(self) -> str:
    #     raise NotImplementedError()

    @property
    def provider(self) -> str:
        raise NotImplementedError()
    #
    # def __init__(self, name=None, opts=None, *args, **kwargs):
    #     if name is None:
    #         name = self.default_name
    #     super().__init__(self.t, name, {}, opts)
    #
    #     kwargs["opts"] = kwargs.get("opts", pulumi.ResourceOptions())
    #     kwargs["opts"].parent = self
