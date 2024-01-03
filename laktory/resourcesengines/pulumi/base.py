import pulumi


class PulumiResourcesEngine(pulumi.ComponentResource):

    @property
    def t(self) -> str:
        return f"laktory:{self.provider}:{type(self).__name__.replace('Pulumi', '')}"

    @property
    def provider(self) -> str:
        raise NotImplementedError()
