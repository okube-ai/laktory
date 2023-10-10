from laktory.resourcesengines import PulumiResourcesEngine


class PulumiResources(PulumiResourcesEngine):
    @property
    def provider(self) -> str:
        return "databricks"


def test_pulumi_engine():
    # TODO: Setup pulumi and deploy resource
    resources = PulumiResources("t", "name")
    assert resources.t == "laktory:databricks:Resources"


if __name__ == "__main__":
    test_pulumi_engine()
