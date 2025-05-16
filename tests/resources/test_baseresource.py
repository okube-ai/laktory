from laktory.models.resources.baseresource import BaseResource


class BaseResource2(BaseResource):
    pass


def test_resource_name():
    r1 = BaseResource()
    dump = r1.model_dump(exclude_unset=True)
    if r1.resource_name_ is not None:
        dump["resource_name_"] = r1.resource_name_
    r1_ = BaseResource2(**dump)

    assert r1.resource_name_ is None
    assert r1.default_resource_name == "base-resource"
    assert r1.resource_name == "base-resource"
    assert r1_.resource_name_ is None
    assert r1_.default_resource_name == "base-resource2"
    assert r1_.resource_name == "base-resource2"

    r2 = BaseResource(resource_name="r2")
    dump = r2.model_dump(exclude_unset=True)
    if r2.resource_name_ is not None:
        dump["resource_name_"] = r2.resource_name_
    r2_ = BaseResource2(**dump)
    assert r2.resource_name_ == "r2"
    assert r2.default_resource_name == "base-resource"
    assert r2.resource_name == "r2"
    assert r2_.resource_name_ == "r2"
    assert r2_.default_resource_name == "base-resource2"
    assert r2_.resource_name == "r2"


if __name__ == "__main__":
    test_resource_name()
