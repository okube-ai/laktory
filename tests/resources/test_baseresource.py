from laktory.models.resources.baseresource import BaseResource


class MyResource(BaseResource):
    name: str = None


def test_resource_with_resource_name():
    r1 = MyResource(resource_name_="r1")

    assert r1.resource_name_ == "r1"
    assert r1.resource_key is None
    assert r1.resource_name == "r1"


def test_resource_without_name():
    r1 = MyResource()

    assert r1.resource_name_ is None
    assert r1.resource_key is None
    assert r1.resource_name == "my-resource"


def test_resource_with_name():
    r = MyResource(name="r1")

    assert r.resource_name_ is None
    assert r.name == "r1"
    assert r.resource_key == "r1"
    assert r.resource_name == "my-resource-r1"


def test_resource_safe_key():
    r = MyResource(name="-x-/@y-")
    assert r.resource_safe_key == "x-y"

    r = MyResource(name="x-${vars.env}-y--")
    assert r.resource_safe_key == "x-${vars.env}-y"

    r = MyResource(name="${resources.secret-scope-my-scope.id}-s2")
    assert r.resource_safe_key == "secret-scope-my-scope-s2"
