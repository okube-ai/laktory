import warnings

from laktory.models.resources.baseresource import BaseResource
from laktory.models.resources.baseresource import ResourceOptions
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.table import Table


class MyResource(BaseResource):
    name: str = None


def test_resource_with_resource_name():
    r1 = MyResource.model_validate({"resource_options": {"name": "r1"}})

    assert r1.resource_options.name == "r1"
    assert r1.resource_key is None
    assert r1.resource_name == "r1"


def test_resource_without_name():
    r1 = MyResource()

    assert r1.resource_options.name is None
    assert r1.resource_key is None
    assert r1.resource_name == "my-resource"


def test_resource_with_name():
    r = MyResource(name="r1")

    assert r.resource_options.name is None
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


# ----------------------------------------------------------------------- #
# resource_options                                                         #
# ----------------------------------------------------------------------- #


def test_resource_options_default():
    r = MyResource()
    assert isinstance(r.resource_options, ResourceOptions)
    assert r.resource_options.name is None
    assert r.resource_options.is_enabled is True
    assert r.resource_options.provider is None
    assert r.resource_options.depends_on == []


def test_resource_options_direct():
    r = MyResource.model_validate(
        {
            "resource_options": {
                "name": "custom-name",
                "is_enabled": False,
                "provider": "my-provider",
            }
        }
    )
    assert r.resource_options.name == "custom-name"
    assert r.resource_name == "custom-name"
    assert r.resource_options.is_enabled is False
    assert r.resource_options.provider == "my-provider"


# ----------------------------------------------------------------------- #
# Backward compatibility                                                  #
# ----------------------------------------------------------------------- #


def test_compat_options_warns():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        r = MyResource.model_validate({"options": {"is_enabled": False}})

    assert r.resource_options.is_enabled is False
    assert any(
        issubclass(w.category, DeprecationWarning)
        and "resource_options" in str(w.message)
        for w in caught
    )


def test_compat_options_no_warn_on_native_options_field():
    # Catalog has a native `options` field — `options:` must NOT be redirected
    # to resource_options and must NOT trigger a DeprecationWarning.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        c = Catalog.model_validate({"name": "dev", "options": {"key": "value"}})

    assert c.options == {"key": "value"}
    assert c.resource_options.is_enabled is True  # default, untouched
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_compat_options_no_warn_table():
    # Table also has a native `options` field.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        t = Table.model_validate(
            {
                "name": "t1",
                "catalog_name": "cat",
                "schema_name": "sch",
                "table_type": "MANAGED",
                "options": {"delta.columnMapping.mode": "name"},
            }
        )

    assert t.options == {"delta.columnMapping.mode": "name"}
    assert t.resource_options.is_enabled is True
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_resource_options_and_native_options_coexist():
    # Catalog: both resource_options and the native options field can be set independently.
    c = Catalog.model_validate(
        {
            "name": "dev",
            "options": {"key": "value"},
            "resource_options": {"is_enabled": False},
        }
    )
    assert c.options == {"key": "value"}
    assert c.resource_options.is_enabled is False
