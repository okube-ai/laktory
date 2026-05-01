import warnings

from laktory.models.resources.baseresource import BaseResource
from laktory.models.resources.baseresource import ResourceOptions
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.table import Table


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


# ----------------------------------------------------------------------- #
# deployment_options                                                       #
# ----------------------------------------------------------------------- #


def test_deployment_options_default():
    r = MyResource()
    assert isinstance(r.deployment_options, ResourceOptions)
    assert r.deployment_options.is_enabled is True
    assert r.deployment_options.provider is None
    assert r.deployment_options.depends_on == []


def test_deployment_options_direct():
    r = MyResource.model_validate(
        {"deployment_options": {"is_enabled": False, "provider": "my-provider"}}
    )
    assert r.deployment_options.is_enabled is False
    assert r.deployment_options.provider == "my-provider"


def test_deployment_options_compat_warns():
    # `options` should still work but emit a DeprecationWarning
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        r = MyResource.model_validate({"options": {"is_enabled": False}})

    assert r.deployment_options.is_enabled is False
    assert any(
        issubclass(w.category, DeprecationWarning)
        and "deployment_options" in str(w.message)
        for w in caught
    )


def test_deployment_options_compat_no_warn_on_native_options_field():
    # Catalog has a native `options` field — `options:` must NOT be redirected
    # to deployment_options and must NOT trigger a DeprecationWarning.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        c = Catalog.model_validate({"name": "dev", "options": {"key": "value"}})

    assert c.options == {"key": "value"}
    assert c.deployment_options.is_enabled is True  # default, untouched
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_deployment_options_compat_no_warn_table():
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
    assert t.deployment_options.is_enabled is True
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_deployment_options_and_native_options_coexist():
    # Catalog: both deployment_options and the native options field can be set independently.
    c = Catalog.model_validate(
        {
            "name": "dev",
            "options": {"key": "value"},
            "deployment_options": {"is_enabled": False},
        }
    )
    assert c.options == {"key": "value"}
    assert c.deployment_options.is_enabled is False
