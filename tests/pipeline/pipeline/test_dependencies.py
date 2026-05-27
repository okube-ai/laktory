"""Tests for Pipeline._dependencies and ._imports."""

from laktory import models
from laktory._version import VERSION


def _pl_with_deps():
    return models.Pipeline(
        name="pl",
        dependencies=["requests>=2.0", "./wheels/lake-0.0.1-py3-none-any.whl"],
        imports=["re"],
        nodes=[],
    )


def test_laktory_always_in_dependencies():
    pl = models.Pipeline(name="pl", nodes=[])
    assert any("laktory" in d for d in pl._dependencies)


def test_extra_dependency_appended():
    pl = models.Pipeline(name="pl", dependencies=["requests>=2.0"], nodes=[])
    deps = pl._dependencies
    assert "requests>=2.0" in deps
    assert f"laktory=={VERSION}" in deps


def test_laktory_not_duplicated():
    pl = models.Pipeline(name="pl", dependencies=[f"laktory=={VERSION}"], nodes=[])
    laktory_deps = [d for d in pl._dependencies if "laktory" in d]
    assert len(laktory_deps) == 1


def test_imports_derived_from_requirements():
    pl = _pl_with_deps()
    assert pl._imports == ["re", "requests", "lake"]
