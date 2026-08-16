"""
Tests for opt-in variable rendering of local file-content fields
(`WorkspaceFile.source`, `Notebook.source`, `DbfsFile.source`,
`Dashboard.file_path`), shared via `RenderableFileMixin`.
"""

from pathlib import Path

import pytest

from laktory._settings import settings
from laktory.models.resources.databricks import Dashboard
from laktory.models.resources.databricks import DbfsFile
from laktory.models.resources.databricks import Notebook
from laktory.models.resources.databricks import WorkspaceFile


def _make(cls, path, **kwargs):
    if cls is Dashboard:
        return cls(
            display_name="mydash",
            warehouse_id="wh123",
            file_path=str(path),
            **kwargs,
        )
    return cls(source=str(path), dirpath="x/", **kwargs)


def _content_field(r):
    return r.file_path if isinstance(r, Dashboard) else r.source


CLASSES = [WorkspaceFile, Notebook, DbfsFile, Dashboard]


@pytest.fixture(autouse=True)
def _build_root(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "build_root", str(tmp_path / "build"))
    return tmp_path / "build"


@pytest.mark.parametrize("cls", CLASSES)
def test_render_vars_default_off(tmp_path, cls):
    """No `render_vars` set -> field value untouched, no rendering."""
    src = tmp_path / "content.json"
    src.write_text('{"catalog": "${vars.catalog}"}')

    r = _make(cls, src)
    assert r.render_vars is False
    assert _content_field(r) == str(src)


@pytest.mark.parametrize("cls", CLASSES)
def test_render_vars_stages_resolved_content(tmp_path, cls, _build_root):
    src = tmp_path / "content.json"
    src.write_text('{"catalog": "${vars.catalog}"}')

    r = _make(cls, src, render_vars=True, variables={"catalog": "dev_catalog"})

    staged = _content_field(r)
    assert staged != str(src)
    assert str(_build_root) in staged

    r.build()
    assert Path(staged).read_text() == '{"catalog": "dev_catalog"}'
    # Original untouched
    assert src.read_text() == '{"catalog": "${vars.catalog}"}'


@pytest.mark.parametrize("cls", CLASSES)
def test_render_vars_resource_key_unaffected(tmp_path, cls):
    """Terraform resource identity is derived from `path`/`display_name`,
    never from the (possibly staged) content field."""
    src = tmp_path / "content.json"
    src.write_text("{}")

    r0 = _make(cls, src)
    r1 = _make(cls, src, render_vars=True)

    assert r0.resource_key == r1.resource_key


@pytest.mark.parametrize("cls", CLASSES)
def test_build_raises_on_missing_original(tmp_path, cls):
    missing = tmp_path / "does_not_exist.json"

    r = _make(cls, missing, render_vars=True)
    with pytest.raises(FileNotFoundError):
        r.build()


@pytest.mark.parametrize("cls", CLASSES)
def test_build_noop_when_render_vars_false(tmp_path, cls, _build_root):
    src = tmp_path / "content.json"
    src.write_text("{}")

    r = _make(cls, src)
    r.build()  # should not raise, should not create anything
    assert not _build_root.exists()


@pytest.mark.parametrize("cls", CLASSES)
def test_check_staged_raises_before_build(tmp_path, cls):
    src = tmp_path / "content.json"
    src.write_text("{}")

    r = _make(cls, src, render_vars=True)
    with pytest.raises(RuntimeError):
        r.check_staged()


@pytest.mark.parametrize("cls", CLASSES)
def test_check_staged_passes_after_build(tmp_path, cls):
    src = tmp_path / "content.json"
    src.write_text("{}")

    r = _make(cls, src, render_vars=True)
    r.build()
    r.check_staged()  # should not raise


@pytest.mark.parametrize("cls", CLASSES)
def test_check_staged_noop_when_render_vars_false(tmp_path, cls):
    src = tmp_path / "content.json"
    src.write_text("{}")

    r = _make(cls, src)
    r.check_staged()  # should not raise, render_vars is False


@pytest.mark.parametrize("cls", CLASSES)
def test_render_vars_excluded_from_terraform(tmp_path, cls):
    src = tmp_path / "content.json"
    src.write_text("{}")

    r = _make(cls, src, render_vars=True)
    assert "render_vars" not in r.terraform_properties
