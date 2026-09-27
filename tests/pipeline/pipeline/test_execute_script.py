"""Tests for the job task entry point (`_execute`) and its run parameters."""

import sys

import pytest

from laktory import models
from laktory.models.pipeline import _execute as execute_module
from laktory.models.pipeline._execute import check_legacy_full_refresh


@pytest.mark.parametrize("full_refresh", [None, "", "false", False])
def test_check_legacy_full_refresh(full_refresh):
    check_legacy_full_refresh(full_refresh)


@pytest.mark.parametrize("full_refresh", ["true", "True", True])
def test_check_legacy_full_refresh_raises(full_refresh):
    with pytest.raises(ValueError, match="run with `refresh='FULL'` instead"):
        check_legacy_full_refresh(full_refresh)


@pytest.fixture
def run_execute(tmp_path, monkeypatch):
    """Run `_execute` with the given arguments and return the `Pipeline.execute` kwargs"""
    pl = models.Pipeline(
        name="pl",
        nodes=[
            {
                "name": "n",
                "sources": [{"path": str(tmp_path / "src"), "format": "PARQUET"}],
            }
        ],
    )
    filepath = tmp_path / "pl.json"
    filepath.write_text(pl.model_dump_json(exclude_unset=True))

    calls = []
    monkeypatch.setattr(
        models.Pipeline, "execute", lambda self, **kwargs: calls.append(kwargs)
    )

    def _run(*args):
        monkeypatch.setattr(
            sys, "argv", ["_execute", "--filepath", str(filepath), *args]
        )
        execute_module._execute()
        return calls[-1]

    return _run


def test_execute_script_refresh(run_execute):
    assert run_execute()["refresh"] == "INCREMENTAL"
    assert run_execute("--refresh", "full")["refresh"] == "full"


def test_execute_script_legacy_full_refresh(run_execute, caplog, monkeypatch):
    monkeypatch.setattr(execute_module.logger, "propagate", True)

    # Job deployed before 0.13.0, full refresh requested: fails instead of running
    # incrementally
    with pytest.raises(ValueError, match="replaced by `refresh`"):
        run_execute("--full_refresh", "true")
    with pytest.raises(ValueError, match="replaced by `refresh`"):
        run_execute("--full_refresh=true", "--refresh", "incremental")

    # Normal run of a job deployed before 0.13.0: incremental, with a warning
    with caplog.at_level("WARNING"):
        kwargs = run_execute("--full_refresh", "false")
    assert kwargs["refresh"] == "INCREMENTAL"
    assert "redeploy jobs deployed before 0.13.0" in caplog.text


def test_execute_script_unknown_arguments(run_execute, caplog, monkeypatch):
    monkeypatch.setattr(execute_module.logger, "propagate", True)
    with caplog.at_level("WARNING"):
        run_execute("--purge", "true")
    assert "Ignoring unknown arguments ['--purge', 'true']" in caplog.text
