import os
from pathlib import Path

import laktory as lk
from laktory._testing import Paths
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan

paths = Paths(__file__)


def get_workspace_tree():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    return lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath),
    )


def test_workspace_tree():
    tree = get_workspace_tree()

    resources = tree.core_resources
    assert len(resources) == 6

    r = resources[0]
    assert r.source.endswith("/tests/data/tree/notebooks/listfiles.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "notebooks"

    r = resources[1]
    assert r.source.endswith("/tests/data/tree/notebooks/listsecrets0.ipynb")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"

    r = resources[2]
    assert r.source.endswith("/tests/data/tree/notebooks/listsecrets1.py")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"

    r = resources[3]
    assert r.source.endswith("/tests/data/tree/notebooks/select.sql")
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.dirpath == "notebooks"
    assert r.language == "SQL"

    r = resources[4]
    assert r.source.endswith("/tests/data/tree/pyfiles/hello.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"

    r = resources[5]
    assert r.source.endswith("/tests/data/tree/pyfiles/sysversion.py")
    assert isinstance(r, lk.models.resources.databricks.WorkspaceFile)
    assert r.dirpath == "pyfiles"


def test_workspace_tree_rel():
    cwd = Path(os.getcwd())
    tree = get_workspace_tree()
    tree.source = os.path.relpath(tree.source, cwd)

    resources = tree.core_resources
    assert len(resources) == 6

    r = resources[0]
    assert r.source == tree.source + "/notebooks/listfiles.py"


def test_workspace_tree_with_path():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath), path="/Workspace/tmp/"
    )

    resources = tree.core_resources
    assert len(resources) == 6

    assert [r.path for r in resources] == [
        "/Workspace/tmp/notebooks/listfiles.py",
        "/Workspace/tmp/notebooks/listsecrets0.ipynb",
        "/Workspace/tmp/notebooks/listsecrets1.py",
        "/Workspace/tmp/notebooks/select.sql",
        "/Workspace/tmp/pyfiles/hello.py",
        "/Workspace/tmp/pyfiles/sysversion.py",
    ]


def test_access_controls():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(dirpath),
        access_controls=[
            {"permission_level": "CAN_READ", "group_name": "account users"}
        ],
    )

    r0 = tree.core_resources[0]
    assert isinstance(r0, lk.models.resources.databricks.WorkspaceFile)
    r1 = tree.core_resources[1]
    assert isinstance(r1, lk.models.resources.databricks.Permissions)
    assert r1.access_control[0].permission_level == "CAN_READ"
    assert r1.workspace_file_path == "/.laktory/notebooks/listfiles.py"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(get_workspace_tree())


def _tree_stack():
    dirpath = paths.data / ".." / ".." / "data" / "tree/"
    return lk.models.Stack(
        name="test",
        resources={
            "databricks_clusters": {
                "cluster-pre": {
                    "cluster_name": "pre",
                    "spark_version": "16.4.x-scala2.13",
                    "node_type_id": "m5.large",
                    "num_workers": 1,
                }
            },
            "databricks_jobs": {
                "job-post": {
                    "name": "post",
                    "resource_options": {"depends_on": ["${resources.workspace-tree}"]},
                }
            },
            "databricks_workspacetrees": {
                "workspace-tree": {
                    "source": str(dirpath),
                    "resource_options": {"depends_on": ["${resources.cluster-pre}"]},
                }
            },
        },
    )


def test_utf8_source_files(tmp_path):
    """Regression test: files containing UTF-8 multi-byte characters (emoji,
    accents) must be read with an explicit utf-8 encoding so that scanning for
    the Databricks notebook marker does not crash on platforms whose default
    locale encoding is not UTF-8 (e.g. cp1252 on French Windows)."""
    treepath = tmp_path / "tree"
    treepath.mkdir()

    # Notebook (python) with emoji + accented content and the notebook marker
    (treepath / "notebook.py").write_text(
        "# Databricks notebook source\n# 🚀 énçodïng — dashes\nprint('café')\n",
        encoding="utf-8",
    )
    # Plain workspace file with emoji content but no marker
    (treepath / "file.py").write_text("# 🎉 not a notebook\n", encoding="utf-8")
    # SQL notebook with emoji + accented content and the SQL notebook marker
    (treepath / "query.sql").write_text(
        "-- Databricks notebook source\n-- 🧮 requête\nSELECT 1;\n",
        encoding="utf-8",
    )

    tree = lk.models.resources.databricks.WorkspaceTree(source=str(treepath))

    # Must complete without raising UnicodeDecodeError on any platform
    resources = tree.additional_core_resources

    by_name = {Path(r.source).name: r for r in resources}
    assert isinstance(by_name["notebook.py"], lk.models.resources.databricks.Notebook)
    assert by_name["notebook.py"].language == "PYTHON"
    assert isinstance(by_name["file.py"], lk.models.resources.databricks.WorkspaceFile)
    assert isinstance(by_name["query.sql"], lk.models.resources.databricks.Notebook)
    assert by_name["query.sql"].language == "SQL"


def test_render_paths_bare_extension_glob(tmp_path):
    """A bare '*.ext' pattern matches files with that extension at any
    depth, same as an extension-based opt-in would."""
    treepath = tmp_path / "tree"
    treepath.mkdir()
    (treepath / "app.json").write_text('{"catalog": "${vars.catalog}"}')
    (treepath / "plain.txt").write_text("literal ${vars.catalog}")

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(treepath), render_paths=["*.json"]
    )

    resources = {Path(r.source_).name: r for r in tree.additional_core_resources}
    assert resources["app.json"].render_vars is True
    assert resources["plain.txt"].render_vars is False


def test_render_paths_scoped_to_subdir(tmp_path):
    treepath = tmp_path / "tree"
    (treepath / "configs").mkdir(parents=True)
    (treepath / "configs" / "app.json").write_text('{"catalog": "${vars.catalog}"}')
    (treepath / "other.json").write_text('{"catalog": "${vars.catalog}"}')

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(treepath), render_paths=["configs/*.json"]
    )

    by_rel = {}
    for r in tree.additional_core_resources:
        rel = os.path.relpath(r.source_, str(treepath))
        by_rel[Path(rel).as_posix()] = r

    assert by_rel["configs/app.json"].render_vars is True
    assert by_rel["other.json"].render_vars is False


def test_render_paths_multiple_patterns(tmp_path):
    treepath = tmp_path / "tree"
    (treepath / "configs").mkdir(parents=True)
    (treepath / "configs" / "app.yaml").write_text("catalog: ${vars.catalog}")
    (treepath / "other.json").write_text('{"catalog": "${vars.catalog}"}')
    (treepath / "untouched.txt").write_text("literal ${vars.catalog}")

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(treepath),
        render_paths=["*.json", "configs/*.yaml"],
    )

    by_name = {Path(r.source_).name: r for r in tree.additional_core_resources}
    assert by_name["app.yaml"].render_vars is True
    assert by_name["other.json"].render_vars is True
    assert by_name["untouched.txt"].render_vars is False


def test_render_default_off_backward_compatible(tmp_path):
    treepath = tmp_path / "tree"
    treepath.mkdir()
    (treepath / "app.json").write_text('{"catalog": "${vars.catalog}"}')

    tree = lk.models.resources.databricks.WorkspaceTree(source=str(treepath))

    resources = tree.additional_core_resources
    assert len(resources) == 1
    assert resources[0].render_vars is False
    assert resources[0].source == str(treepath / "app.json")


def test_render_notebook_file(tmp_path):
    """A file matching render_paths and detected as a notebook is still
    deployed as a Notebook, and is rendered like any other opted-in file."""
    treepath = tmp_path / "tree"
    treepath.mkdir()
    (treepath / "notebook.py").write_text(
        "# Databricks notebook source\nprint('${vars.env}')\n"
    )

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(treepath), render_paths=["*.py"]
    )

    resources = tree.additional_core_resources
    assert len(resources) == 1
    r = resources[0]
    assert isinstance(r, lk.models.resources.databricks.Notebook)
    assert r.render_vars is True


def test_render_tree_variables_precedence(tmp_path, monkeypatch):
    from laktory._settings import settings

    monkeypatch.setattr(settings, "build_root", str(tmp_path / "build"))

    treepath = tmp_path / "tree"
    treepath.mkdir()
    (treepath / "app.json").write_text('{"catalog": "${vars.catalog}"}')

    tree = lk.models.resources.databricks.WorkspaceTree(
        source=str(treepath),
        render_paths=["*.json"],
        variables={"catalog": "tree_catalog"},
    )

    r = tree.additional_core_resources[0]
    r.build()
    assert Path(r.source).read_text() == '{"catalog": "tree_catalog"}'


def test_depends_on_directions():
    """A virtual WorkspaceTree participates in the dependency graph in both
    directions: its own depends_on propagates to the child files (deploy X
    before the tree), and a dependency on the tree expands to all its child
    files (deploy the tree before X)."""
    d = _tree_stack().to_terraform().model_dump()

    child_names = list(d["resource"].get("databricks_notebook", {})) + list(
        d["resource"].get("databricks_workspace_file", {})
    )
    assert child_names

    # Direction 1: each child inherits the tree's upstream dependency.
    for rtype in ("databricks_notebook", "databricks_workspace_file"):
        for body in d["resource"].get(rtype, {}).values():
            assert body.get("depends_on") == ["databricks_cluster.cluster-pre"]

    # Direction 2: the job's dependency on the virtual tree expands to all its
    # children, and the virtual name never leaks into the Terraform output.
    job_deps = d["resource"]["databricks_job"]["job-post"]["depends_on"]
    assert not any("workspace-tree" in dep for dep in job_deps)
    for child in child_names:
        assert any(child in dep for dep in job_deps)
