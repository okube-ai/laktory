from laktory import models
from laktory import settings
from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import EntityTagAssignment
from laktory.models.resources.databricks import File
from laktory.models.resources.databricks import Volume


def get_volume():
    return Volume(
        name="landing",
        catalog_name="laktory",
        schema_name="unit_tests",
        tags={"purpose": "ingestion"},
        files=[
            {"source": "./test_catalog.py", "dirpath": "/configs/"},
            {"source": "./test_schema.py"},
            {"source": "./test_table.py", "path": "/Volumes/other/vol/table.py"},
        ],
    )


def test_volume():
    volume = get_volume()
    assert volume.full_name == "laktory.unit_tests.landing"
    assert volume.path == "/Volumes/laktory/unit_tests/landing"


def test_volume_tags_and_files():
    volume = get_volume()
    resources = volume.additional_core_resources

    (tag,) = [r for r in resources if isinstance(r, EntityTagAssignment)]
    assert tag.entity_type == "volumes"
    assert tag.entity_name == "${resources.volume-laktory-unit_tests-landing.id}"

    files = [r for r in resources if isinstance(r, File)]
    assert [f.path for f in files] == [
        "/Volumes/laktory/unit_tests/landing/configs/test_catalog.py",
        "/Volumes/laktory/unit_tests/landing/test_schema.py",
        "/Volumes/other/vol/table.py",
    ]

    # Idempotent
    assert [
        f.path for f in volume.additional_core_resources if isinstance(f, File)
    ] == [f.path for f in files]
    assert "files" not in volume.terraform_properties
    assert "tags" not in volume.terraform_properties


def test_build_renders_volume_files(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "build_root", str(tmp_path / "build"))
    source = tmp_path / "config.json"
    source.write_text('{"catalog": "${vars.catalog}"}')

    stack = models.Stack(
        name="render-test",
        organization="o3",
        variables={"catalog": "dev_cat"},
        environments={"dev": {}},
        resources=models.StackResources(
            databricks_schemas={
                "schema-sources": {
                    "catalog_name": "dev",
                    "name": "sources",
                    "volumes": [
                        {
                            "name": "landing",
                            "files": [{"source": str(source), "render_vars": True}],
                        }
                    ],
                }
            },
        ),
    )
    stack.build(env_name="dev")

    staged = tmp_path / "build" / "files" / "Volumes/dev/sources/landing/config.json"
    assert staged.read_text() == '{"catalog": "dev_cat"}'


def test_terraform_plan():
    skip_terraform_plan()
    volume = get_volume().model_copy(update={"files": None})
    plan_resource(volume)
