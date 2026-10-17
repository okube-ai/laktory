from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import ArtifactAllowlist

allowlist = ArtifactAllowlist.model_validate(
    {
        "artifact_type": "INIT_SCRIPT",
        "artifact_matchers": [
            {
                "artifact": "/Volumes/laktory/unit_tests/init_scripts",
                "match_type": "PREFIX_MATCH",
            }
        ],
    }
)


def test_artifactallowlist():
    assert allowlist.artifact_matcher[0].match_type == "PREFIX_MATCH"
    assert allowlist.resource_name == "artifact-allowlist-INIT_SCRIPT"
    assert allowlist.terraform_resource_type == "databricks_artifact_allowlist"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(allowlist)
