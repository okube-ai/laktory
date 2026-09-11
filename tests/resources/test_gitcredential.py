from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import GitCredential

credential = GitCredential(
    git_provider="gitHub",
    git_username="my-user",
    name="my-credential",
)


def test_gitcredential():
    assert credential.git_provider == "gitHub"
    assert credential.git_username == "my-user"
    assert credential.name == "my-credential"
    assert credential.resource_key == "my-credential"
    assert credential.terraform_resource_type == "databricks_git_credential"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(credential)
