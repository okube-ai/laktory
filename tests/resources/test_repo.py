from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Repo
from laktory.models.resources.databricks.permissions import Permissions

repo = Repo(
    url="https://github.com/okube-ai/laktory",
    path="/Users/olivier.soucy@okube.ai/laktory-repo",
    branch="main",
    access_controls=[{"permission_level": "CAN_READ", "group_name": "account users"}],
)


def test_repo():
    assert repo.url == "https://github.com/okube-ai/laktory"


def test_repo_additional_resources():
    assert len(repo.additional_core_resources) == 1
    assert isinstance(repo.additional_core_resources[0], Permissions)


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(repo)


if __name__ == "__main__":
    test_repo()
