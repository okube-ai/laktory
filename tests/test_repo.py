from laktory.models.resources.databricks import Repo

repo = Repo(
    url="https://github.com/okube-ai/laktory",
    path="/Users/olivier.soucy@okube.ai/laktory-repo",
    branch="main",
    access_controls=[{"permission_level": "CAN_READ", "group_name": "account users"}],
)


def test_repo():
    assert repo.url == "https://github.com/okube-ai/laktory"


if __name__ == "__main__":
    test_repo()
