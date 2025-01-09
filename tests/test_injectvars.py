import os

# from laktory._testing import MonkeyPatch
from laktory.models import BaseModel


class MonkeyPatch:
    def __init__(self):
        self.env0 = {}

    def setenv(self, key, value):
        self.env0[key] = os.getenv(key, None)
        os.environ[key] = value

    def cleanup(self):
        for k, v in self.env0.items():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v


class ClusterLibrary(BaseModel):
    name: str = None
    version: str = None


class Owner(BaseModel):
    name: str = None
    id: int = None


class Cluster(BaseModel):
    id: int | str = None
    name: str = None
    size: list[int] = None
    tags: dict[str, str] | str = None
    owner: Owner | str = None
    libraries: list[ClusterLibrary] = None
    job_id: str = None


def test_simple_substitution():
    c = Cluster(
        name="${vars.my_cluster}",
        id="${vars.cluster_id}",
        tags={"bu": "finance"},
        variables={
            "my_cluster": "laktory-cluster",
            "cluster_id": 23,
        },
    ).inject_vars()
    assert c.name == "laktory-cluster"
    assert c.id == 23


def test_regex():
    c = Cluster(
        job_id="${resources.this-job.id}",
        variables={
            r"\$\{resources\.(.*?)\}": r"${\1}",
        },
    ).inject_vars()
    assert c.job_id == "${this-job.id}"


def test_nested():
    c = Cluster(
        name="${vars.cluster_name}",
        variables={"env": "prd", "cluster_name": "laktory-cluster-${vars.env}"},
    ).inject_vars()
    assert c.name == "laktory-cluster-prd"
    assert c.variables["cluster_name"] == "laktory-cluster-${vars.env}"


def test_missing():
    c = Cluster(name="${vars.na}", variables={}).inject_vars()
    assert c.name == "${vars.na}"


def test_envvar(monkeypatch):
    monkeypatch.setenv("env", "dev")

    # Simple injection
    c = Cluster(
        name="cluster-${vars.env}",
    ).inject_vars()
    assert c.name == "cluster-dev"

    # Stack overwrite
    c = Cluster(name="cluster-${vars.env}", variables={"env": "prd"}).inject_vars()
    assert c.name == "cluster-prd"

    # Nested
    c = Cluster(
        name="${vars.region}", variables={"region": "east-${vars.env}"}
    ).inject_vars()
    assert c.name == "east-dev"

    # Test executed as script
    if isinstance(monkeypatch, MonkeyPatch):
        monkeypatch.cleanup()


def test_submodels(monkeypatch):
    monkeypatch.setenv("env", "stg")

    c = Cluster(
        name="cluster-${vars.env}",
        owner=Owner(name="owner-${vars.env}"),
        variables={
            "env": "prd",
        },
    ).inject_vars()
    assert c.name == "cluster-prd"
    assert c.owner.name == "owner-prd"

    # Submodel overwrite
    c = Cluster(
        name="cluster-${vars.env}",
        owner=Owner(
            name="owner-${vars.env}",
            variables={
                "env": "dev",
            },
        ),
        variables={
            "env": "prd",
        },
    ).inject_vars()
    assert c.name == "cluster-prd"
    assert c.owner.name == "owner-dev"

    # Submodel overwrite with env var
    c = Cluster(
        name="cluster-${vars.env}",
        owner=Owner(
            name="owner-${vars.env}",
            variables={
                "env": "dev",
            },
        ),
    ).inject_vars()
    assert c.name == "cluster-stg"
    assert c.owner.name == "owner-dev"

    # Test executed as script
    if isinstance(monkeypatch, MonkeyPatch):
        monkeypatch.cleanup()


def test_expression():
    # String compare
    c = Cluster(
        id="${{ 4 if vars.env == 'prod' else 2 }}",
        variables={
            "env": "prod",
        },
    ).inject_vars()
    assert c.id == 4

    # Bool compare
    c = Cluster(
        id="${{ 4 if vars.is_dev else 2 }}",
        variables={
            "is_dev": True,
        },
    ).inject_vars()
    assert c.id == 4

    # Dict keys
    c = Cluster(
        id="${{ vars.sizes[vars.env] }}",
        variables={
            "env": "prod",
            "sizes": {"dev": 2, "prod": 4},
        },
    ).inject_vars()
    assert c.id == 4


def test_complex():
    c = Cluster(
        tags="${vars.tags}",
        owner="${vars.owner}",
        variables={
            "env": "prd",
            "tags": {"id": "t1", "env": "${vars.env}"},
            "owner": {"name": "okube", "id": 0},
        },
    ).inject_vars()
    assert c.owner == Owner(name="okube", id=0)
    assert c.tags == {"id": "t1", "env": "prd"}


def test_dump():
    c = Cluster(
        name="${vars.my_cluster}",
        id="${vars.cluster_id}",
        tags={"bu": "finance"},
        variables={
            "my_cluster": "laktory-cluster",
            "cluster_id": 23,
        },
    )
    d = c.model_dump()
    di = c.inject_vars_into_dump(d, inplace=False)
    assert c.inject_vars(inplace=False).model_dump() == di


if __name__ == "__main__":
    test_simple_substitution()
    test_regex()
    test_nested()
    test_missing()
    test_envvar(MonkeyPatch())
    test_submodels(MonkeyPatch())
    test_expression()
    test_complex()
    test_dump()
