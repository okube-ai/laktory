from typing import Union

from laktory._testing import MonkeyPatch
from laktory.models import BaseModel
from laktory.typing import ResolvableInt
from laktory.typing import ResolvableString
from laktory.typing import var


class Owner(BaseModel):
    name: str = None
    id: int = None


class Cluster(BaseModel):
    id: ResolvableInt = None
    name: ResolvableString = None
    size: list[ResolvableInt] = None
    tags: Union[dict[ResolvableString, ResolvableInt], var] = None
    owner: Union[Owner, var] = None
    job_id: ResolvableString = None


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


def test_case_sensitive(monkeypatch):
    monkeypatch.setenv("HOST", ".local")

    # All lower
    c = Cluster(name="cluster-${vars.env}", variables={"env": "prd"}).inject_vars()
    assert c.name == "cluster-prd"

    # Var name upper
    c = Cluster(name="cluster-${vars.env}", variables={"ENV": "prd"}).inject_vars()
    assert c.name == "cluster-prd"

    # Reference upper
    c = Cluster(name="cluster-${vars.ENV}", variables={"env": "prd"}).inject_vars()
    assert c.name == "cluster-prd"

    # Env Var
    c = Cluster(
        name="cluster${vars.host}",
    ).inject_vars()
    assert c.name == "cluster.local"

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


def test_inplace():
    # Not in place
    c = Cluster(
        name="${vars.my_cluster}",
        variables={
            "my_cluster": "laktory-cluster",
        },
    )
    c1 = c.inject_vars(inplace=False)
    assert c.name == "${vars.my_cluster}"
    assert c1.name == "laktory-cluster"

    # In place
    c1 = c.inject_vars(inplace=True)
    assert c.name == "laktory-cluster"
    assert c1 is None


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
    test_case_sensitive(MonkeyPatch())
    test_submodels(MonkeyPatch())
    test_expression()
    test_complex()
    test_inplace()
    test_dump()
