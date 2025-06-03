from laktory import models


class M2(models.BaseChild):
    name: str = None
    parent_name: str = None

    def update_from_parent(self):
        self.parent_name = self._parent.name


class M1(models.BaseChild):
    name: str = None
    parent_name: str = None
    m2: M2 = None

    @property
    def children_names(self):
        return ["m2"]

    def update_from_parent(self):
        self.parent_name = self._parent.name


class M0(models.BaseChild):
    name: str = None
    m1: M1 = None

    @property
    def children_names(self):
        return ["m1"]


def check_m0(m0):
    assert m0.m1.parent == m0
    assert m0.m1.parent_name == m0.name
    assert m0.m1.m2.parent == m0.m1
    assert m0.m1.m2.parent_name == m0.m1.name


def test_with_dict():
    m0 = M0(
        name="m0",
        m1={
            "name": "m1",
            "m2": {
                "name": "m2",
            },
        },
    )
    check_m0(m0)


def test_with_model():
    m0 = M0(name="m0", m1={"name": "m1", "m2": M2(name="m2")})
    check_m0(m0)

    m0 = M0(name="m0", m1=M1(name="m1", m2=M2(name="m2")))
    check_m0(m0)


def test_post_init_model():
    m0 = M0(name="m0")
    m0.m1 = M1(name="m1")
    m0.m1.m2 = M2(name="m2")
    check_m0(m0)


def test_post_init_dict():
    m0 = M0(name="m0")
    m0.m1 = {"name": "m1"}
    m0.m1.m2 = {"name": "m2"}
    check_m0(m0)


def test_set_parent():
    m0 = M0(name="m0")
    m1 = M1(name="m1")
    m1.parent = m0
