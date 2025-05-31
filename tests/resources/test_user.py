from __future__ import annotations

import pytest

from laktory.models.resources.databricks import Group
from laktory.models.resources.databricks import User
from laktory.models.resources.databricks.user import UserLookup

user = User(
    user_name="data.engineer@gmail.moc",
    group_ids=[
        "${resources.group-role-engineers.id}",
    ],
    roles=["store-admin"],
)

group = Group(
    display_name="role-engineers",
)


def test_user_group():
    assert user.roles == ["store-admin"]
    assert group.display_name == "role-engineers"


def test_both_user_id_and_user_name():
    with pytest.raises(ValueError):
        UserLookup(user_id=123, user_name="test_user")


def test_neither_user_id_and_user_name():
    with pytest.raises(ValueError):
        UserLookup()


if __name__ == "__main__":
    test_user_group()
    test_both_user_id_and_user_name()
    test_neither_user_id_and_user_name()
