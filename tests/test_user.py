from laktory.models import User
from laktory.models import Group

user = User(
    user_name="data.engineer@gmail.moc",
    groups=[
        "role-engineers",
    ],
    roles=["store-admin"],
)

group = Group(
    display_name="role-engineers",
)


def test_user_group():
    assert user.roles == ["store-admin"]
    assert group.display_name == "role-engineers"


if __name__ == "__main__":
    test_user_group()
