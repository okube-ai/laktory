from laktory.models.resources.databricks import Group

group = Group(
    display_name="role-engineers",
    member_ids=[
        "${resources.user-1.id}",
        "${resources.user-2.id}",
        "${resources.user-3.id}",
        "${resources.user-4.id}",
    ],
)



def test_group_members():
    assert group.display_name == "role-engineers"
    assert group.member_ids == [
        "${resources.user-1.id}",
        "${resources.user-2.id}",
        "${resources.user-3.id}",
        "${resources.user-4.id}",
    ]


if __name__ == "__main__":
    test_group_members()
