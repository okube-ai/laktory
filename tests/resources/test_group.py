from laktory._testing import plan_resource
from laktory._testing import skip_terraform_plan
from laktory.models.resources.databricks import Group
from laktory.models.resources.databricks.groupmember import GroupMember

group = Group(
    display_name="role-engineers",
    member_ids=[
        "${resources.user-1.id}",
        "${resources.user-2.id}",
        "${resources.user-3.id}",
        "${resources.user-4.id}",
    ],
)

group_simple = Group(display_name="role-engineers")


def test_group_members():
    assert group.display_name == "role-engineers"
    assert group.member_ids == [
        "${resources.user-1.id}",
        "${resources.user-2.id}",
        "${resources.user-3.id}",
        "${resources.user-4.id}",
    ]

    assert len(group.additional_core_resources) == 4
    for r in group.additional_core_resources:
        assert isinstance(r, GroupMember)
        assert r.resource_key == f"{r.group_id}-{r.member_id}"


def test_terraform_plan():
    skip_terraform_plan()
    plan_resource(group_simple)


if __name__ == "__main__":
    test_group_members()
