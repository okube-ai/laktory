import json
from laktory.models.resources.databricks import ClusterPolicy

cluster_policy = ClusterPolicy(
    name="okube",
    definition=json.dumps(
        {
            "dbus_per_hour": {
                "type": "range",
                "maxValue": 10,
            },
            "autotermination_minutes": {"type": "fixed", "value": 30, "hidden": True},
            "custom_tags.team": {
                "type": "fixed",
                "value": "okube",
            },
        }
    ),
    libraries=[
        {
            "pypi": {
                "package": "laktory==0.5.0",
            }
        }
    ],
    access_controls=[{"permission_level": "CAN_USE", "group_name": "account users"}],
)


def test_policy_cluster():
    print(cluster_policy)
    assert cluster_policy.name == "okube"
    assert (
        cluster_policy.definition
        == '{"dbus_per_hour": {"type": "range", "maxValue": 10}, "autotermination_minutes": {"type": "fixed", "value": 30, "hidden": true}, "custom_tags.team": {"type": "fixed", "value": "okube"}}'
    )


if __name__ == "__main__":
    test_policy_cluster()
