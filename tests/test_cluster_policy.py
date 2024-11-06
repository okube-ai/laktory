import json
from laktory.models.resources.databricks import ClusterPolicy

definition = {
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

cluster_policy = ClusterPolicy(
    name="okube",
    definition=json.dumps(definition),
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
    assert cluster_policy.name == "okube"
    assert cluster_policy.definition == json.dumps(definition)


def test_policy_cluster_as_dict():

    cp = ClusterPolicy(
        name="okube",
        definition=definition,
        libraries=[
            {
                "pypi": {
                    "package": "laktory==0.5.0",
                }
            }
        ],
        access_controls=[
            {"permission_level": "CAN_USE", "group_name": "account users"}
        ],
    )

    assert cluster_policy.name == "okube"
    assert cp.definition == json.dumps(definition)


if __name__ == "__main__":
    test_policy_cluster()
