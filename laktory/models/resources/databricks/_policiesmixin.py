from pydantic import Field

from laktory.models.resources.databricks.policyinfo import PolicyInfo


class PoliciesMixin:
    """
    Shared support for Unity Catalog securables (catalog, schema, table)
    declaring their attribute-based access control (ABAC) policies with a
    `policies` field.
    """

    policies: list[PolicyInfo] = Field(
        None,
        description="Attribute-based access control (ABAC) policies defined on this securable. `on_securable_type` and `on_securable_fullname` are set automatically.",
    )

    def get_policies_additional_resources(self, securable_type: str) -> list:
        resources = []
        for policy in self.policies or []:
            if policy.on_securable_type is None:
                policy.on_securable_type = securable_type
            if policy.on_securable_fullname is None:
                policy.on_securable_fullname = f"${{resources.{self.resource_name}.id}}"
            resources += [policy]
        return resources
