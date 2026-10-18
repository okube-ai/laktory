# Typed setting values whose `effective_*` attribute the provider (1.137.0)
# marks optional instead of computed
TYPED_VALUES_WITH_OPTIONAL_EFFECTIVE = (
    "aibi_dashboard_embedding_access_policy",
    "aibi_dashboard_embedding_approved_domains",
    "automatic_cluster_update_workspace",
    "personal_compute",
    "restrict_workspace_admins",
    "workspace_label",
)


class SettingV2Mixin:
    """
    Shared Terraform output of settings v2 resources. For typed values whose
    `effective_*` attribute is optional but not computed, Terraform expects
    the effective value to stay null and fails the apply ("Provider produced
    inconsistent result") when Databricks returns it: the requested value is
    also sent as the effective value.
    """

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties
        for k in TYPED_VALUES_WITH_OPTIONAL_EFFECTIVE:
            if k in d:
                d[f"effective_{k}"] = d[k]
        return d
