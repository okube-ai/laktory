from typing import Union

import pulumi
import pulumi_databricks as databricks

from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.databricks.group import Group
from laktory.models.databricks.serviceprincipal import ServicePrincipal

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiServicePrincipal(PulumiResourcesEngine):
    @property
    def provider(self):
        return "databricks"

    def __init__(
        self,
        name=None,
        service_principal: ServicePrincipal = None,
        group_ids: dict[str, str] = None,
        opts=None,
    ):
        sp = service_principal
        if name is None:
            name = service_principal.resource_name
        super().__init__(self.t, name, {}, opts)

        opts = pulumi.ResourceOptions(
            parent=self,
            delete_before_replace=True,
        )

        self.sp = databricks.ServicePrincipal(
            name,
            opts=opts,
            **sp.model_pulumi_dump(),
        )

        self.roles = []
        for role in sp.roles:
            self.roles += [
                databricks.ServicePrincipalRole(
                    f"role-{role}-{name}",
                    service_principal_id=self.sp.id,
                    role=role,
                    opts=opts,
                )
            ]

        if not group_ids:
            if sp.groups:
                logger.warning(
                    "User is member of groups, but groups have not been provided. Group member resources will "
                    "be skipped."
                )
            return

        # Group Member
        self.group_members = []
        for g in sp.groups:
            # Find matching group
            group_id = group_ids.get(g, None)

            if group_id:
                self.group_members += [
                    databricks.GroupMember(
                        f"group-member-{sp.display_name}-{g}",
                        group_id=group_id,
                        member_id=self.sp.id,
                        opts=opts,
                    )
                ]
