from typing import Union
import pulumi_databricks as databricks
from laktory.resourcesengines.pulumi.base import PulumiResourcesEngine
from laktory.models.group import Group
from laktory.models.serviceprincipal import ServicePrincipal

from laktory._logger import get_logger

logger = get_logger(__name__)


class PulumiServicePrincipal(PulumiResourcesEngine):

    @property
    def provider(self):
        return "databricks"

    @property
    def default_name(self) -> str:
        return f"service-principal-{self.model.display_name}"

    def __init__(
            self,
            name=None,
            service_principal: ServicePrincipal = None,
            groups: Union[list[Group], dict] = None,
            opts=None,
            **kwargs
    ):
        sp = service_principal
        if name is None:
            name = f"service-principal-{sp.display_name}"
        super().__init__(self.t, name, {}, opts)

        kwargs["opts"].delete_before_replace = getattr(kwargs["opts"], "delete_before_replace", True)

        self.sp = databricks.ServicePrincipal(
            f"service-principal-{sp.display_name}",
            display_name=sp.display_name,
            application_id=sp.application_id,
            **kwargs,
        )

        for role in sp.roles:
            databricks.ServicePrincipalRole(
                f"service-principal-role-{sp.display_name}-{role}",
                service_principal_id=self.sp.id,
                role=role,
                **kwargs,
            )

        if not groups:
            if sp.groups:
                logger.warning(
                    "User is member of groups, but groups have not been provided. Group member resources will "
                    "be skipped."
                )
            return

        # Group Member
        for g in sp.groups:

            # Find matching group
            group_id = None

            # List of Group models
            if isinstance(groups, list):
                for _g in groups:
                    if g == _g.display_name:
                        group_id = _g.resources.group.id

            elif isinstance(groups, dict):
                group_id = groups[g]

            databricks.GroupMember(
                f"group-member-{sp.display_name}-{g}",
                group_id=group_id,
                member_id=self.sp.id,
                **kwargs,
            )
