from pydantic import Field

from laktory.models.resources.databricks.entitytagassignment import EntityTagAssignment
from laktory.models.resources.databricks.workspaceentitytagassignment import (
    WorkspaceEntityTagAssignment,
)

WORKSPACE_ENTITY_TYPES = ("apps", "dashboards", "geniespaces", "notebooks")


class TagsMixin:
    """
    Shared support for resources declaring their tags with a `tags` field.
    Each tag is deployed as an `EntityTagAssignment` (Unity Catalog entities)
    or a `WorkspaceEntityTagAssignment` (workspace entities).
    """

    tags: dict[str, str | None] = Field(
        None,
        description="Tags assigned to the entity as `{tag_key: tag_value}`. Use a `null` value for a key-only tag. Each tag is deployed as a separate tag assignment resource.",
    )

    def get_tags_additional_resources(
        self, entity_type: str, entity: str, tags: dict[str, str | None] | None = None
    ) -> list:
        """
        Tag assignment resources for `entity` (a name or a resource reference).
        Defaults to the `tags` of this resource.
        """
        if tags is None:
            tags = self.tags
        resources = []
        for key, value in (tags or {}).items():
            if entity_type in WORKSPACE_ENTITY_TYPES:
                r = WorkspaceEntityTagAssignment(
                    entity_type=entity_type,
                    entity_id=entity,
                    tag_key=key,
                    tag_value=value,
                )
            else:
                r = EntityTagAssignment(
                    entity_type=entity_type,
                    entity_name=entity,
                    tag_key=key,
                    tag_value=value,
                )
            resources += [r]
        return resources
