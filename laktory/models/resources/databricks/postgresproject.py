from pydantic import Field
from pydantic import model_validator

from laktory.models.resources.databricks.postgresbranch import PostgresBranch
from laktory.models.resources.databricks.postgresbranch import add_depends_on
from laktory.models.resources.databricks.postgresproject_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.postgresproject_base import PostgresProjectBase


class PostgresProject(PostgresProjectBase):
    """
    Lakebase Autoscaling (Postgres) Project

    Top-level container of a managed, autoscaling Postgres: branches, each
    with endpoints (compute), databases and roles. A `production` branch with
    a `primary` read-write endpoint, a `databricks-postgres` database and an
    owner role is created implicitly with the project; declare them with
    `replace_existing: true` to manage them.

    Lakebase Autoscaling resources follow specific Terraform semantics:
    changes made outside Terraform are not detected (no drift detection), a
    value removed from `spec` is kept by the server, and the Terraform state
    should be shared (remote backend) when several users manage the same
    project.

    Examples
    --------
    ```py
    import io

    from laktory import models

    project_yaml = '''
    project_id: app
    spec:
      pg_version: 17
      display_name: App backend
      default_endpoint_settings:
        autoscaling_limit_min_cu: 0.5
        autoscaling_limit_max_cu: 4.0
    branches:
    - branch_id: production
      replace_existing: true
      spec:
        no_expiry: true
    - branch_id: dev
      spec:
        source_branch: projects/app/branches/production
        ttl: 604800s
    '''
    project = models.resources.databricks.PostgresProject.model_validate_yaml(
        io.StringIO(project_yaml)
    )
    ```

    References
    ----------

    * [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/)
    * [Terraform databricks_postgres_project](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_project)
    """

    # Laktory-specific
    branches: list[PostgresBranch] = Field(
        None,
        description="Branches of the project. Their `parent` is set automatically.",
    )

    @model_validator(mode="after")
    def assign_parents(self):
        """Set the project path on nested branches and their resources"""
        for b in self.branches or []:
            if b.parent is None:
                b.parent = self.path
            b.assign_parents()
        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def path(self) -> str:
        """Lakebase resource path `projects/{project_id}`"""
        return f"projects/{self.project_id}"

    @property
    def resource_key(self) -> str:
        return self.project_id

    @property
    def additional_core_resources(self) -> list:
        """
        - branches

        A branch forked from a sibling (`spec.source_branch`) is created after it.
        """
        branches = self.branches or []
        paths = {b.path: b for b in branches}
        for b in branches:
            source = paths.get(b.spec.source_branch) if b.spec else None
            if source is not None and source is not b:
                add_depends_on(b, [source])
        return branches

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["branches"]
