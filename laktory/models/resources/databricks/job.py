from typing import Any
from typing import Union

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator
from pydantic._internal._decorators import Decorator
from pydantic._internal._decorators import FieldValidatorDecoratorInfo
from pydantic_core import PydanticUndefined

from laktory.models.resources.baseresource import ResourceLookup
from laktory.models.resources.databricks.accesscontrol import AccessControl
from laktory.models.resources.databricks.job_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.job_base import JobBase
from laktory.models.resources.databricks.job_base import JobTask
from laktory.models.resources.databricks.job_base import JobTaskForEachTaskTask
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.pulumiresource import PulumiResource


def _inject_field_validator(model_cls, field_name, method_name, func):
    """Duck-type a field validator onto an existing Pydantic model class."""
    cls_ref = f"{model_cls.__module__}.{model_cls.__qualname__}:{id(model_cls)}"
    model_cls.__pydantic_decorators__.field_validators[method_name] = Decorator(
        cls_ref=cls_ref,
        cls_var_name=method_name,
        func=classmethod(func).__get__(model_cls),
        shim=None,
        info=FieldValidatorDecoratorInfo(
            fields=(field_name,),
            mode="after",
            check_fields=None,
            json_schema_input_type=PydanticUndefined,
        ),
    )
    model_cls.model_rebuild(force=True)


@field_validator("depends_on")
@classmethod
def sort_depends_on(cls, v):
    if v is None:
        return v
    return sorted(v, key=lambda task: task.task_key)


_inject_field_validator(
    JobTask, "depends_on", "sort_depends_on", sort_depends_on.wrapped.__func__
)
_inject_field_validator(
    JobTaskForEachTaskTask,
    "depends_on",
    "sort_depends_on",
    sort_depends_on.wrapped.__func__,
)


class JobLookup(ResourceLookup):
    id: str = Field(
        serialization_alias="id", description="The id of the databricks job"
    )


class Job(JobBase, PulumiResource):
    """
    Databricks Job

    Examples
    --------
    ```py
    import io

    from laktory import models

    # Define job
    job_yaml = '''
    name: job-stock-prices
    job_clusters:
      - job_cluster_key: main
        new_cluster:
            spark_version: 16.3.x-scala2.12
            node_type_id: Standard_DS3_v2

    tasks:
      - task_key: ingest
        job_cluster_key: main
        notebook_task:
          notebook_path: /jobs/ingest_stock_prices.py
        libraries:
          - pypi:
              package: yfinance

      - task_key: pipeline
        depends_on:
          - task_key: ingest
        pipeline_task:
          pipeline_id: 74900655-3641-49f1-8323-b8507f0e3e3b

    access_controls:
      - group_name: account users
        permission_level: CAN_VIEW
      - group_name: role-engineers
        permission_level: CAN_MANAGE_RUN
    '''
    job = models.resources.databricks.Job.model_validate_yaml(io.StringIO(job_yaml))

    # Define job with for each task
    job_yaml = '''
    name: job-hello
    tasks:
      - task_key: hello-loop
        for_each_task:
          inputs: "[{'id':1, 'name': 'olivier'}, {'id':2, 'name': 'kubic'}]"
          task:
            task_key: hello-task
            notebook_task:
              notebook_path: /Workspace/Users/olivier.soucy@okube.ai/hello-world
              base_parameters:
                input: "{{input}}"
    '''
    job = models.resources.databricks.Job.model_validate_yaml(io.StringIO(job_yaml))
    ```

    References
    ----------

    * [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
    * [Pulumi Databricks Job](https://www.pulumi.com/registry/packages/databricks/api-docs/job/#databricks-job)
    """

    access_controls: list[AccessControl] = Field([], description="Access controls list")

    lookup_existing: JobLookup = Field(
        None,
        exclude=True,
        description="Specifications for looking up existing resource. Other attributes will be ignored.",
    )
    name_prefix: str = Field(None, description="Prefix added to the job name")
    name_suffix: str = Field(None, description="Suffix added to the job name")

    @field_validator("task")
    @classmethod
    def sort_tasks(cls, v: list[JobTask]) -> list[JobTask]:
        return sorted(v, key=lambda task: task.task_key)

    @model_validator(mode="after")
    def update_name(self) -> Any:
        with self.validate_assignment_disabled():
            if self.name_prefix:
                self.name = self.name_prefix + self.name
                self.name_prefix = ""
            if self.name_suffix:
                self.name = self.name + self.name_suffix
                self.name_suffix = ""

        return self

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list[PulumiResource]:
        """
        - permissions
        """
        resources = []
        if self.access_controls:
            resources += [
                Permissions(
                    resource_name=f"permissions-{self.resource_name}",
                    access_controls=self.access_controls,
                    job_id=f"${{resources.{self.resource_name}.id}}",
                )
            ]

        return resources

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:Job"

    @property
    def pulumi_excludes(self) -> Union[list[str], dict[str, bool]]:
        return ["access_controls", "name_prefix", "name_suffix"]

    @property
    def pulumi_properties(self):
        d = super().pulumi_properties

        # Rename dbt task schema
        for task in d["task"]:
            if "dbt_task" in task:
                if "schema_" in task["dbt_task"]:
                    task["dbt_task"]["schema"] = task["dbt_task"]["schema_"]
                    del task["dbt_task"]["schema_"]

        # Rename environment environment version
        if "environment" in d:
            for env in d["environment"]:
                if "spec" in env:
                    if "environmentVersion" in env["spec"]:
                        env["spec"]["client"] = env["spec"]["environmentVersion"]
                        del env["spec"]["environmentVersion"]
        return d

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return self.pulumi_excludes

    @property
    def terraform_properties(self) -> dict:
        d = super().terraform_properties

        # Rename dbt task schema
        if "task" in d:
            for task in d["task"]:
                if "dbt_task" in task:
                    if "schema_" in task["dbt_task"]:
                        task["dbt_task"]["schema"] = task["dbt_task"]["schema_"]
                        del task["dbt_task"]["schema_"]

        return d
