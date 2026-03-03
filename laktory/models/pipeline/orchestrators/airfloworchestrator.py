from typing import Literal

from pydantic import Field

from laktory.models.pipelinechild import PipelineChild

ENV_KEY = "laktory"


class AirflowOrchestrator(PipelineChild):
    """
    Airflow used as an orchestrator to execute a Laktory pipeline.

    References
    ----------
    * [Data Pipeline](https://www.laktory.ai/concepts/pipeline/)
    """

    type: Literal["AIRFLOW"] = Field("AIRFLOW", description="Type of orchestrator")
    # description:

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def to_airflow(self):
        from airflow.sdk import dag
        from airflow.sdk import get_current_context
        from airflow.sdk import task

        pl = self.parent_pipeline
        plan = pl.get_execution_plan()

        def register_task(pl_task):
            @task(task_id=pl_task.name)
            def airflow_task():
                ctx = get_current_context()

                # Values from dag_run.conf (Trigger DAG JSON)
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

                # Defaults from DAG params (if set)
                params = ctx.get("params") or {}

                # Let conf override params override hard defaults
                full_refresh = conf.get(
                    "full_refresh", params.get("full_refresh", False)
                )

                pl_task.execute(
                    full_refresh=bool(full_refresh),
                )

            return airflow_task

        @dag(
            dag_id=pl.name,
            # start_date="",
            # schedule=None,
            # catchup=None,
            # tags=[],
            params={
                "full_refresh": False,
            },
        )
        def airflow_dag():
            tasks = {}
            nodes = {}

            # Create tasks and nodes
            for pl_task in plan.tasks:
                tasks[pl_task.name] = register_task(pl_task)
                nodes[pl_task.name] = tasks[pl_task.name]()

            # Set dependencies
            for task_name in tasks.keys():
                for upstream_task_name in plan.tasks_dict[
                    task_name
                ].upstream_task_names:
                    nodes[upstream_task_name] >> nodes[task_name]

        return airflow_dag()
