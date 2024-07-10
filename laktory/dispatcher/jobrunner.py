from __future__ import annotations
from typing import TYPE_CHECKING

import time
from typing import Literal

from laktory.dispatcher.dispatcherrunner import DispatcherRunner
from laktory._logger import get_logger

if TYPE_CHECKING:
    from databricks.sdk.service.jobs import Wait
    from databricks.sdk.service.jobs import Run
    from databricks.sdk.service.jobs import RunLifeCycleState
    from databricks.sdk.errors import OperationFailed

logger = get_logger(__name__)


class JobRunner(DispatcherRunner):
    """
    Job runner.
    """

    _run_start: Wait = None
    _run: Run = None

    def get_id(self) -> str:
        """Get deployed job id"""
        logger.info(f"Getting id for job {self.name}")
        for _job in self.wc.jobs.list(name=self.name):
            _job_data = _job.as_dict()
            if self.name == _job_data["settings"]["name"]:
                self.id = _job.job_id
                return self.id

    def run(
        self,
        wait: bool = True,
        timeout: int = 20 * 60,
        raise_exception: bool = False,
        current_run_action: Literal["WAIT", "CANCEL", "FAIL"] = "WAIT",
    ):
        """
        Run remote job and monitor failures.

        Parameters
        ----------
        wait:
            If `True`, runs synchronously and wait for completion.
        timeout:
            Maximum time allowed for the job to run before an exception is
            raised.
        raise_exception:
            If `True`, exceptions are raised if the job fails.
        current_run_action:
            Action to take with respect to current run (if any). Possible
            options are:
                - WAIT: wait for the current run to complete
                - CANCEL: cancel the current run
                - FAIL: raise an exception

        Returns
        -------
        output:
            None
        """
        from databricks.sdk.service.jobs import RunLifeCycleState
        from databricks.sdk.errors import OperationFailed

        active_runs = list(self.wc.jobs.list_runs(job_id=self.id, active_only=True))

        if len(active_runs) > 0:
            if current_run_action.upper() == "FAIL":
                logger.info(f"Job {self.name} already running...")
                raise Exception(f"Job {self.name} already running...")

            elif current_run_action.upper() == "WAIT":
                logger.info(
                    f"Job {self.name} waiting for current run(s) to be completed..."
                )
                for run in active_runs:
                    try:
                        self.wc.jobs.wait_get_run_job_terminated_or_skipped(
                            run_id=run.run_id
                        )
                    except OperationFailed:
                        pass

            elif current_run_action.upper() == "CANCEL":
                logger.info(f"Job {self.name} cancelling current run(s)...")
                for run in active_runs:
                    self.wc.jobs.cancel_run_and_wait(run_id=run.run_id)

        # Start update
        t0 = time.time()
        logger.info(f"Job {self.name} run started...")
        self._run_start = self.wc.jobs.run_now(
            job_id=self.id,
        )

        pstates = {}
        self.get_run()
        logger.info(f"Job {self.name} run URL: {self._run.run_page_url}")
        if wait:
            while (
                time.time() - t0 < timeout
                or self.run_state == RunLifeCycleState.TERMINATED
            ):
                self.get_run()

                if self.run_state != pstates.get(self.run_id, None):
                    logger.info(f"Job {self.name} state: {self.run_state.value}")
                    pstates[self.run_id] = self.run_state

                for task in self._run.tasks:
                    state = task.state
                    if state.life_cycle_state != pstates.get(task.run_id, None):
                        logger.info(
                            f"   Task {self.name}.{task.task_key} state: {state.life_cycle_state.value}"
                        )
                    pstates[task.run_id] = state.life_cycle_state

                if self.run_state in [
                    RunLifeCycleState.TERMINATED,
                    RunLifeCycleState.SKIPPED,
                    RunLifeCycleState.INTERNAL_ERROR,
                ]:
                    break

                time.sleep(1)

            logger.info(
                f"Job {self.name} run terminated after {time.time() - t0: 5.2f} sec with {self.run_state} ({self._run.state.state_message})"
            )
            for task in self._run.tasks:
                logger.info(
                    f"Task {self.name}.{task.task_key} terminated with {task.state.result_state} ({task.state.state_message})"
                )
            if raise_exception and self.run_state != RunLifeCycleState.TERMINATED:
                raise Exception(
                    f"Job {self.name} update not completed ({self.run_state})"
                )

    def get_run(self):
        self._run = self.wc.jobs.get_run(
            run_id=self.run_id,
        )
        return self._run

    @property
    def run_state(self):
        return self._run.state.life_cycle_state

    @property
    def run_id(self) -> str:
        return self._run_start.run_id
