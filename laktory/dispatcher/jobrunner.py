import time
from typing import Literal
from databricks.sdk.service.jobs import Wait

from laktory.dispatcher.runner import Runner
from laktory._logger import get_logger

logger = get_logger(__name__)


class JobRunner(Runner):
    _run_start: Wait = None

    def get_id(self) -> str:
        logger.info(f"Getting id for job {self.name}")
        for _job in self.wc.jobs.list(name=self.name):
            _job_data = _job.as_dict()
            if self.name == _job_data["settings"]["name"]:
                self.id = _job.job_id
                return self.id

    def run(
            self,
            wait: bool = True,
            timeout: int = 20*60,
            current_run_action: Literal["WAIT", "CANCEL", "FAIL"] = "WAIT",

    ):

        active_runs = list(self.wc.jobs.list_runs(job_id=self.id, active_only=True))

        if len(active_runs) > 0:

            if current_run_action.upper() == "FAIL":
                logger.info(f"Job {self.name} already running...")
                raise Exception(f"Job {self.name} already running...")

            elif current_run_action.upper() == "WAIT":
                logger.info(f"Job {self.name} waiting for current run(s) to be completed...")
                for run in active_runs:
                    logger.info(f"Waiting for {run.run_id}")
                    self.wc.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)

            elif current_run_action.upper() == "CANCEL":
                logger.info(f"Job {self.name} cancelling current run(s)...")
                for run in active_runs:
                    logger.info(f"Cancelling {run.run_id}")
                    self.wc.jobs.cancel_run_and_wait(run_id=run.run_id)

        # Start update
        t0 = time.time()
        logger.info(f"Job {self.name} run started...")
        self._run_start = self.wc.jobs.run_now(
            job_id=self.id,
        )
