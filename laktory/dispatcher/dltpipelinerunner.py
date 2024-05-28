from __future__ import annotations
import time
from typing import Literal
from typing import TYPE_CHECKING

from laktory.dispatcher.dispatcherrunner import DispatcherRunner
from laktory.datetime import unix_timestamp
from laktory._logger import get_logger

if TYPE_CHECKING:
    from databricks.sdk.service.pipelines import StartUpdateResponse
    from databricks.sdk.service.pipelines import GetUpdateResponse

logger = get_logger(__name__)


class DLTPipelineRunner(DispatcherRunner):
    """
    DLT Pipeline runner.
    """

    _update_start: StartUpdateResponse = None
    _update: GetUpdateResponse = None

    def get_id(self) -> str:
        """Get deployed pipeline id"""
        logger.info(f"Getting id for pipeline {self.name}")
        for _pl in self.wc.pipelines.list_pipelines(
            filter=f"name LIKE '{self.name}'", max_results=1
        ):
            if self.name == _pl.name:
                self.id = _pl.pipeline_id
                return self.id

    def run(
        self,
        wait: bool = True,
        timeout: int = 20 * 60,
        full_refresh: bool = False,
        raise_exception: bool = False,
        current_run_action: Literal["WAIT", "CANCEL", "FAIL"] = "WAIT",
    ):
        """
        Run remote pipeline and monitor failures.

        Parameters
        ----------
        wait:
            If `True`, runs synchronously and wait for completion.
        timeout:
            Maximum time allowed for the pipeline to run before an exception is
            raised.
        full_refresh:
            If `True`, tables are fully refreshed (re-built). Otherwise, only
            increments are processed.
        raise_exception:
            If `True`, exceptions are raised if the pipeline fails.
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
        from databricks.sdk.service.pipelines import UpdateInfoState
        from databricks.sdk.service.pipelines import EventLevel
        from databricks.sdk.core import DatabricksError

        event_ids = []

        # Start update
        t0 = time.time()
        try:
            self._update_start = self.wc.pipelines.start_update(
                pipeline_id=self.id,
                full_refresh=full_refresh,
                # validate_only=False,
            )
            logger.info(f"Pipeline {self.name} update started...")

        except DatabricksError as e:
            # Exception is raised when update is currently in progress
            if current_run_action.upper() == "FAIL":
                logger.info(f"Pipeline {self.name} already running...")
                raise e

            elif current_run_action.upper() == "WAIT":
                logger.info(
                    f"Pipeline {self.name} waiting for current update to be completed..."
                )
                self.wc.pipelines.wait_get_pipeline_idle(pipeline_id=self.id)

            elif current_run_action.upper() == "CANCEL":
                logger.info(f"Pipeline {self.name} cancelling current update...")
                self.wc.pipelines.stop_and_wait(pipeline_id=self.id)

            logger.info(f"Pipeline {self.name} update started...")
            self._update_start = self.wc.pipelines.start_update(
                pipeline_id=self.id,
                full_refresh=full_refresh,
                # validate_only=False,
            )

        pstate = None
        self.get_update()
        logger.info(f"Pipeline {self.name} run URL: {self.update_url}")
        if wait:
            while (
                time.time() - t0 < timeout
                or self.update_state == UpdateInfoState.COMPLETED
            ):
                self.get_update()

                if self.update_state != pstate:
                    logger.info(
                        f"Pipeline {self.name} state: {self.update_state.value}"
                    )
                    pstate = self.update_state

                # filter don't seem to work
                for event in self.wc.pipelines.list_pipeline_events(
                    pipeline_id=self.id,
                    max_results=100,
                    # filter=f"(level in ('ERROR', 'WARN')) AND (timestamp > '{utc_datetime(t0).isoformat()}Z')"
                    # filter=f"level in ('ERROR', 'WARN')",
                    # filter=f"timestamp > '{utc_datetime(t0).isoformat()}Z'"
                    page_token=None,
                ):
                    if (
                        event.id in event_ids
                        or unix_timestamp(event.timestamp) < t0
                        or event.origin.update_id != self.update_id
                    ):
                        continue

                    if event.level == EventLevel.WARN:
                        logger.warn(event.message)
                    elif event.level == EventLevel.ERROR:
                        logger.error(event.message)
                    event_ids += [event.id]

                if self.update_state in [
                    UpdateInfoState.CANCELED,
                    UpdateInfoState.COMPLETED,
                    UpdateInfoState.FAILED,
                ]:
                    break

                time.sleep(1.0)

            logger.info(
                f"Pipeline {self.name} update terminated after {time.time() - t0: 5.2f} sec with {self.update_state}"
            )
            if raise_exception and self.update_state != UpdateInfoState.COMPLETED:
                raise Exception(
                    f"Pipeline {self.name} update not completed ({self.update_state})"
                )

    def get_update(self):
        self._update = self.wc.pipelines.get_update(
            pipeline_id=self.id, update_id=self.update_id
        )
        return self._update

    @property
    def update_state(self):
        return self._update.update.state

    @property
    def update_id(self) -> str:
        return self._update_start.update_id

    @property
    def update_url(self) -> str:
        return f"{self.wc.config.host}/pipelines/{self.id}/updates/{self.update_id}"
