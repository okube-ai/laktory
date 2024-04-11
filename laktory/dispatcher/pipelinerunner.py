from laktory.dispatcher.runner import Runner
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineRunner(Runner):

    def get_id(self):
        logger.info(f"Getting id for pipeline {self.name}")
        for _pl in self.wc.pipelines.list_pipelines(filter=f"name LIKE '{self.name}'", max_results=1):
            if self.name == _pl.name:
                self.id = _pl.pipeline_id
                break
