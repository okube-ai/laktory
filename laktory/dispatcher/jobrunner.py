from laktory.dispatcher.runner import Runner
from laktory._logger import get_logger

logger = get_logger(__name__)


class JobRunner(Runner):

    def get_id(self):
        logger.info(f"Getting id for job {self.name}")
        for _job in self.wc.jobs.list(name=self.name):
            _job_data = _job.as_dict()
            if self.name == _job_data["settings"]["name"]:
                self.id = _job.job_id
                break
