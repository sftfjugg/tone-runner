from core.exception import UpdateDagException, JobEndException
from core.dag.plugin.prepare_dag_plugin import PrepareDagPlugin
from core.dag.plugin.job_complete import JobComplete
from tools.log_util import LoggerFactory
from .dag_factory import DagFactory
from .dag_factory import BaseGenerateDag
from .build_dag import UpdateDag


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class UpdateJobDagFactory(DagFactory):

    def __init__(self, job_id):
        self.job_id = job_id

    def object_factory(self):
        return JobUpdateDag(self.job_id)


class JobUpdateDag(BaseGenerateDag, UpdateDag):

    def __init__(self, job_id):
        super(JobUpdateDag, self).__init__(job_id)
        self.prepare_dag_plugin = PrepareDagPlugin

    def build(self):
        try:
            JobComplete.check_job_end(self.job_id)
            self.update_dag()
            return self.db_dag_id
        except JobEndException as error:
            raise JobEndException(error)
        except Exception as error:
            logger.exception("Update dag exception:")
            summary_logger.exception(f"Update dag exception: {error}")
            raise UpdateDagException(error)
