from core.exception import GenerateDagException, JobEndException, DagExistsException
from core.dag.plugin.prepare_dag_plugin import PrepareDagPlugin
from core.dag.plugin.job_complete import JobComplete
from tools.log_util import LoggerFactory
from .dag_factory import DagFactory, BaseGenerateDag
from .build_dag import BuildDag


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class GeneralJobDagFactory(DagFactory):

    def __init__(self, job_id):
        self.job_id = job_id

    def object_factory(self):
        return JobGenerateDag(self.job_id)


class JobGenerateDag(BaseGenerateDag, BuildDag):

    def __init__(self, job_id):
        super(JobGenerateDag, self).__init__(job_id)
        self.prepare_dag_plugin = PrepareDagPlugin

    def build(self):
        try:
            JobComplete.check_job_end(self.job_id)
            self.build_dag()
            return self.db_dag_id
        except JobEndException as error:
            raise JobEndException(error)
        except DagExistsException as error:
            raise DagExistsException(error)
        except Exception as error:
            logger.exception("Build dag exception:")
            summary_logger.exception(f"Build dag exception: {error}")
            raise GenerateDagException(error)
