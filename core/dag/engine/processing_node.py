from core.exception import (
    ProcessDagNodeException,
    JobEndException,
    DagNodeEndException,
    ExecChanelException
)
from core.dag.plugin.process_node_plugin import ProcessDagNodePlugin
from core.dag.plugin.job_complete import JobComplete
from constant import ExecState
from models.dag import DagStepInstance
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class ProcessingNode:

    process_dag_node_plugin = ProcessDagNodePlugin

    def __init__(self, node_id):
        self.node_id = node_id
        self.is_complete = False

    def check_node(self):
        logger.info(f"now check dag node<{self.node_id}> state.")
        self.process_dag_node_plugin.check_step(self.node_id)

    def check_node_state(self):
        dag_node_inst = DagStepInstance.get_by_id(self.node_id)
        node_state = dag_node_inst.state
        if node_state in ExecState.end:
            logger.info(
                f"Node {self.node_id} has completed "
                f"and the state is {node_state}"
            )
            self.is_complete = True

    def run(self):
        try:
            JobComplete.check_job_end(dag_node_id=self.node_id)
            JobComplete.check_dag_node_end(self.node_id)
            self.check_node()
            self.check_node_state()
            return self.is_complete
        except ExecChanelException as error:
            logger.error(error)
        except JobEndException as error:
            raise JobEndException(error)
        except DagNodeEndException as error:
            raise DagNodeEndException(error)
        except Exception as error:
            logger.exception("Processing dag node exception:")
            summary_logger.exception(f"Processing dag node exception: {error}")
            raise ProcessDagNodeException(error)
