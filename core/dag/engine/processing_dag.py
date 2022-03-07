import pickle
from core.exception import ProcessDagException, JobEndException
from core.dag.plugin.process_dag_plugin import ProcessDagPlugin
from core.dag.plugin.job_complete import JobComplete
from tools.log_util import LoggerFactory
from constant import ExecState, DbField
from models.dag import Dag as DagModel, DagStepInstance
from .base_dag import Dag


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class ProcessingDag:
    process_dag_plugin = ProcessDagPlugin

    def __init__(self, dag_id, update_dag_inst=None):
        self.job_id = None
        self.dag_id = dag_id
        self.update_dag_inst = update_dag_inst
        self.dag_inst = None
        self.db_dag_inst = None
        self.wait_scheduler_nodes = set()
        self.is_update = False
        self.block = False
        self.is_complete = False

    def _get_dag_inst(self):
        self.db_dag_inst = DagModel.get_by_id(self.dag_id)
        self.job_id = self.db_dag_inst.job_id
        self.dag_inst = pickle.loads(self.db_dag_inst.graph)
        assert isinstance(self.dag_inst, Dag), "dag_inst is not an instance of Dag"
        self.is_update = self.db_dag_inst.is_update
        JobComplete.check_job_end(self.job_id)

    def update_dag(self):
        self.block = DbField.FALSE
        if self.is_update:
            logger.info(f"update dag<{self.dag_id}>, job_id: {self.job_id}.")
            self.update_dag_inst.build()
            db_dag = DagModel.get_by_id(self.dag_id)
            if db_dag.is_update:
                if not DagStepInstance.filter(
                        DagStepInstance.dag_id == self.dag_id,
                        DagStepInstance.state.in_(ExecState.no_end_set)
                ).exists():
                    self.block = DbField.TRUE
        DagModel.update(block=self.block).where(DagModel.id == self.dag_id).execute()

    def parse_nodes(self):
        self.wait_scheduler_nodes.clear()
        list_nodes = self.dag_inst.list_nodes()
        scheduled_nodes = {
            node.id for node in
            DagStepInstance.filter(
                DagStepInstance.state != ExecState.PENDING,
                DagStepInstance.dag_id == self.db_dag_inst.id
            )
        }
        nearest_wait_scheduled_nodes = self.dag_inst.nearest_scheduler_nodes(scheduled_nodes)
        logger.info(f"\ndag<{self.dag_id}> dag_graph is: {self.dag_inst.graph}\n"
                    f"list_nodes is: {list_nodes};\nscheduled_nodes is: {scheduled_nodes}\n")
        for node in nearest_wait_scheduled_nodes:
            self._check_node_scheduled(node)

    def produce_exec_node(self):
        for node_id in self.wait_scheduler_nodes:
            db_node_inst = DagStepInstance.get_or_none(id=node_id)
            if db_node_inst:
                self.process_dag_plugin.exec_dag_node(db_node_inst)
            else:
                logger.error(f"the node<{node_id}> of dag<{self.dag_id}> does not exists!")

    def _check_node_scheduled(self, node):
        if not JobComplete.check_node_stop_or_skip_by_user(node):
            # 当前依赖的节点链，防止skip操作时只检测上一层不准确的问题
            depend_nodes = self.dag_inst.depend_nodes_with_all(node, set())
            if not depend_nodes:
                self.wait_scheduler_nodes.add(node)
            else:
                for depend_node in depend_nodes:
                    self.process_dag_plugin.check_prefix_dag_step_by_exception(depend_node)
                    JobComplete.check_node_stop_or_skip_by_user(depend_node, True)
                depend_node_inst_list = DagStepInstance.filter(DagStepInstance.id.in_(depend_nodes))
                if self.process_dag_plugin.check_node_scheduled_by_depends(depend_node_inst_list):
                    self.wait_scheduler_nodes.add(node)

    def check_dag_complete(self):
        if self.db_dag_inst.is_update:
            end_line = self.dag_inst.kwargs.get("end_line", dict())
            no_ready_dag_node = self.dag_inst.kwargs["no_ready_dag_node"]
            logger.info(f"dag<{self.dag_id}> has no_ready_dag_node for job_case_id: "
                        f"{no_ready_dag_node}, end_line: {end_line}")
            self.process_dag_plugin.check_job_complete(
                self.job_id, self.dag_inst, end_line, complete_check=False)
        else:
            end_line = self.dag_inst.kwargs.get("end_line")
            logger.info(f"dag<{self.dag_id}> end_line: {end_line}")
            self.is_complete, job_state = self.process_dag_plugin.check_job_complete(
                self.job_id, self.dag_inst, end_line)
            if self.is_complete:
                logger.info(f"dag<{self.dag_id}> is completed and job<{self.job_id}> status is {job_state}")

    def run(self):
        try:
            self._get_dag_inst()
            self.update_dag()
            self.parse_nodes()
            self.produce_exec_node()
            self.check_dag_complete()
            return self.is_complete, self.block
        except JobEndException as error:
            raise JobEndException(error)
        except Exception as error:
            logger.exception("Processing dag exception:")
            summary_logger.exception(f"Processing dag exception: {error}")
            raise ProcessDagException(error)
