import pickle
from collections import OrderedDict
from core.exception import DagExistsException
from models.base import db
from models.dag import Dag as DagModel
from tools.log_util import LoggerFactory
from .base_dag import Dag


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class BuildDag:

    def __init__(self, job_id):
        self.job_id = job_id
        self.prepare_dag_plugin = None
        self.previous_node_index_method = None
        self.prepare_dag_plugin_inst = None
        self.dag_required_update = False
        self.dag_inst = None
        self.db_dag_id = None
        self.db_dag_inst = None
        self.db_dag_node_inst = None
        self.old_no_ready_dag_node = None
        self.old_no_ready_node_flag = None
        self.no_ready_dag_node = set()
        self.no_ready_node_flag = dict()
        self.expect_steps = []
        self.custom_depends = []
        self.end_line = dict()
        self.node_index = OrderedDict()
        self.reverse_node_index = OrderedDict()
        self.dag_graph = OrderedDict()

    def init_data(self):
        self.init_dag()
        self.prepare_graph_data()

    def init_dag(self):
        try:
            if not DagModel.filter(job_id=self.job_id).exists():
                self.db_dag_inst = DagModel.create(job_id=self.job_id)
                self.db_dag_id = self.db_dag_inst.id
                logger.info(
                    f"Now create db dag when find process job, "
                    f"job_id<{self.job_id}>, dag_id<{self.db_dag_inst.id}>"
                )
            else:
                raise DagExistsException(f"There is already a DAG for this job, job_id: {self.job_id}.")
        except Exception as error:
            logger.exception(error)
            summary_logger.exception(error)
            raise DagExistsException(f"There is already a DAG for this job, job_id: {self.job_id}.")

    def prepare_graph_data(self, old_no_ready_dag_node=None, old_no_ready_node_flag=None, expect_steps=None):
        self.prepare_dag_plugin_inst = self.prepare_dag_plugin(
            self.job_id,
            self.db_dag_id
        )
        self.prepare_dag_plugin_inst.prepare_graph_data(old_no_ready_dag_node, old_no_ready_node_flag, expect_steps)
        self.no_ready_dag_node = self.prepare_dag_plugin_inst.no_ready_dag_node
        self.no_ready_node_flag = self.prepare_dag_plugin_inst.no_ready_node_flag
        self.end_line = self.prepare_dag_plugin_inst.end_line
        self.node_index = self.prepare_dag_plugin_inst.node_index
        self.reverse_node_index = self.prepare_dag_plugin_inst.reverse_node_index
        self.custom_depends = self.prepare_dag_plugin_inst.custom_depends
        self.previous_node_index_method = self.prepare_dag_plugin_inst.previous_node_index

    def add_node(self):
        for node in self.node_index:
            self.dag_graph[node] = set()

    def add_edge(self):
        """self.node_index: {node_id: index}
           self.reverse_node_index: {index: node_id}
           Reference: the method of _build_index
        """
        for node in self.node_index:
            node_idx = self.node_index[node]
            pre_node_idx = self.previous_node_index_method(node_idx)
            if pre_node_idx:
                pre_node = self.reverse_node_index.get(pre_node_idx)
                if pre_node:
                    self.dag_graph[pre_node].add(node)
        # 自定义依赖关系目前只用于集群的特殊情况处理
        for depend in self.custom_depends:
            current_node, depend_node = depend
            self.dag_graph[depend_node].add(current_node)

    def store_dag(self):
        """Store the DAG to the database"""
        self.dag_inst = Dag(
            self.dag_graph,
            no_ready_dag_node=self.no_ready_dag_node,
            no_ready_node_flag=self.no_ready_node_flag,
            end_line=self.end_line
        )
        self.db_dag_inst.graph = pickle.dumps(self.dag_inst)
        if self.prepare_dag_plugin_inst.no_ready_dag_node:
            self.db_dag_inst.is_update = True
        if self.prepare_dag_plugin_inst.get_server_success:
            self.db_dag_inst.block = False
        self.db_dag_inst.save()
        self.prepare_dag_plugin_inst.update_job_state()
        logger.info(
            f"Store the DAG to the database, "
            f"job_id: {self.job_id}, dag_id:{self.db_dag_id}, "
            f"dag_graph: {self.dag_graph}, end_line: {self.end_line}, "
            f"no_ready_dag_node: {self.no_ready_dag_node}, "
            f"no_ready_node_flag: {self.no_ready_node_flag}, "
            f"dag_is_update: {self.db_dag_inst.is_update}"
        )

    @db.atomic()
    def build_dag(self):
        self.init_data()
        self.add_node()
        self.add_edge()
        self.store_dag()


class UpdateDag(BuildDag):

    def init_data(self):
        self.no_ready_dag_node = set()
        self.db_dag_inst = DagModel.get_or_none(job_id=self.job_id)
        self.db_dag_id = self.db_dag_inst.id
        self.dag_inst = pickle.loads(self.db_dag_inst.graph)
        self.old_no_ready_dag_node = self.dag_inst.kwargs.get("no_ready_dag_node")
        self.old_no_ready_node_flag = self.dag_inst.kwargs.get("no_ready_node_flag")
        self.expect_steps = self.dag_inst.kwargs.get("expect_steps")
        self.prepare_graph_data(self.old_no_ready_dag_node, self.old_no_ready_node_flag, self.expect_steps)
        logger.info(f"Now update dag for job<{self.job_id}>, dag_id: {self.db_dag_id}")

    def update_dag_inst(self):
        if self.dag_graph:
            logger.info(
                f"Update dag graph, job_id: {self.job_id}, "
                f"dag_id:{self.db_dag_id}, new_added_graph: {self.dag_graph}"
            )
            self.dag_inst.graph.update(self.dag_graph)
        if self.end_line:
            logger.info(
                f"Update dag end_line, job_id: {self.job_id}, "
                f"dag_id: {self.db_dag_id}, new_added_end_line: {self.end_line}"
            )
            self.dag_inst.kwargs["end_line"].update(self.end_line)
        if self.no_ready_dag_node:
            logger.info(
                f"Newest no ready dag node is {self.no_ready_dag_node}, "
                f"job_id:{self.job_id}, dag_id: {self.db_dag_id}"
            )
            self.dag_inst.kwargs["no_ready_dag_node"] = self.no_ready_dag_node
        if self.no_ready_node_flag:
            logger.info(
                f"Newest no ready node flag is {self.no_ready_node_flag}, "
                f"job_id:{self.job_id}, dag_id: {self.db_dag_id}"
            )
            self.dag_inst.kwargs["no_ready_node_flag"] = self.no_ready_node_flag

    def update_db_dag_inst(self):
        if not self.no_ready_dag_node:
            logger.info(
                f"Job dag is updating, job_id: {self.job_id}, "
                f"dag_id: {self.db_dag_id}, is_update: False"
            )
            self.db_dag_inst.is_update = False
            self.db_dag_inst.save()
        if self.old_no_ready_dag_node != self.no_ready_dag_node:
            logger.info(f"Update db dag inst, job_id: {self.job_id}, dag_id:{self.db_dag_id}")
            self.db_dag_inst.graph = pickle.dumps(self.dag_inst)
            self.db_dag_inst.save()
        self.prepare_dag_plugin_inst.update_job_state()

    @db.atomic()
    def update_dag(self):
        self.init_data()
        self.add_node()
        self.add_edge()
        self.update_dag_inst()
        self.update_db_dag_inst()
