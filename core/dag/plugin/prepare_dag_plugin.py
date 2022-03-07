import json
import config
import importlib
from copy import deepcopy
from collections import OrderedDict
from core.exception import BuildDagException
from core.dag.plugin.step_plugin.step_before_case import JobStepBeforeCase
from core.dag.plugin.step_plugin.step_in_case import JobStepInCase
from constant import (
    RunMode, StepStage,
    ServerFlowFields,
    ClusterRole,
    SuiteCaseFlag,
    ExecState
)
from core.op_acc_msg import OperationAccompanyMsg
from models.job import TestJob, TestJobSuite, TestJobCase
from models.dag import DagStepInstance
from tools.log_util import LoggerFactory
from tools import utils


alloc_server_module = importlib.import_module(config.ALLOC_SERVER_MODULE)
logger = LoggerFactory.scheduler()


class PrepareDagPlugin:

    def __init__(self, job_id, db_dag_id):
        self.job_id = job_id
        self.db_dag_id = db_dag_id
        self.test_job = None
        self.db_dag_node_inst = None
        self.test_order = None
        self.get_server_success = False
        self.server_exclude_fields = tuple()  # 排除字段
        self.no_ready_dag_node = set()
        self.standalone = set()
        self.cluster = set()
        self.last_case_nodes = set()
        self.server_info = []
        self.steps_before_case = []
        self.custom_depends = []  # example: [(current_node_id, depend_node_id), ...]
        self.no_ready_node_flag = dict()  # 用于记录未准备好的job_case在job_suite中的位置
        self.job_info = dict()
        self.server_case_steps = dict()
        self.server_cluster = dict()
        self.end_line = dict()
        self.cluster_local = dict()
        self.node_index = OrderedDict()
        self.reverse_node_index = OrderedDict()

    def parse_expect_step_by_job(self, expect_steps=None):
        self.test_job = TestJob.get_by_id(self.job_id)
        if expect_steps:
            # 针对更新dag情况的优化，不需要重复去job表中查询构建预期步骤
            self.steps_before_case = expect_steps
        else:
            self.steps_before_case = JobStepBeforeCase(self.test_job).get_expect_steps()
        logger.info(
            f"Steps before case is {self.steps_before_case}, "
            f"job_id<{self.job_id}>, dag_id<{self.db_dag_id}>"
        )

    def parse_servers(self, no_ready_dag_node=None, no_ready_node_flag=None):
        if not no_ready_dag_node:
            self.parse_job_suites()
        else:
            self.parse_no_ready_job_cases(no_ready_dag_node, no_ready_node_flag)

    def parse_job_suites(self):
        job_suites = TestJobSuite.filter(
            job_id=self.job_id).order_by(
            TestJobSuite.priority.desc()).order_by(TestJobSuite.gmt_created.asc())
        for job_suite in job_suites:
            self.parse_job_cases(job_suite)

    def _get_filter_cond_by_server(self, test_suite_id):
        query_with_group_by = TestJobCase.filter(
            job_id=self.job_id,
            test_suite_id=test_suite_id
        ).group_by(
            TestJobCase.run_mode,
            TestJobCase.server_provider,
            TestJobCase.server_object_id,
            TestJobCase.server_tag_id,
            TestJobCase.server_snapshot_id
        )
        filter_cond_by_server = [
            {
                "job_id": self.job_id,
                "test_suite_id": test_suite_id,
                "run_mode": inst.run_mode,
                "server_provider": inst.server_provider,
                "server_object_id": inst.server_object_id,
                "server_tag_id": inst.server_tag_id,
                "server_snapshot_id": inst.server_snapshot_id
            } for inst in query_with_group_by
        ]
        return filter_cond_by_server

    def _parse_job_cases(self, job_suite, job_cases):
        suite_case_count = job_cases.count()
        for index, job_case in enumerate(job_cases):
            suite_case_flag = [None, None]
            if index == 0:
                suite_case_flag[0] = SuiteCaseFlag.FIRST
            if index + 1 == suite_case_count:
                suite_case_flag[1] = SuiteCaseFlag.LAST
            server_info = alloc_server_module.AllocServer(
                self.job_info, job_suite, job_case).get_server_info()
            self._collect_server_info(server_info, job_suite, job_case, suite_case_flag)

    def parse_job_cases(self, job_suite):
        test_suite_id = job_suite.test_suite_id
        # 将suite下的test_conf按照机器分组，并将分组后的不同组机器的过滤条件提取出来
        filter_cond_by_server = self._get_filter_cond_by_server(test_suite_id)
        logger.info(f"Group by server conditions: {filter_cond_by_server}")
        # 按提取出的同一机器上的过滤条件，将一个suite下的test_conf按机器维度切分到1个或多个机器上
        for cond in filter_cond_by_server:
            job_cases = TestJobCase.filter(**cond).order_by(
                TestJobCase.priority.desc()).order_by(TestJobCase.gmt_created.asc())
            self._parse_job_cases(job_suite, job_cases)

    def parse_no_ready_job_cases(self, no_ready_dag_node, no_ready_node_flag):
        job_cases = TestJobCase.filter(
            TestJobCase.id.in_(no_ready_dag_node),
            TestJobCase.state.not_in(ExecState.end)
        ).order_by(
            TestJobCase.priority.desc()
        ).order_by(TestJobCase.gmt_created.asc())
        for job_case in job_cases:
            suite_case_flag = no_ready_node_flag.get(job_case.id)
            job_suite = TestJobSuite.get_or_none(
                test_suite_id=job_case.test_suite_id,
                job_id=self.job_id
            )
            server_info = alloc_server_module.AllocServer(
                self.job_info, job_suite, job_case).get_server_info()
            self._collect_server_info(server_info, job_suite, job_case, suite_case_flag)

    def _collect_server_info(self, server_info, job_suite, job_case, suite_case_flag):
        logger.info(f"Collect server, server_info:{server_info}, job_suite:{job_suite}, "
                    f"job_case:{job_case}, suite_case_flag:{suite_case_flag}")
        if not server_info[ServerFlowFields.READY]:
            self._collect_server_info_by_no_ready(server_info, suite_case_flag)
            return
        if server_info[ServerFlowFields.RUN_MODE] == RunMode.STANDALONE:
            self._collect_server_info_by_standalone(
                server_info, job_suite, job_case, suite_case_flag)
        else:
            self._collect_server_info_by_cluster(
                server_info, job_suite, job_case, suite_case_flag)
        self.get_server_success = True

    def _collect_server_info_by_no_ready(self, server_info, suite_case_flag):
        self.no_ready_dag_node.add(server_info[ServerFlowFields.JOB_CASE_ID])
        self.no_ready_node_flag[server_info[ServerFlowFields.JOB_CASE_ID]] = suite_case_flag

    def _collect_server_info_by_standalone(self, server_info, job_suite, job_case, suite_case_flag):
        server_id = server_info[ServerFlowFields.SERVER_ID]
        if server_id not in self.standalone:
            self.standalone.add(server_id)
            tmp_server_info = deepcopy(server_info)
            tmp_server_info[ServerFlowFields.JOB_ID] = self.job_id
            for se in self.server_exclude_fields:
                tmp_server_info.pop(se)
            self.server_info.append(tmp_server_info)
        steps_in_case = JobStepInCase(
            self.test_job, job_suite, job_case, suite_case_flag).get_case_step()
        if server_id in self.server_case_steps:
            self.server_case_steps[server_id].extend(steps_in_case)
        else:
            self.server_case_steps[server_id] = steps_in_case

    def _collect_server_info_by_cluster(self, server_info, job_suite, job_case, suite_case_flag):
        local_server = None
        cluster_id = server_info[ServerFlowFields.CLUSTER_ID]
        if cluster_id not in self.cluster:
            self.cluster.add(cluster_id)
            _server_info = server_info.copy()
            servers = _server_info.pop(ServerFlowFields.CLUSTER_SERVERS)
            remote_servers = deepcopy(servers)
            for server in servers:
                server_id = server[ServerFlowFields.SERVER_ID]
                self.server_cluster[server_id] = cluster_id
                self.standalone.add(server_id)
                if server[ServerFlowFields.ROLE] == ClusterRole.LOCAL:
                    local_server = server
                    self.cluster_local[cluster_id] = local_server
                    remote_servers.remove(server)
                    server[ClusterRole.REMOTE] = []
                    for remote_server in remote_servers:
                        remote_server.update(_server_info)
                        server[ClusterRole.REMOTE].append(remote_server)
                server.update(_server_info)
                self.server_info.append(server)
        else:
            local_server = self.cluster_local[cluster_id]
        # 只有local角色的server作为主控机跑case
        if not local_server:
            raise BuildDagException(f"Cluster server not has local role! "
                                    f"cluster_id:{cluster_id}, job_id:{self.job_id}")
        server_id = local_server[ServerFlowFields.SERVER_ID]
        steps_in_case = JobStepInCase(self.test_job, job_suite, job_case, suite_case_flag).get_case_step()
        if server_id in self.server_case_steps:
            self.server_case_steps[server_id].extend(steps_in_case)
        else:
            self.server_case_steps[server_id] = steps_in_case

    def prepare_data(self, no_ready_dag_node, no_ready_node_flag, expect_steps):
        self.parse_expect_step_by_job(expect_steps)
        self.parse_servers(no_ready_dag_node, no_ready_node_flag)

    @staticmethod
    def _build_step_data(*data_items) -> str:
        step_data = dict()
        for da in data_items:
            step_data.update(da)
        step_data = json.dumps(step_data)
        return step_data

    @staticmethod
    def check_kernel_install(server, expect_step):
        """主要针对集群中每个server定义的是否安装内核配置进行校验"""
        if (ServerFlowFields.KERNEL_INSTALL in server and
                expect_step[ServerFlowFields.STEP] in StepStage.KERNEL_INSTALL_SET):
            if not server[ServerFlowFields.KERNEL_INSTALL]:
                return False
        return True

    def build_index(self):
        self._build_index_before_test_case()
        self._build_index_in_test_case()

    def _build_index_before_test_case(self):
        """Index the combination of the machine and the expected steps
        in the machine dimension, exclude test cases"""
        for expect_step in self.steps_before_case:
            for server in self.server_info:
                if self.check_kernel_install(server, expect_step):
                    step_data = self._build_step_data(server, expect_step)
                    self.create_dag_node_in_db(expect_step[ServerFlowFields.STEP], step_data)
                    server_idx = self.server_info.index(server)
                    expect_step_idx = self.steps_before_case.index(expect_step)
                    self._build_index(f"{server_idx}_{expect_step_idx}")
                    self._build_end_line(server[ServerFlowFields.SERVER_ID])

    def _build_index_in_test_case(self):
        """Index the combination of machine and test case with
        machine as dimension"""
        for server in self.server_info:
            if ServerFlowFields.ROLE in server and server[ServerFlowFields.ROLE] != ClusterRole.LOCAL:
                # 集群里非local角色的机器不直接参与test步骤
                continue
            job_cases = self.server_case_steps[server[ServerFlowFields.SERVER_ID]]
            for job_case in job_cases:
                stage = job_case[ServerFlowFields.STEP]
                step_data = self._build_step_data(server, job_case)
                self.create_dag_node_in_db(stage, step_data)
                # 建立local角色第一个case步骤与集群内所有机器
                # 非case步骤的依赖关系
                if (
                        ServerFlowFields.ROLE in server and
                        server[ServerFlowFields.ROLE] == ClusterRole.LOCAL and
                        job_cases.index(job_case) == 0
                ):
                    self._build_cluster_depend_before_case(server)
                # 将每个server最后一个跑的case放入last_case_nodes集合中
                if job_case == job_cases[-1]:
                    self.last_case_nodes.add(self.db_dag_node_inst.id)
                server_idx = self.server_info.index(server)
                job_case_idx = len(self.steps_before_case) + job_cases.index(job_case)
                self._build_index(f"{server_idx}_{job_case_idx}")
                if (
                        stage in StepStage.JOB_END_POINT_WITH_SUITE or
                        job_case == job_cases[-1]
                ):
                    self._build_end_line(server[ServerFlowFields.SERVER_ID])

    def _build_cluster_depend_before_case(self, local_server_info):
        # 在测试阶段前建立好集群主控机和其它机器依赖关系，
        # 相当于所有机器的执行步骤合到主控机上执行(主要用于case的执行)
        remote_servers = local_server_info[ClusterRole.REMOTE]
        for rs in remote_servers:
            first_idx = self.server_info.index(rs)
            second_idx = len(self.steps_before_case) - 1
            rs_idx = f"{first_idx}_{second_idx}"
            rs_node = self.reverse_node_index[rs_idx]
            self.custom_depends.append((self.db_dag_node_inst.id, rs_node))

    def _build_index(self, index_key):
        """Build the database node positive
        and negative index of the graph"""
        self.node_index[self.db_dag_node_inst.id] = index_key
        self.reverse_node_index[index_key] = self.db_dag_node_inst.id

    def create_dag_node_in_db(self, stage, step_data):
        self.db_dag_node_inst = DagStepInstance.create(
            dag_id=self.db_dag_id,
            stage=stage,
            step_data=step_data
        )

    def _build_end_line(self, server_id):
        """Build the end line, which is a collection of
         possible end points for each line abstracting
         from the graph."""
        if server_id in self.server_cluster:
            cluster_id = self.server_cluster[server_id]
            end_line_key = f"{RunMode.CLUSTER}_{cluster_id}"
        else:
            end_line_key = f"{RunMode.STANDALONE}_{server_id}"

        self._build_end_line_with_end_key(end_line_key)

    def _build_end_line_with_end_key(self, end_line_key):
        if end_line_key in self.end_line:
            self.end_line[end_line_key].append(self.db_dag_node_inst.id)
        else:
            self.end_line[end_line_key] = [self.db_dag_node_inst.id]

    @staticmethod
    def previous_node_index(node_idx):
        node_idx = node_idx.split("_")
        node_idx = list(map(int, node_idx))
        pre_second_idx = node_idx[1] - 1
        if pre_second_idx < 0:
            return None
        return f"{node_idx[0]}_{pre_second_idx}"

    def prepare_graph_data(self, no_read_dag_node=None, no_ready_node_flag=None, expect_steps=None):
        self.prepare_data(no_read_dag_node, no_ready_node_flag, expect_steps)
        self.build_index()

    def update_job_state(self):
        if self.standalone and self.test_job.state == ExecState.PENDING_Q:
            self.test_job.state = ExecState.RUNNING
            self.test_job.start_time = utils.get_now()
            self.test_job.save()
            OperationAccompanyMsg.send_msg_with_job_running(self.test_job.id)
