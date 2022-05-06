import json
import traceback
import config
from core.agent import tone_agent
from core.exception import ExecStepException
from core.dag.plugin import db_operation
from core.dag.plugin.job_complete import JobComplete
from core.cache.remote_source import RemoteFlowSource
from core.server.alibaba.db_operation import AliCloudDbServerOperation as Alc
from models.server import TestServerSnapshot
from scheduler.job import get_test_class
from constant import StepStage, ExecState, ServerFlowFields, ServerProvider
from models.dag import DagStepInstance
from models.job import TestJob, TestStep
from tools.log_util import LoggerFactory
from tools import utils


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class ProcessDagPlugin:
    check_node_exception_timeout = config.CHECK_DAG_STEP_EXCEPTION_TIMEOUT

    @classmethod
    def exec_dag_node(cls, db_node_inst):
        """执行可调度的节点"""
        success_exec, err_msg = False, ""
        meta_data = json.loads(db_node_inst.step_data)
        dag_node_id = db_node_inst.id
        job_id = meta_data[ServerFlowFields.JOB_ID]
        meta_data[ServerFlowFields.DAG_ID] = db_node_inst.dag_id
        meta_data[ServerFlowFields.DAG_STEP_ID] = dag_node_id
        if not meta_data[ServerFlowFields.SERVER_SN]:
            server_snapshot = TestServerSnapshot.get_by_id(meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID])
            meta_data[ServerFlowFields.SERVER_SN] = server_snapshot.sn
        stage = meta_data[ServerFlowFields.STEP]
        test_class = get_test_class(meta_data[ServerFlowFields.TEST_TYPE])
        db_node_inst.state = ExecState.RUNNING
        db_node_inst.save()
        meta_data = cls.update_server_os_type(meta_data)
        try:
            getattr(test_class, stage)(meta_data)
            cls.update_cloud_server(meta_data)
            RemoteFlowSource.push_pending_dag_node(
                None, RemoteFlowSource.source_with_slave_fingerprint(dag_node_id)
            )
            success_exec = True
        except ExecStepException as error:
            logger.exception("run step has exception: {}".format(error))
            summary_logger.exception("run step has exception: ")
            err_msg = traceback.format_exc(config.TRACE_LIMIT)
            snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
            job_suite_id = meta_data[ServerFlowFields.JOB_SUITE_ID]
            job_case_id = 0
            if stage in StepStage.TEST:
                job_case_id = meta_data.get(ServerFlowFields.JOB_CASE_ID, 0)
            snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
            TestStep.create(
                job_id=job_id,
                state=ExecState.FAIL,
                stage=stage,
                job_suite_id=job_suite_id,
                job_case_id=job_case_id,
                server=snapshot_server_id,
                cluster_id=snapshot_cluster_id,
                dag_step_id=dag_node_id,
                result=error
            )
            test_step = TestStep.get_or_none(dag_step_id=dag_node_id)
            JobComplete.set_job_suite_or_case_state_by_test_step(test_step, stage, ExecState.FAIL)
        finally:
            if not success_exec:
                DagStepInstance.update(
                    state=ExecState.FAIL,
                    remark=err_msg
                ).where(
                    DagStepInstance.id == dag_node_id,
                ).execute()

    @classmethod
    def update_cloud_server(cls, meta_data):
        provider = meta_data[ServerFlowFields.SERVER_PROVIDER]
        job_id = meta_data[ServerFlowFields.JOB_ID]
        dag_id = meta_data[ServerFlowFields.DAG_ID]
        old_server_id = meta_data[ServerFlowFields.SERVER_ID]
        old_snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        if provider == ServerProvider.ALI_CLOUD:
            new_cloud_server = Alc.get_cloud_server(job_id, old_server_id)
            DagStepInstance.update_cloud_server_info(dag_id, new_cloud_server, old_snapshot_server_id)

    @classmethod
    def update_server_os_type(cls, meta_data):
        """
        查询该step对应的机器的os类型，在执行步骤的时候根据os类型选择对应的脚本
        """
        if not meta_data.get(ServerFlowFields.SERVER_OS_TYPE):
            server_os_type = tone_agent.check_server_os_type(meta_data[ServerFlowFields.SERVER_IP])
            meta_data.update({ServerFlowFields.SERVER_OS_TYPE: server_os_type})
        return meta_data

    @classmethod
    def check_prefix_dag_step_by_exception(cls, node):
        """检测running的dag_step，超过一定的时间阈值，仍未创建test_step步骤，
        可能原因是runner进程重启或crash导致，则将此dag_step状态重置为pending"""
        dag_step = DagStepInstance.get_by_id(node)
        state = dag_step.state
        start_tm = dag_step.gmt_created
        test_step = TestStep.get_or_none(dag_step_id=node)
        if (
                state == ExecState.RUNNING and not test_step and
                utils.check_timeout_for_now(start_tm, cls.check_node_exception_timeout)
        ):
            logger.warning(
                f"Check dag node(dag_step_id) state all running, bug test_step is not created,"
                f"reset it's state to pending, node is {node}"
            )
            DagStepInstance.update(state=ExecState.PENDING).where(
                DagStepInstance.id == node,
                DagStepInstance.state == ExecState.RUNNING
            ).execute()

    @classmethod
    def check_node_scheduled_by_depends(cls, depend_node_inst_list):
        """检查当前节点依赖的节点的执行情况，以此检测当前节点是否可以被调度"""
        check_res = []
        test_state_set = set()
        no_test_state_set = set()
        for depend_node_inst in depend_node_inst_list:
            stage = depend_node_inst.stage
            state = depend_node_inst.state
            if stage in StepStage.TEST:
                test_state_set.add(state)
            else:
                no_test_state_set.add(state)

        if no_test_state_set.symmetric_difference({ExecState.SUCCESS}):
            check_res.append(False)
        else:
            check_res.append(True)
        if ExecState.no_end_set & test_state_set:
            check_res.append(False)
        else:
            check_res.append(True)

        return all(check_res)

    @classmethod
    def _check_test_end_node(cls, dag_inst, test_stage_node):
        """通过test阶段的end_node来检测它前面的所有test节点是否都结束，
        以防止用户直接触发的skip或stop最后一个节点，导致任务提前结束(
        正常情况当某条线最后一个节点处于终态时该条线的状态可认为是结束)"""
        all_depend_nodes = dag_inst.depend_nodes_with_all(test_stage_node, set())
        state_set = db_operation.get_state_of_dag_steps(all_depend_nodes)
        if ExecState.no_end_set & state_set:
            return False
        return True

    @classmethod
    def check_dag_end_line(cls, job_id, dag_inst, end_line):
        """检查一个dag上end_line执行情况，end_line按一个job
        里每个server或cluster的执行生命周期作为一条线，
        每条线是该server或cluster执行链上可以认为是终态节点的集合"""
        check_end, check_fail = [], []
        for end_line_key in end_line:
            each_line = end_line[end_line_key]
            is_end, is_fail = cls._check_each_end_line(job_id, dag_inst, each_line)
            check_end.append(is_end)
            check_fail.append(is_fail)
        return check_end, check_fail

    @classmethod
    def _check_each_end_line(cls, job_id, dag_inst, each_line):
        """检查end_line里某一条线上的执行情况"""
        is_end, is_fail, _end_node = False, False, None
        no_test_stage_node = each_line[:-1]
        test_stage_node = each_line[-1]
        no_test_end_node = DagStepInstance.filter(
            DagStepInstance.id.in_(no_test_stage_node),
            DagStepInstance.state.in_(ExecState.no_test_end)
        ).first()
        test_end_node = DagStepInstance.filter(
            DagStepInstance.id == test_stage_node,
            DagStepInstance.state.in_(ExecState.end)
        ).first()
        if test_end_node:
            if not cls._check_test_end_node(dag_inst, test_stage_node):
                test_end_node = None
        _end_node = no_test_end_node or test_end_node

        if _end_node:
            logger.info(f"check dag end line, is_end:True, job_id:{job_id}, dag_node_id:{_end_node.id}")
            is_end = True
            if no_test_end_node:
                is_fail = True
                JobComplete.set_job_case_state_fail_by_node(_end_node)
            if test_end_node and test_end_node.state == ExecState.FAIL:
                is_fail = True
            JobComplete.set_job_suite_state_in_real_time(_end_node.job_suite_id)
            JobComplete.release_server_with_dag_node(job_id, _end_node)
        return is_end, is_fail

    @classmethod
    def check_job_complete(cls, job_id, dag_inst, end_line, complete_check=True):
        """检查job是否完成"""
        is_complete = False
        job_state = TestJob.get_by_id(job_id).state
        check_end, check_fail = cls.check_dag_end_line(job_id, dag_inst, end_line)

        if all(check_end) and complete_check:
            job_state = ExecState.FAIL if any(check_fail) else None
            job_complete_inst = JobComplete(job_id, job_state)
            job_state = job_complete_inst.end_state
            is_complete = job_complete_inst.process()

        return is_complete, job_state
