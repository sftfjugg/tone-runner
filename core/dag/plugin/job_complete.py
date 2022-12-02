import importlib
import json
import operator
from functools import reduce

import config
import re
import datetime
from core.exception import (
    JobEndException,
    DagNodeEndException,
    ExecChanelException,
    JobNotExistsException
)
from core.cache.remote_source import (
    RemoteFlowSource,
    RemoteAllocServerSource,
    RemoteReleaseServerSource
)
from core.client.goldmine_monitor import GoldmineMonitor
from core.dag.plugin import db_operation
from core.exec_channel import ExecChannel
from core.op_acc_msg import OperationAccompanyMsg as Oam
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from models.server import TestServerSnapshot, CloudServerSnapshot, TestServer, CloudServer, TestCluster
from models.job import MonitorInfo
from scheduler.job.base_test import BaseTest
from constant import (
    ExecState,
    StepStage,
    DbField,
    RunMode,
    JobCfgFields,
    MonitorParam,
    QueueName,
    ServerAttribute,
    ServerFlowFields, RunStrategy, ServerProvider, ServerState, ProcessDataSource
)
from models.base import db
from models.job import TestJob, TestJobSuite, TestJobCase, TestStep
from models.dag import Dag, DagStepInstance
from tools import utils
from tools.log_util import LoggerFactory
from tools.redis_cache import redis_cache
from constant import OtherCache

alloc_server_module = importlib.import_module(config.ALLOC_SERVER_MODULE)
logger = LoggerFactory.job_complete()
summary_logger = LoggerFactory.summary_error_log()


class JobComplete:

    def __init__(self, job_id, state=None, err_msg=DbField.EMPTY):
        self.job_id = job_id
        self.job = TestJob.filter(
            TestJob.id == job_id,
            TestJob.is_deleted.in_([DbField.TRUE, DbField.FALSE])
        ).first()
        self.dag = Dag.get_or_none(job_id=job_id)
        self.dag_id = self.dag.id if self.dag else None
        self.job_deleted = self.job.is_deleted if self.job else DbField.TRUE
        self.end_state = self._get_end_state(state)
        self.err_msg = err_msg
        self.complete_flag = "job process is completed."
        self.SOURCE_STORE = redis_cache

    def _get_end_state(self, state):
        end_state, state_from = None, "job self"
        if self.job:
            end_state = self.job.state
        if end_state not in ExecState.end:
            if state:
                end_state, state_from = state, "external args"
            elif self.job_deleted:
                end_state = ExecState.STOP
            else:
                state_from = "all suite with real-time computing"
                end_state = db_operation.get_job_state_by_all_suite(self.job_id)
        logger.info(
            f"get end state by {state_from}, job_id: {self.job_id}, end_state: {end_state}"
        )
        return end_state

    @staticmethod
    def check_job_end(job_id=None, dag_id=None, dag_node_id=None):
        assert any([job_id, dag_id, dag_node_id]) is True, \
            "job_id or dag_id or dag_node_id can't be empty at the same time"
        if dag_id:
            job_id = Dag.get_by_id(dag_id).job_id
        elif dag_node_id:
            job_id = DagStepInstance.get_by_id(dag_node_id).job_id
        JobComplete._check_job_end(job_id)

    @staticmethod
    def _check_job_end(job_id=None, dag_id=None):
        if dag_id:
            job_id = Dag.get_by_id(dag_id).job_id
        if not db_operation.exists_job(job_id):
            raise JobNotExistsException(f"Job is not exists! job_id:{job_id}.")
        if db_operation.check_job_end(job_id):
            raise JobEndException(f"Job is end, job_id:{job_id}.")

    @staticmethod
    def check_dag_node_end(dag_node_id):
        if db_operation.check_dag_node_end(dag_node_id):
            err_msg = f"The dag node<{dag_node_id}> is complete, you don't have to deal with it!"
            logger.error(err_msg)
            raise DagNodeEndException(err_msg)

    def set_job_state(self):
        if self.job:
            logger.info(f"set job state, job_id: {self.job_id}, state: {self.end_state}")
            nowt = utils.get_now()
            self.job.state = self.end_state
            test_result = BaseTest.get_job_result(self.job_id, self.job.test_type)
            self.job.test_result = json.dumps(test_result)
            self.job.end_time = nowt
            if not self.job.start_time:
                self.job.start_time = nowt
            self.job.state_desc = self.err_msg + "\n" * 3 + self.complete_flag
            self.job.save()
        if self.dag:
            self.dag.save()

    def set_dag_node_state_by_job(self):
        if self.dag_id:
            logger.info(f"set dag_node state when job done, job_id: {self.job_id}, dag_id: {self.dag_id}")
            no_end_dag_node_ids = db_operation.get_dag_node_ids_with_no_end(self.dag_id)
            self.stop_steps_by_job_stop()
            db_operation.set_dag_node_state_by_job(self.dag_id, self.end_state, self.err_msg)
            db_operation.set_test_step_state_by_job(self.job_id, self.end_state)
            self.release_job_server(no_end_dag_node_ids)

    def release_server_by_job_deleted_or_exception(self):
        """主要针对指定机器的job创建好后分配机器还未就绪的情况就被删除
        或build dag异常的极端场景，指定机器的spec_use状态未能被正确更新的问题"""
        spec_server_job_cases = TestJobCase.select(
            TestJobCase.server_object_id,
            TestJobCase.server_provider,
            TestJobCase.run_mode,
            TestJobCase.test_suite_id
        ).filter(
            TestJobCase.job_id == self.job_id,
            TestJobCase.server_object_id.is_null(is_null=False),
            TestJobCase.state.not_in(ExecState.end)
        )
        for se in spec_server_job_cases:
            server_object_id = se.server_object_id
            server_provider = se.server_provider
            run_mode = se.run_mode
            run_strategy = self.get_run_strategy(se)
            job_suite_id = TestJobSuite.get(test_suite_id=se.test_suite_id, job_id=self.job_id).id
            self.job_release_server(
                self.job_id, server_provider, run_mode, job_suite_id, server_object_id, run_strategy
            )
        # 集团内机器，非机器池机器释放
        if self.job.server_provider == 'aligroup':
            clauses = [
                (TestJobCase.server_tag_id.is_null(is_null=True)),
                (TestJobCase.server_tag_id == ''),
            ]
            spec_server_job_cases = TestJobCase.select(
                TestJobCase.server_object_id,
                TestJobCase.server_provider,
                TestJobCase.run_mode,
                TestJobCase.test_suite_id,
                TestJobCase.server_snapshot_id,
                TestJobCase.server_tag_id
            ).filter(
                TestJobCase.job_id == self.job_id,
                reduce(operator.or_, clauses)
            )
            server_sn = None
            for se in spec_server_job_cases:
                server_snapshot_id = se.server_snapshot_id
                server_object_id = se.server_object_id
                if server_snapshot_id:
                    server_snapshot = TestServerSnapshot.filter(id=server_snapshot_id).first()
                    if server_snapshot:
                        server_sn = server_snapshot.server_sn
                elif server_object_id:
                    server = TestServer.filter(id=server_snapshot_id).first()
                    if server:
                        server_sn = server.server_sn
                if server_sn:
                    is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
                    if is_using and str(using_id) == str(self.job_id):
                        RemoteAllocServerSource.remove_server_from_using_cache(server_sn)

    @staticmethod
    def get_run_strategy(test_job_case):
        if test_job_case.server_tag_id:
            run_strategy = RunStrategy.TAG
        elif test_job_case.server_object_id:
            run_strategy = RunStrategy.SPEC
        elif test_job_case.server_snapshot_id:
            run_strategy = RunStrategy.SPEC
        else:
            run_strategy = RunStrategy.RAND
        return run_strategy

    @classmethod
    def set_job_case_state_fail_by_node(cls, dag_node):
        """当非测试阶段步骤(测试准备步骤如reboot、prepare等)失败时，
        将当前机器相关的case全部置为失败"""
        dag_steps = DagStepInstance.filter(dag_id=dag_node.dag_id, stage=StepStage.RUN_CASE)
        for dag_step in dag_steps:
            if dag_step.server_id == dag_node.server_id:
                logger.info(f"server's({dag_step.server_ip}) step before run_case failed, "
                            f"now set job case{dag_step.job_case_id} state to fail")
                if dag_step.id != dag_node.id:
                    dag_step.state = ExecState.SKIP
                    dag_step.save()
                cls.set_job_case_state_when_no_test_fail(dag_step.job_case_id)

    @staticmethod
    def set_job_case_state_when_no_test_fail(job_case_id):
        logger.info(
            f"Update job_case state fail when no test fail, "
            f"job_case_id is: {job_case_id}"
        )
        TestJobCase.update(state=ExecState.FAIL, end_time=utils.get_now()).where(
            TestJobCase.id == job_case_id,
            TestJobCase.state.not_in(ExecState.end)
        ).execute()

    @staticmethod
    def set_job_suite_state_by_test_step(test_step, stage, state):
        if stage in StepStage.SUITE_SET:
            job_suite_id = test_step.job_suite_id
            job_suite = TestJobSuite.get_by_id(job_suite_id)
            job_id, test_suite_id = job_suite.job_id, job_suite.test_suite_id
            job_suite_state = job_suite.state
            if job_suite_state not in ExecState.end:
                if stage in StepStage.SUITE_END_POINT and state == ExecState.FAIL:
                    end_time = utils.get_now()
                    logger.info(
                        f"Now update job_case state, job_id: {job_id}, "
                        f"test_suite_id: {test_suite_id}, state: {state}"
                    )
                    db_operation.set_job_case_state_by_suite(job_id, test_suite_id, state)
                    logger.info(
                        f"Now update job_suite state, job_id:{job_id}, "
                        f"job_suite_id:{job_suite_id}, state: {state}"
                    )
                    job_suite.state = state
                    if end_time:
                        job_suite.end_time = end_time
                    job_suite.save()

    @staticmethod
    def set_job_case_state_by_test_step(test_step, stage, state, server_broken=None):
        stage_list = StepStage.STEPS if server_broken else StepStage.ONE_CASE_SET
        if stage in stage_list:
            job_case_id = test_step.job_case_id
            job_id = test_step.job_id
            job_case = TestJobCase.get_by_id(job_case_id)
            job_case_state = job_case.state
            if job_case_state not in ExecState.end:
                if state == ExecState.FAIL or (stage == StepStage.RUN_CASE and state in ExecState.end):
                    end_time = utils.get_now()
                    logger.info(
                        f"Now update job_case state, job_id:{job_id}, "
                        f"job_case_id:{job_case_id}, state: {state}"
                    )
                    job_case.state = state
                    if end_time:
                        job_case.end_time = end_time
                    job_case.save()

    @staticmethod
    def set_job_suite_or_case_state_by_test_step(test_step, stage, state):
        try:
            JobComplete.set_job_case_state_by_test_step(test_step, stage, state)
            JobComplete.set_job_suite_state_by_test_step(test_step, stage, state)
        except Exception as error:
            logger.exception(error)
            summary_logger.exception(error)

    @staticmethod
    def set_job_state_by_test_step(job_id, state):
        if not TestJobCase.filter(TestJobCase.job_id == job_id, TestJobCase.state.in_(ExecState.no_end_set)).exists():
            TestJob.update(state=state, end_time=utils.get_now()).where(id=job_id).execute()

    @staticmethod
    def set_job_suite_state_in_real_time(job_suite_id):
        job_suite = TestJobSuite.get_by_id(job_suite_id)
        job_id, test_suite_id = job_suite.job_id, job_suite.test_suite_id
        job_cases = TestJobCase.select(TestJobCase.state).filter(
            job_id=job_id, test_suite_id=test_suite_id
        )
        job_case_states = {job_case.state for job_case in job_cases}
        has_no_end_job_case = ExecState.no_end_set & job_case_states
        if has_no_end_job_case:
            return
        if job_suite.state not in ExecState.end:
            if ExecState.SUCCESS in job_case_states and ExecState.FAIL not in job_case_states:
                state = ExecState.SUCCESS
            elif ExecState.FAIL in job_case_states:
                state = ExecState.FAIL
            elif not (job_case_states - {ExecState.STOP}):
                state = ExecState.STOP
            else:
                state = ExecState.SKIP
            job_suite.state = state
            job_suite.end_time = utils.get_now()
            job_suite.save()
            logger.info(f"set job_suite state when job_suite done, job_id: {job_id}, "
                        f"job_suite_id: {job_suite_id}, state: {state}")

    @classmethod
    def check_node_stop_or_skip_by_user(cls, node, clear_que=False):
        """检测节点(job_suite/job_case)由用户主动触发的停止或skip操作"""
        check_res = False
        dag_step = DagStepInstance.get_by_id(node)
        stage = dag_step.stage
        if stage in StepStage.TEST:
            state = None
            job_suite_id = dag_step.job_suite_id
            job_case_id = dag_step.job_case_id
            job_suite = TestJobSuite.get_by_id(job_suite_id)
            job_case = TestJobCase.get_by_id(job_case_id)
            compare_states = [job_suite.state, job_case.state]
            if ExecState.SKIP in compare_states:
                state = ExecState.SKIP
            if ExecState.STOP in compare_states:
                state = ExecState.STOP
            if state and dag_step.state not in ExecState.end:
                if clear_que:
                    RemoteFlowSource.remove_dag_node(node)
                DagStepInstance.update(state=state).where(
                    DagStepInstance.id == node,
                    DagStepInstance.state.not_in(ExecState.end)
                ).execute()
                test_step = TestStep.get_or_none(dag_step_id=node)
                if test_step:
                    if state == ExecState.STOP and test_step.state not in ExecState.end:
                        cls.stop_step(test_step)
                    test_step.state = state
                    test_step.save()
                check_res = True
        return check_res

    def set_one_dag_node_state(self, node_id, state, err_msg):
        dag_step = DagStepInstance.get_by_id(node_id)
        db_operation.set_one_dag_node_state(node_id, state, err_msg)
        test_step = TestStep.get_or_none(dag_step_id=node_id)
        if test_step:
            self.stop_step(test_step)
            test_step.state = state
            test_step.save()
        self.set_job_suite_or_case_state_by_test_step(test_step, dag_step.stage, state)
        try:
            # 如果当前job、当前机器下没有pending或者running状态的case，则释放该机器
            if not TestJobCase.filter(
                    TestJobCase.job_id == dag_step.job_id,
                    TestJobCase.server_snapshot_id == dag_step.snapshot_server_id,
                    TestJobCase.state.in_(ExecState.no_end_set)
            ).exists():
                logger.info(f"dag({dag_step.id}) exec {state}. "
                            f"and current server(id:{dag_step.snapshot_server_id}) no pending case"
                            f"now release the server(ip:{dag_step.server_ip})")
                self.release_server_with_dag_node(self.job_id, dag_step)
            else:
                logger.info(f"dag({dag_step.id}) exec {state}. "
                            f"current server(id:{dag_step.snapshot_server_id}) has pending case")
        except Exception as error:
            logger.error(f"set_one_dag_node_state: release server failed!{str(error)}")
        logger.info(
            f"set one dag_node state when job stop or delete or process dag_node fail, "
            f"job_id: {self.job_id}, node_id: {node_id}, state: {state}"
        )

    def set_job_suite_state(self):
        if self.job_deleted:
            state = ExecState.SKIP
        else:
            state = self.end_state
        logger.info(f"set job_suite state when job complete, job_id: {self.job_id}, state: {state}")
        db_operation.set_job_suite_state_by_job(self.job_id, state)

    def set_job_case_state(self):
        if self.job_deleted:
            state = ExecState.SKIP
        else:
            state = self.end_state
        logger.info(f"set job_case state when job complete, job_id: {self.job_id}, state: {state}")
        db_operation.set_job_case_state_by_job(self.job_id, state)

    def release_job_server(self, dag_node_ids):
        for dag_node_id in dag_node_ids:
            dag_step = DagStepInstance.get_by_id(dag_node_id)
            self.release_server_with_dag_node(self.job_id, dag_step)

    @staticmethod
    def release_server_with_dag_node(job_id, dag_node):
        dag_node_id = dag_node.id
        meta_data = dag_node.meta_data
        run_mode = meta_data[ServerFlowFields.RUN_MODE]
        run_strategy = meta_data[ServerFlowFields.RUN_STRATEGY]
        server_provider = meta_data[ServerFlowFields.SERVER_PROVIDER]
        job_suite_id = meta_data[ServerFlowFields.JOB_SUITE_ID]
        in_pool = meta_data.get(ServerFlowFields.IN_POOL)
        server_sn = meta_data[ServerFlowFields.SERVER_SN]
        if run_mode == RunMode.CLUSTER:
            server_object_id = dag_node.cluster_id
        else:
            server_object_id = dag_node.server_id
        JobComplete.job_release_server(
            job_id, server_provider, run_mode, job_suite_id, server_object_id, run_strategy,
            server_sn=server_sn, in_pool=in_pool, dag_node_id=dag_node_id, meta_data=meta_data
        )

    @staticmethod
    def job_release_server(job_id, server_provider, run_mode, job_suite_id, server_object_id,
                           run_strategy, server_sn=None, in_pool=True, dag_node_id=None, meta_data=None):
        check_release_msg = (f"Now check server needs to be released, "
                             f"server_object_id:{server_object_id}, in_pool:{in_pool}")

        if dag_node_id:
            check_release_msg += f", dag_node_id:{dag_node_id}"
        logger.info(check_release_msg)
        if not in_pool:
            server_object_id = meta_data['server_snapshot_id']
        # 检查job中该机器是否已经释放了
        if RemoteReleaseServerSource.check_job_release_server(
                job_id, f"{server_provider}_{run_mode}_{job_suite_id}_{server_object_id}_{run_strategy}"
        ):
            warning_msg = (
                f"The server is released, server_provider:{server_provider}, "
                f"job_id: {job_id}, run_mode: {run_mode}, job_suite_id:{job_suite_id}, "
                f"server_object_id:{server_object_id}, in_pool:{in_pool}, run_strategy:{run_strategy}"
            )
            if dag_node_id:
                warning_msg += f", dag_node_id:{dag_node_id}"
            logger.warning(warning_msg)
        else:
            # 释放机器前执行清理脚本(如果有的话)
            if meta_data:
                JobComplete.clean_script(
                    job_id, server_object_id, run_mode, server_provider, meta_data, in_pool
                )
            # 释放机器
            release_msg = f"Now release server, server_object_id:{server_object_id}, in_pool:{in_pool}"
            if dag_node_id:
                release_msg += f", dag_node_id:{dag_node_id}"
            logger.info(release_msg)
            if not in_pool:
                is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
                if is_using and str(using_id) == str(job_id):
                    RemoteAllocServerSource.remove_server_from_using_cache(server_sn)
                    return
            alloc_server_module.AllocServer.release_server(
                job_id, server_object_id, run_mode, server_provider
            )
            RemoteReleaseServerSource.add_job_release_server(
                job_id, f"{server_provider}_{run_mode}_{job_suite_id}_{server_object_id}_{run_strategy}"
            )

    @staticmethod
    def clean_script(job_id, server_object_id, run_mode, server_provider, meta_data, in_pool):
        test_job = TestJob.filter(
            TestJob.id == job_id,
            TestJob.is_deleted.in_([DbField.TRUE, DbField.FALSE])
        ).first()
        clean_script = test_job.cleanup_info
        if clean_script:
            logger.info(
                f"Now clean job script for servers, job_id: {job_id}, "
                f"server_object_id:{server_object_id}, run_mode:{run_mode}, "
                f"server_provider:{server_provider}, meta_data: {meta_data}"
            )
            meta_data[JobCfgFields.ENV_INFO] = utils.safe_json_loads(test_job.env_info, dict())
            meta_data[JobCfgFields.SCRIPT] = clean_script
            if run_mode == RunMode.CLUSTER:
                servers = Cs.get_servers_by_cluster(server_object_id, server_provider)
            else:
                if in_pool:
                    servers = [Cs.get_server_by_provider(server_object_id, server_provider)]
                else:
                    servers = [Cs.get_snapshot_server_by_provider(server_object_id, server_provider)]
            for server in servers:
                if test_job.server_provider == ServerProvider.ALI_GROUP:
                    server_ip = server.ip
                else:
                    server_ip = server.private_ip
                meta_data[ServerFlowFields.SERVER_IP] = server_ip
                meta_data[ServerFlowFields.SERVER_SN] = server.sn
                meta_data[ServerFlowFields.CHANNEL_TYPE] = server.channel_type
                BaseTest.exec_custom_script(meta_data, sync=True, timeout=config.SYNC_SCRIPT_TIMEOUT)

    def stop_steps_by_job_stop(self):
        if self.end_state == ExecState.STOP:
            test_steps = TestStep.filter(job_id=self.job_id, state=ExecState.RUNNING)
            for test_step in test_steps:
                JobComplete.stop_step(test_step)

    @staticmethod
    def stop_step(test_step):
        step_id = test_step.id
        stage = test_step.stage
        try:
            tid = test_step.tid
            job_id = test_step.job_id
            if tid:
                dag_step = test_step.dag_step
                meta_data = dag_step.meta_data
                ip = meta_data[ServerFlowFields.SERVER_IP]
                sn = meta_data[ServerFlowFields.SERVER_SN]
                channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
                ExecChannel.do_stop(channel_type, ip=ip, sn=sn, tid=tid)
                logger.info(f"Stop job step success, job_id:{job_id}, step_id:{step_id}, stage:{stage}")
        except ExecChanelException as error:
            logger.error(f"Stop job step has error: {error},\nstep_id:{step_id},stage:{stage}")
            summary_logger.error(f"Stop job step has error: {error},\nstep_id:{step_id},stage:{stage}")

    def notice(self):
        try:
            send_key = str(self.job_id) + '_' + self.end_state
            if not self.SOURCE_STORE.hexists(OtherCache.is_send_msg, send_key) and self.job.end_time:
                Oam.send_msg_with_job_complete(self.job_id, self.end_state)
                self.SOURCE_STORE.hset(OtherCache.is_send_msg, send_key, self.job.end_time.strftime('%Y-%m-%d'))
                if self.SOURCE_STORE.hlen(OtherCache.is_send_msg) > 50:
                    for tmp_job_key in self.SOURCE_STORE.hkeys(OtherCache.is_send_msg):
                        end_span = datetime.datetime.now() - datetime.datetime.strptime(
                            self.SOURCE_STORE.hget(OtherCache.is_send_msg, tmp_job_key), '%Y-%m-%d')
                        if end_span.days > 3:
                            self.SOURCE_STORE.hdel(OtherCache.is_send_msg, tmp_job_key)
        except Exception as error:
            logger.error(f"produce notice error: {error}, \njob_id:{self.job_id}")

    def save_report(self):
        if self.end_state == ExecState.SUCCESS and self.job.report_template_id and not self.job.report_is_saved:
            Oam.send_msg_with_save_report(self.job_id)

    def clean_job_cache(self):
        """清理异常情况可能导致的脏数据"""
        RemoteAllocServerSource.remove_all_with_job(
            self.job.ws_id, self.job_id, self.job.server_provider
        )
        RemoteReleaseServerSource.delete_job_release_server(self.job_id)

    def clean_job_occupied_server(self):
        if self.job.server_provider == ServerProvider.ALI_GROUP:
            server_model = TestServer
        else:
            server_model = CloudServer
        server_model.update(state=ServerState.AVAILABLE, occupied_job_id=None).where(
            (server_model.state == ServerState.OCCUPIED) &
            ((server_model.occupied_job_id == self.job_id) |
             (server_model.occupied_job_id.is_null(is_null=True)) |
             (server_model.occupied_job_id == ''))
        ).execute()
        if TestJobCase.filter(job_id=self.job_id, run_mode='cluster').exists():
            TestCluster.update(is_occpuied=0, occupied_job_id=None).where(
                (TestCluster.occupied_job_id == self.job_id) |
                (TestCluster.occupied_job_id.is_null(is_null=True)) |
                (TestCluster.occupied_job_id == '')).execute()
        using_server = self.SOURCE_STORE.hgetall(ProcessDataSource.USING_SERVER)
        for key in using_server:
            if using_server[key] == self.job_id:
                self.SOURCE_STORE.hdel(ProcessDataSource.USING_SERVER, key)

    def update_job_info(self):
        if self.job.server_provider == 'aligroup':
            snapshot_model = TestServerSnapshot
        else:
            snapshot_model = CloudServerSnapshot
        kernel_version_count = snapshot_model.filter(
            snapshot_model.job_id == self.job_id, snapshot_model.kernel_version.is_null(is_null=False)).group_by(
            snapshot_model.kernel_version
        )
        product_version_count = snapshot_model.filter(
            snapshot_model.job_id == self.job_id, snapshot_model.product_version.is_null(is_null=False)).group_by(
            snapshot_model.product_version
        )
        if not self.job.show_kernel_version and len(kernel_version_count) == 1:
            kernel_version = kernel_version_count.first().kernel_version
            self.job.show_kernel_version = kernel_version
        if not self.job.product_version and len(product_version_count) == 1:
            product_version = product_version_count.first().product_version
            self.job.product_version = product_version
        self.job.save()

    def remove_monitor(self):
        try:
            monitors = MonitorInfo.filter(object_id=self.job_id, monitor_level=QueueName.JOB, is_open=True)
            if monitors.exists():
                goldmine_monitor_client = GoldmineMonitor()
                for _monitor in monitors:
                    ip = _monitor.server
                    query_type = ServerAttribute.IP if re.match(ServerAttribute.IP_PATTEN, ip) else ServerAttribute.SN
                    req_data = {
                        MonitorParam.QUERY_TYPE: query_type,
                        MonitorParam.QUERY: ip,
                    }
                    goldmine_monitor_client.stop_and_delete_monitor(req_data)
                    _monitor.is_open = False
                    _monitor.save()
        except Exception as error:
            logger.error(f'remove_monitor error: {error}')

    def process(self):
        is_complete = False
        try:
            state_desc = self.job.state_desc or DbField.EMPTY
            if self.complete_flag not in state_desc:
                logger.info(f"Job complete process, job_id is {self.job_id}")
                self.release_server_by_job_deleted_or_exception()
                self.set_job_case_state()
                self.set_job_suite_state()
                self.set_dag_node_state_by_job()
                self.update_job_info()
                self.set_job_state()
                self.notice()
                self.save_report()
                self.remove_monitor()
                self.clean_job_cache()
                self.clean_job_occupied_server()
            is_complete = True
        except Exception as error:
            logger.exception(error)
            summary_logger.exception(error)
        return is_complete
