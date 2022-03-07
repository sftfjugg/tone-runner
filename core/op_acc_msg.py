import json
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from constant import MsgTopics, MsgKey, ServerState, RunMode, ExecState
from models.job import TestJobCase
from tools.notice.msg_producer import MsgProducer
from tools.log_util import LoggerFactory

logger = LoggerFactory.notice_msg()


class OperationAccompanyMsg:
    msg_producer = MsgProducer()

    @classmethod
    def send_msg_with_job_running(cls, job_id, state="running"):
        send_data = json.dumps(
            {
                "job_id": job_id,
                "state": state
            }
        ).encode()
        is_done, msg_info = cls.msg_producer.send(
            MsgTopics.JOB_TOPIC,
            send_data,
            msg_key=MsgKey.JOB_START_RUNNING
        )
        cls._write_logger(is_done, msg_info, send_data)

    @classmethod
    def send_msg_with_job_complete(cls, job_id, state):
        send_data = json.dumps(
            {
                "job_id": job_id,
                "state": state
            }
        ).encode()
        is_done, msg_info = cls.msg_producer.send(
            MsgTopics.JOB_TOPIC,
            send_data,
            msg_key=MsgKey.JOB_STATE_CHANGE
        )
        cls._write_logger(is_done, msg_info, send_data)

    @classmethod
    def send_msg_with_plan_complete(cls, plan_inst_id, plan_id, state):
        send_data = json.dumps(
            {
                "plan_id": plan_id,
                "plan_inst_id": plan_inst_id,
                "state": state
            }
        ).encode()
        is_done, msg_info = cls.msg_producer.send(
            MsgTopics.PLAN_TOPIC,
            send_data,
            msg_key=MsgKey.PLAN_STATE_CHANGE
        )
        cls._write_logger(is_done, msg_info, send_data)

    @classmethod
    def send_msg_with_save_report(cls, job_id):
        send_data = json.dumps(
            {
                "job_id": job_id,
            }
        ).encode()
        is_done, msg_info = cls.msg_producer.send(
            MsgTopics.REPORT_TOPIC,
            send_data,
            msg_key=MsgKey.JOB_SAVE_REPORT
        )
        cls._write_logger(is_done, msg_info, send_data)

    @classmethod
    def set_server_broken_and_send_msg(cls, job_id, server, in_pool=True, error_msg="",
                                       run_mode=RunMode.STANDALONE, cluster_id=None):
        if server.state != ServerState.BROKEN:
            server_id = server.id
            provider = server.server_provider
            Cs.set_server_broken(server, job_id, error_msg)
            if in_pool:
                server_object_id = cluster_id or server_id
                impact_suite = cls._impact_suite_with_server_object(job_id, server_object_id, provider, run_mode)
            else:
                impact_suite = cls._impact_suite_with_server_snapshot(job_id, server.id, provider, run_mode)
            send_data = json.dumps(
                {
                    "machine_id": server_id,
                    "machine_type": server.server_provider,
                    "in_pool": 1 if in_pool else 0,
                    "impact_job": job_id,
                    "impact_suite": impact_suite,
                    "state": ServerState.BROKEN
                }
            ).encode()
            is_done, msg_info = cls.msg_producer.send(
                MsgTopics.MACHINE_TOPIC,
                send_data,
                msg_key=MsgKey.MACHINE_BROKEN
            )
            cls._write_logger(is_done, msg_info, send_data)

    @classmethod
    def _get_snapshot_server_id(cls, job_id, server, provider, in_pool):
        if in_pool:
            return Cs.get_snapshot_server_id_by_provider(job_id, server.id, provider)
        else:
            return server.id

    @classmethod
    def _impact_suite_with_server_snapshot(cls, job_id, server_snapshot_id, provider, run_mode=RunMode.STANDALONE):
        return [
            job_case.test_suite_id
            for job_case in TestJobCase.select(
                TestJobCase.test_suite_id
            ).filter(
                TestJobCase.job_id == job_id,
                TestJobCase.run_mode == run_mode,
                TestJobCase.server_provider == provider,
                TestJobCase.server_snapshot_id == server_snapshot_id,
                TestJobCase.state.not_in(ExecState.end)
            )
        ]

    @classmethod
    def _impact_suite_with_server_object(cls, job_id, server_object_id, provider, run_mode):
        return [
            job_case.test_suite_id
            for job_case in TestJobCase.select(
                TestJobCase.test_suite_id
            ).filter(
                TestJobCase.job_id == job_id,
                TestJobCase.run_mode == run_mode,
                TestJobCase.server_provider == provider,
                TestJobCase.server_object_id == server_object_id,
                TestJobCase.state.not_in(ExecState.end)
            )
        ]

    @classmethod
    def _write_logger(cls, is_done, msg_info, send_data):
        if is_done:
            logger.info(f"send msg {send_data} success, result info is:\n{msg_info}")
        else:
            logger.error(f"send msg {send_data} fail, result info is:\n{msg_info}")
