from core.exec_channel import ExecChannel
from core.cache.remote_source import RemoteAllocServerSource
from core.server.alibaba.pool_common import PoolCommonOperation
from constant import (
    ServerFlowFields,
    ServerState,
    UsingIdFlag,
    PlanServerLockBit,
)
from models.plan import PlanInstancePrepareRelation as Pip
from .plan_common import PlanCommon


class PlanServer:

    @classmethod
    def get_plan_server_lock_key(cls, plan_inst_id, sn):
        return str(plan_inst_id) + "_" + sn

    @classmethod
    def lock_plan_server(cls, plan_inst_id, sn):
        lock_key = cls.get_plan_server_lock_key(plan_inst_id, sn)
        RemoteAllocServerSource.set_plan_server_lock_bit(lock_key, PlanServerLockBit.LOCK)

    @classmethod
    def unlock_plan_server(cls, plan_inst_id, sn):
        lock_key = cls.get_plan_server_lock_key(plan_inst_id, sn)
        RemoteAllocServerSource.set_plan_server_lock_bit(lock_key, PlanServerLockBit.UNLOCK)

    @classmethod
    def remove_plan_server_lock(cls, plan_inst_id, sn):
        lock_key = cls.get_plan_server_lock_key(plan_inst_id, sn)
        RemoteAllocServerSource.remove_plan_server_lock(lock_key)

    @classmethod
    def get_plan_server_lock_bit(cls, plan_inst_id, sn):
        lock_key = cls.get_plan_server_lock_key(plan_inst_id, sn)
        return int(RemoteAllocServerSource.get_plan_server_lock_bit(lock_key))

    @classmethod
    def get_server_by_plan(cls, plan_inst_id, channel_type, server_ip, server_sn):
        lock_bit = cls.get_plan_server_lock_bit(plan_inst_id, server_sn)
        if lock_bit == PlanServerLockBit.UNLOCK:
            return True, None
        elif lock_bit == PlanServerLockBit.LOCK:
            return False, None

        is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
        if is_using:
            return False, {
                ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                ServerFlowFields.JOB_ID: using_id
            }
        check_success, error_msg = ExecChannel.check_server_status(
            channel_type, server_ip
        )
        if not check_success:
            return False, {
                ServerFlowFields.SERVER_STATE: ServerState.BROKEN,
            }
        else:
            _plan_inst_id = str(plan_inst_id) + UsingIdFlag.PLAN_INST
            RemoteAllocServerSource.add_server_to_using_cache(server_sn, _plan_inst_id)
            cls.lock_plan_server(plan_inst_id, server_sn)
            return True, None

    @classmethod
    def get_state_desc_by_server_state(cls, result):
        job_state_desc = None
        if result:
            server_state = result[ServerFlowFields.SERVER_STATE]
            if server_state == ServerState.OCCUPIED:
                using_id = result.get(ServerFlowFields.JOB_ID)
                who_using = PoolCommonOperation.who_using_server(using_id)
                using_id = PoolCommonOperation.real_using_id(using_id)
                job_state_desc = f"当前指定机器被{who_using}({using_id})占用。"
            elif server_state == ServerState.BROKEN:
                job_state_desc = "当前指定机器状态已Broken，请检查。"
        return job_state_desc

    @classmethod
    def release_server_by_plan(cls, plan_inst_id):
        stage_id = PlanCommon.get_inst_prepare_stage_id(plan_inst_id)
        stage_elements = PlanCommon.stage_elements(Pip, stage_id)
        for se in stage_elements:
            RemoteAllocServerSource.remove_server_from_using_cache(se.server_sn)
