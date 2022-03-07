from core.cache.clear_remote_inst import ClearProcessPlanInstSource as Clp
from core.op_acc_msg import OperationAccompanyMsg as Oam
from constant import ExecState, DbField
from models.plan import PlanInstance
from tools.log_util import LoggerFactory
from tools import utils
from .plan_executor import PlanExecutor


logger = LoggerFactory.plan_complete()


class PlanInstComplete:

    def __init__(self, plan_inst_id, state=None, error_msg=DbField.EMPTY):
        self.plan_inst_id = plan_inst_id
        self.plan_inst = self._get_plan_inst()
        self.state = state or self._get_plan_inst_state()
        self.error_msg = error_msg or ""
        self.complete_flag = "plan instance process is completed."

    def _get_plan_inst_state(self):
        if self.plan_inst.is_deleted:
            return ExecState.STOP
        return self.plan_inst.state

    def _get_plan_inst(self):
        return PlanInstance.filter(
            PlanInstance.id == self.plan_inst_id,
            PlanInstance.is_deleted.in_([DbField.TRUE, DbField.FALSE])
        ).first()

    def update_plan_inst(self):
        state_desc = self.error_msg + "\n" * 3 + self.complete_flag
        if self.plan_inst.start_time:
            start_time = self.plan_inst.start_time
        else:
            start_time = utils.get_now()
        PlanInstance.update(
            state=self.state,
            state_desc=state_desc,
            start_time=start_time,
            end_time=utils.get_now()
        ).where(
            PlanInstance.id == self.plan_inst_id
        ).execute()
        logger.info(f"Update plan instance, plan_inst_id:{self.plan_inst_id}, state:{self.state}")
        if self.state == ExecState.STOP or self.plan_inst.is_deleted:
            logger.info(f"Stop plan instance stage, plan_inst_id:{self.plan_inst_id}")
            PlanExecutor.stop_plan_stage(self.plan_inst_id)
        logger.info(f"Update plan instance stage state, plan_inst_id:{self.plan_inst_id}, state:{self.state}")
        PlanExecutor.update_plan_stage_state(self.plan_inst_id, self.state)

    def notice(self):
        if self.state in ExecState.end:
            logger.info(f"Now notice by plan instance, plan_inst_id:{self.plan_inst_id}")
            Oam.send_msg_with_plan_complete(self.plan_inst.id, self.plan_inst.plan_id, self.state)

    def clear(self):
        logger.info(f"Clear plan instance context cache, plan_inst_id:{self.plan_inst_id}")
        if Clp.remove_process_plan_inst(self.plan_inst_id):
            logger.info(f"Clear plan instance context cache success, plan_inst_id:{self.plan_inst_id}")
        else:
            logger.error(f"Clear plan instance context cache fail, plan_inst_id:{self.plan_inst_id}")
        logger.info(f"Clear plan instance server, plan_inst_id:{self.plan_inst_id}")
        PlanExecutor.clear_plan_server(self.plan_inst_id)

    def process(self):
        try:
            state_desc = self.plan_inst.state_desc or DbField.EMPTY
            if self.complete_flag not in state_desc:
                self.update_plan_inst()
                self.notice()
            self.clear()
        except Exception as error:
            logger.exception(error)
            return False
        return True
