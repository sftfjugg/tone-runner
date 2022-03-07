import traceback
import config
from core.exception import PlanRunException, PlanEndException, PlanNotExistsException
from core.cache.remote_source import RemoteFlowSource
from core.cache.remote_inst import RemoteProcessPlanInstSource as Rpl
from constant import ExecState
from tools.log_util import LoggerFactory
from .plan_complete import PlanInstComplete


logger = LoggerFactory.scheduler()


def _processing_plan_inst(real_plan_inst_id, fmt_plan_inst_id):
    logger.info(f"Now processing plan, plan_inst_id:{real_plan_inst_id}")
    try:
        process_plan_inst = Rpl.get_process_plan_inst(real_plan_inst_id)
        is_complete = process_plan_inst.run()
    except PlanNotExistsException:
        Rpl.remove_process_plan_inst(fmt_plan_inst_id)
        RemoteFlowSource.remove_running_plan(None, fmt_plan_inst_id)
    except PlanEndException:
        PlanInstComplete(
            real_plan_inst_id,
            error_msg=f"Plan instance is end, plan_inst_id:{real_plan_inst_id}"
        ).process()
        RemoteFlowSource.remove_running_plan(None, fmt_plan_inst_id)
    except PlanRunException:
        PlanInstComplete(
            real_plan_inst_id, ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_plan(None, fmt_plan_inst_id)
    except Exception as error:
        logger.exception(error)
        PlanInstComplete(
            real_plan_inst_id, ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_plan(None, fmt_plan_inst_id)
    else:
        if is_complete:
            RemoteFlowSource.remove_running_plan(None, fmt_plan_inst_id)
        else:
            # 以下顺序不能颠倒
            Rpl.update_process_plan_inst(real_plan_inst_id, process_plan_inst)
            RemoteFlowSource.push_and_remove_plan(real_plan_inst_id, fmt_plan_inst_id)


async def processing_plan():
    """process_plan_inst_id: plan_instance table id"""
    max_same_pull_times = config.MAX_SAME_PULL_TIMES
    last_plan_inst_id, continuous = None, 0
    for _ in range(config.PROCESS_PLAN_NUM):
        real_plan_inst_id, fmt_plan_inst_id = RemoteFlowSource.pull_pending_plan()
        if real_plan_inst_id:
            if real_plan_inst_id == last_plan_inst_id:
                continuous += 1
            else:
                continuous = 0
            if continuous >= max_same_pull_times:
                logger.info(f"The same plan inst is checked {max_same_pull_times} times in a row.")
                RemoteFlowSource.push_and_remove_plan(real_plan_inst_id, fmt_plan_inst_id)
                break
            last_plan_inst_id = real_plan_inst_id
            _processing_plan_inst(real_plan_inst_id, fmt_plan_inst_id)
