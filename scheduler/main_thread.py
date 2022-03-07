import time
import asyncio
import config
from core.cache.remote_source import RemoteFlowSource
from constant import ExecState
from models.plan import PlanInstance
from models.job import TestJob
from tools.log_util import LoggerFactory
from scheduler.plan.process_plan import processing_plan


logger = LoggerFactory.scheduler()


async def produce_exec_plan():
    process_plan_inst_ids = [
        plan_inst.id
        for plan_inst in
        PlanInstance.filter(
            state=ExecState.PENDING
        ).order_by(
            PlanInstance.gmt_created.desc()
        ).limit(config.PROCESS_PLAN_NUM)
    ]
    for real_plan_inst_id in process_plan_inst_ids:
        res = PlanInstance.update(state=ExecState.PENDING_Q).where(
            PlanInstance.id == real_plan_inst_id,
            PlanInstance.state == ExecState.PENDING,
        ).execute()
        if res:
            logger.info(f"Now find an executable plan instance, plan_instance_id:{real_plan_inst_id}")
            RemoteFlowSource.push_pending_plan(
                None, RemoteFlowSource.source_with_slave_fingerprint(real_plan_inst_id)
            )


async def produce_exec_job():
    process_job_ids = [
        test_job.id
        for test_job in
        TestJob.filter(
            state=ExecState.PENDING
        ).order_by(
            TestJob.gmt_created.desc()
        ).limit(config.PROCESS_JOB_NUM)
    ]
    for real_process_job_id in process_job_ids:
        res = TestJob.update(state=ExecState.PENDING_Q).where(
            TestJob.id == real_process_job_id,
            TestJob.state == ExecState.PENDING,
        ).execute()
        if res:
            logger.info(f"Now find an executable job, job_id:{real_process_job_id}")
            RemoteFlowSource.push_pending_job(
                None, RemoteFlowSource.source_with_slave_fingerprint(real_process_job_id)
            )


def main_thread():
    interval = config.PROCESS_SCHEDULER_INTERVAL
    while 1:
        asyncio.run(produce_exec_plan())
        asyncio.run(produce_exec_job())
        asyncio.run(processing_plan())
        time.sleep(interval)
