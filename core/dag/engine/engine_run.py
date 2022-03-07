import time
import traceback
import threading
import asyncio
import config
from core.exception import (
    GenerateDagException,
    UpdateDagException,
    ProcessDagException,
    ProcessDagNodeException,
    JobEndException,
    DagNodeEndException,
    DagExistsException,
    JobNotExistsException
)
from core.cache.remote_source import RemoteFlowSource
from core.cache.local_inst import LocalInstancePool
from core.dag.plugin.job_complete import JobComplete
from constant import ExecState, QueueType
from models.dag import Dag, DagStepInstance
from tools.log_util import LoggerFactory
from .dag_factory import create_dag_from_job
from .generate_dag import GeneralJobDagFactory


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


def _generate_dag(real_job_id, fmt_job_id):
    logger.info(f"Now generate the DAG with the associated job id of {real_job_id}")
    try:
        real_dag_id = create_dag_from_job(
            GeneralJobDagFactory(real_job_id)
        )
        is_complete = True
    except JobEndException:
        JobComplete(real_job_id).process()
        RemoteFlowSource.remove_running_job(None, fmt_job_id)
    except (JobNotExistsException, DagExistsException):
        RemoteFlowSource.remove_running_job(None, fmt_job_id)
    except GenerateDagException:
        logger.exception(f"Generate dag of job({real_job_id}) has exception: ")
        summary_logger.exception(f"Generate dag of job({real_job_id}):{traceback.format_exc(config.TRACE_LIMIT)}")
        JobComplete(
            real_job_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_job(None, fmt_job_id)
    except Exception as error:
        logger.exception(error)
        summary_logger.exception(f"traceback:{traceback.format_exc(config.TRACE_LIMIT)}")
        JobComplete(
            real_job_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_job(None, fmt_job_id)
    else:
        if is_complete:
            RemoteFlowSource.remove_running_job(None, fmt_job_id)
            RemoteFlowSource.push_pending_dag(
                None,
                QueueType.FAST,
                RemoteFlowSource.source_with_slave_fingerprint(real_dag_id)
            )
        else:
            RemoteFlowSource.push_and_remove_job(real_job_id, fmt_job_id)


async def generate_dag():
    """process_job_id: test_job table id"""
    max_same_pull_times = config.MAX_SAME_PULL_TIMES
    last_job_id, continuous = None, 0
    for _ in range(config.GENERATE_DAG_NUM):
        real_job_id, fmt_job_id = RemoteFlowSource.pull_pending_job()
        if real_job_id:
            if real_job_id == last_job_id:
                continuous += 1
            else:
                continuous = 0
            if continuous >= max_same_pull_times:
                logger.info(f"The same job is checked {max_same_pull_times} times in a row.")
                RemoteFlowSource.push_and_remove_job(real_job_id, fmt_job_id)
                break
            last_job_id = real_job_id
            _generate_dag(real_job_id, fmt_job_id)


def _processing_dag_inst(queue_type, job_id, real_dag_id, fmt_dag_id):
    logger.info(f"Now deal with DAG, DAG id is {real_dag_id}")
    try:
        process_dag_inst = LocalInstancePool.get_process_dag_inst(real_dag_id)
        is_complete, block = process_dag_inst.run()
    except JobEndException:
        JobComplete(job_id).process()
        RemoteFlowSource.remove_running_dag(None, queue_type, fmt_dag_id)
    except (UpdateDagException, ProcessDagException):
        JobComplete(
            job_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_dag(None, queue_type, fmt_dag_id)
    except Exception as error:
        logger.exception(error)
        summary_logger.exception(error)
        JobComplete(
            job_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        ).process()
        RemoteFlowSource.remove_running_dag(None, queue_type, fmt_dag_id)
    else:
        if is_complete:
            RemoteFlowSource.remove_running_dag(None, queue_type, fmt_dag_id)
        else:
            _change_queue(queue_type, block, real_dag_id, fmt_dag_id)


def _change_queue(queue_type, block, real_dag_id, fmt_dag_id):
    # 目前针对block的dag直接从快速队列转到慢速队列，以后也可以
    # 适当调整策略，比如block次数达到一定阈值再转到慢速队列
    logger.info(
        f"Change queue info, query_type:{queue_type}, block:{block}, "
        f"real_dag_id:{real_dag_id}, fmt_dag_id:{fmt_dag_id}"
    )
    remove_query_type = push_queue_type = queue_type
    if block:
        if queue_type == QueueType.FAST:
            push_queue_type = QueueType.SLOW
    else:
        if queue_type == QueueType.SLOW:
            push_queue_type = QueueType.FAST
    RemoteFlowSource.push_and_remove_dag(
        real_dag_id, fmt_dag_id,
        remove_query_type, push_queue_type
    )


async def processing_fast_dag_inst():
    """dag_id: dag table id"""
    max_same_pull_times = config.MAX_SAME_PULL_TIMES
    last_dag_id, continuous = None, 0
    for _ in range(config.FAST_PROCESS_DAG_NUM):
        real_dag_id, fmt_dag_id = RemoteFlowSource.pull_pending_dag(QueueType.FAST)
        if real_dag_id:
            logger.info(f"Now deal with DAG, DAG id is {real_dag_id}")
            if real_dag_id == last_dag_id:
                continuous += 1
            else:
                continuous = 0
            if continuous >= max_same_pull_times:
                logger.info(f"The same dag is checked {max_same_pull_times} times in a row.")
                RemoteFlowSource.push_and_remove_dag(real_dag_id, fmt_dag_id)
                break
            last_dag_id = real_dag_id
            dag = Dag.get_or_none(id=real_dag_id)
            _processing_dag_inst(QueueType.FAST, dag.job_id, real_dag_id, fmt_dag_id)


async def processing_slow_dag_inst():
    """dag_id: dag table id"""
    max_same_pull_times = config.MAX_SAME_PULL_TIMES
    last_dag_id, continuous = None, 0
    for _ in range(config.SLOW_PROCESS_DAG_NUM):
        real_dag_id, fmt_dag_id = RemoteFlowSource.pull_pending_dag(QueueType.SLOW)
        if real_dag_id:
            if real_dag_id == last_dag_id:
                continuous += 1
            else:
                continuous = 0
            if continuous >= max_same_pull_times:
                logger.info(f"The same dag is checked {max_same_pull_times} times in a row.")
                RemoteFlowSource.push_and_remove_dag(
                    real_dag_id, fmt_dag_id,
                    QueueType.SLOW, QueueType.SLOW
                )
                break
            last_dag_id = real_dag_id
            dag = Dag.get_or_none(id=real_dag_id)
            _processing_dag_inst(QueueType.SLOW, dag.job_id, real_dag_id, fmt_dag_id)


def _processing_dag_node_inst(real_dag_node_id, dag_step, fmt_dag_node_id):
    logger.info(f"Now deal with node of DAG, node id is {real_dag_node_id}")
    job_id = dag_step.job_id
    try:
        process_dag_node_inst = LocalInstancePool.get_process_dag_node_inst(real_dag_node_id)
        is_complete = process_dag_node_inst.run()
    except JobEndException:
        JobComplete(job_id).process()
        RemoteFlowSource.remove_running_dag_node(None, fmt_dag_node_id)
    except ProcessDagNodeException:
        JobComplete(job_id).set_one_dag_node_state(
            real_dag_node_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        )
        RemoteFlowSource.remove_running_dag_node(None, fmt_dag_node_id)
    except DagNodeEndException as error:
        logger.exception(error)
        JobComplete(job_id).set_one_dag_node_state(
            real_dag_node_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        )
        RemoteFlowSource.remove_running_dag_node(None, fmt_dag_node_id)
    except Exception as error:
        logger.exception(error)
        summary_logger.exception(error)
        JobComplete(job_id).set_one_dag_node_state(
            real_dag_node_id,
            ExecState.FAIL,
            traceback.format_exc(config.TRACE_LIMIT)
        )
        RemoteFlowSource.remove_running_dag_node(None, fmt_dag_node_id)
    else:
        if is_complete:
            RemoteFlowSource.remove_running_dag_node(None, fmt_dag_node_id)
        else:
            RemoteFlowSource.push_and_remove_dag_node(real_dag_node_id, fmt_dag_node_id)


async def processing_dag_node_inst():
    """dag_node_id: dag_step_instance table id"""
    max_same_pull_times = config.MAX_SAME_PULL_TIMES
    last_dag_node_id, continuous = None, 0
    for _ in range(config.PROCESS_DAG_NODE_NUM):
        real_dag_node_id, fmt_dag_node_id = RemoteFlowSource.pull_pending_dag_node()
        if real_dag_node_id:
            if real_dag_node_id == last_dag_node_id:
                continuous += 1
            else:
                continuous = 0
            if continuous >= max_same_pull_times:
                logger.info(f"The same node is checked {max_same_pull_times} times in a row.")
                RemoteFlowSource.push_and_remove_dag_node(real_dag_node_id, fmt_dag_node_id)
                break
            last_dag_node_id = real_dag_node_id
            dag_step = DagStepInstance.get_by_id(real_dag_node_id)
            _processing_dag_node_inst(real_dag_node_id, dag_step, fmt_dag_node_id)


async def fast_concurrent_execution():
    await asyncio.gather(
        generate_dag(),
        processing_fast_dag_inst(),
        processing_dag_node_inst()
    )


async def slow_concurrent_execution():
    await asyncio.gather(
        processing_slow_dag_inst()
    )


def fast_run():
    interval = config.FAST_ENGINE_INTERVAL
    while 1:
        asyncio.run(fast_concurrent_execution())
        time.sleep(interval)


def slow_run():
    interval = config.SLOW_ENGINE_INTERVAL
    while 1:
        asyncio.run(slow_concurrent_execution())
        time.sleep(interval)


def start_engine():
    threading.Thread(target=fast_run, daemon=True).start()
    threading.Thread(target=slow_run, daemon=True).start()
