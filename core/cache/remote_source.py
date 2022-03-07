import os
import time
import json
import random
import config
from core.distr_lock import DistributedLock
from constant import (
    ProcessDataSource,
    ServerProvider,
    QueueType,
    QueueState,
    QueueName,
    RunMode,
    RunStrategy
)
from core.cache.local_stable import LocalStableSource
from core.dag.plugin import db_operation
from tools import utils
from tools.redis_cache import redis_cache
from tools.log_util import LoggerFactory


logger = LoggerFactory.queue()
summary_logger = LoggerFactory.summary_error_log()


class BaseRemoteSource:
    SOURCE_STORE = redis_cache
    pid = os.getppid()
    device_fingerprint = LocalStableSource.get_device_fingerprint()  # 设备指纹
    slave_fingerprint = f"{device_fingerprint}_{pid}_{random.randrange(999999)}"  # 进程指纹,代表一个唯一的节点


class RemoteFlowSource(BaseRemoteSource):
    SLAVE_EXPIRE = config.SLAVE_EXPIRE

    @classmethod
    def _slave_exists(cls, slave):
        return cls.SOURCE_STORE.get(slave)

    @classmethod
    def _split_source(cls, source):
        return source.rsplit("_", 1)

    @classmethod
    def _real_source(cls, source):
        """去除slave指纹信息的原始资源"""
        return int(source.rsplit("_", 1)[-1])

    @classmethod
    def _real_source_set(cls, source_set):
        return {cls._real_source(source) for source in source_set}

    @classmethod
    def source_with_slave_fingerprint(cls, source):
        """将原始资源带上节点指纹信息"""
        return f"{cls.slave_fingerprint}_{source}"

    @classmethod
    def _get_source_name(cls, queue_type=QueueType.FAST,
                         queue_state=QueueState.PENDING,
                         queue_name=QueueName.DAG):
        """根据快慢和状态标识获取队列名称"""
        if queue_type == QueueType.FAST:
            if queue_state == QueueState.PENDING:
                if queue_name == QueueName.DAG:
                    return ProcessDataSource.FAST_PENDING_DAG
            else:
                if queue_name == QueueName.DAG:
                    return ProcessDataSource.FAST_RUNNING_DAG
        else:
            if queue_state == QueueState.PENDING:
                if queue_name == QueueName.DAG:
                    return ProcessDataSource.SLOW_PENDING_DAG
            else:
                if queue_name == QueueName.DAG:
                    return ProcessDataSource.SLOW_RUNNING_DAG
        return ProcessDataSource.FAST_PENDING_DAG

    @classmethod
    def _pending_plan_inst_list(cls) -> list:
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.PENDING_PLAN,
            0, -1
        )

    @classmethod
    def _running_plan_inst_list(cls):
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.RUNNING_PLAN,
            0, -1
        )

    @classmethod
    def _pending_job_list(cls) -> list:
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.PENDING_JOB,
            0, -1
        )

    @classmethod
    def _running_job_list(cls):
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.RUNNING_JOB,
            0, -1
        )

    @classmethod
    def _pending_dag_list(cls, source_name):
        return cls.SOURCE_STORE.lrange(
            source_name,
            0, -1
        )

    @classmethod
    def _running_dag_list(cls, source_name):
        return cls.SOURCE_STORE.lrange(
            source_name,
            0, -1
        )

    @classmethod
    def _pending_dag_node_list(cls):
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.PENDING_DAG_NODE,
            0, -1
        )

    @classmethod
    def _running_dag_node_list(cls):
        return cls.SOURCE_STORE.lrange(
            ProcessDataSource.RUNNING_DAG_NODE,
            0, -1
        )

    @classmethod
    def _slave_set(cls):
        return cls.SOURCE_STORE.smembers(ProcessDataSource.SLAVE)

    @classmethod
    def pull_pending_plan(cls):
        fmt_plan_inst_id = cls.SOURCE_STORE.rpoplpush(
            ProcessDataSource.PENDING_PLAN,
            ProcessDataSource.RUNNING_PLAN
        )
        real_plan_inst_id = None
        if fmt_plan_inst_id:
            real_plan_inst_id = cls._real_source(fmt_plan_inst_id)
            logger.info(
                f"Pull pending plan, real_plan_inst_id:{real_plan_inst_id}, "
                f"fmt_plan_inst_id:{fmt_plan_inst_id}, slave:{cls.slave_fingerprint}"
            )
        return real_plan_inst_id, fmt_plan_inst_id

    @classmethod
    def push_pending_plan(cls, pipe=None, *fmt_plan_inst_id):
        logger.info(
            f"Push pending plan, fmt_plan_inst_id:{fmt_plan_inst_id},"
            f" slave:{cls.slave_fingerprint}, pipe:{pipe}"
        )
        store = pipe or cls.SOURCE_STORE
        res = store.lpush(
            ProcessDataSource.PENDING_PLAN,
            *fmt_plan_inst_id
        )
        if not res:
            logger.error(f"Push pending plan fail, fmt_plan_inst_id:{fmt_plan_inst_id}")
        return res

    @classmethod
    def remove_running_plan(cls, pipe=None, fmt_plan_inst_id=None):
        logger.info(
            f"Remove running plan, fmt_plan_inst_id:{fmt_plan_inst_id},"
            f" slave:{cls.slave_fingerprint}, pipe:{pipe}"
        )
        store = pipe or cls.SOURCE_STORE
        res = store.lrem(
            ProcessDataSource.RUNNING_PLAN, 1,
            fmt_plan_inst_id
        )
        if not res:
            logger.error(f"Remove running plan fail, fmt_plan_inst_id:"
                         f"{fmt_plan_inst_id}, slave:{cls.slave_fingerprint}")
        return res

    @classmethod
    def push_and_remove_plan(cls, real_plan_inst_id, fmt_plan_inst_id):
        with cls.SOURCE_STORE.pipeline() as pipe:
            cls.push_pending_plan(pipe, cls.source_with_slave_fingerprint(real_plan_inst_id))
            cls.remove_running_plan(pipe, fmt_plan_inst_id)
            pipe.execute()

    @classmethod
    def pull_pending_job(cls):
        fmt_job_id = cls.SOURCE_STORE.rpoplpush(
            ProcessDataSource.PENDING_JOB,
            ProcessDataSource.RUNNING_JOB
        )
        real_job_id = None
        if fmt_job_id:
            real_job_id = cls._real_source(fmt_job_id)
            logger.info(f"Pull pending job, real_job_id:{real_job_id}, "
                        f"fmt_job_id:{fmt_job_id}, slave:{cls.slave_fingerprint}")
        return real_job_id, fmt_job_id

    @classmethod
    def push_pending_job(cls, pipe=None, *fmt_job_id):
        logger.info(f"Push pending job, fmt_job_id:{fmt_job_id},"
                    f" slave:{cls.slave_fingerprint}, pipe:{pipe}")
        store = pipe or cls.SOURCE_STORE
        res = store.lpush(
            ProcessDataSource.PENDING_JOB,
            *fmt_job_id
        )
        if not res:
            logger.error(f"Push pending job fail, fmt_job_id:{fmt_job_id}")
        return res

    @classmethod
    def remove_pending_job(cls, fmt_job_id):
        logger.info(f"Remove pending job, fmt_job_id:"
                    f"{fmt_job_id}, slave:{cls.slave_fingerprint}")
        cls.SOURCE_STORE.lrem(
            ProcessDataSource.PENDING_JOB, 1,
            fmt_job_id
        )

    @classmethod
    def remove_running_job(cls, pipe=None, fmt_job_id=None):
        logger.info(f"Remove running job, fmt_job_id:{fmt_job_id}, "
                    f"slave:{cls.slave_fingerprint}, pipe:{pipe}")
        store = pipe or cls.SOURCE_STORE
        res = store.lrem(
            ProcessDataSource.RUNNING_JOB, 1,
            fmt_job_id
        )
        if not res:
            logger.error(f"Remove running job fail, fmt_job_id:"
                         f"{fmt_job_id}, slave:{cls.slave_fingerprint}.")
        return res

    @classmethod
    def push_and_remove_job(cls, real_job_id, fmt_job_id):
        with cls.SOURCE_STORE.pipeline() as pipe:
            cls.push_pending_job(pipe, cls.source_with_slave_fingerprint(real_job_id))
            cls.remove_running_job(pipe, fmt_job_id)
            pipe.execute()

    @classmethod
    def pull_pending_dag(cls, queue_type):
        _pending_dag_name = cls._get_source_name(queue_type)
        _running_dag_name = cls._get_source_name(queue_type, QueueState.RUNNING)
        fmt_dag_id = cls.SOURCE_STORE.rpoplpush(
            _pending_dag_name,
            _running_dag_name
        )
        real_dag_id = None
        if fmt_dag_id:
            real_dag_id = cls._real_source(fmt_dag_id)
            logger.info(
                f"Pull pending dag, queue_type:{queue_type}, real_dag_id:{real_dag_id}, "
                f"fmt_dag_id:{fmt_dag_id}, slave:{cls.slave_fingerprint}"
            )
        return real_dag_id, fmt_dag_id

    @classmethod
    def push_pending_dag(cls, pipe=None, queue_type=QueueType.FAST, *fmt_dag_id):
        logger.info(f"Push pending dag, queue_type:{queue_type}, fmt_dag_id:"
                    f"{fmt_dag_id}, slave:{cls.slave_fingerprint}, pipe:{pipe}")
        source_name = cls._get_source_name(queue_type)
        store = pipe or cls.SOURCE_STORE
        res = store.lpush(
            source_name,
            *fmt_dag_id
        )
        if not res:
            logger.error(f"Push pending dag fail, fmt_dag_id:{fmt_dag_id}")
        return res

    @classmethod
    def remove_pending_dag(cls, queue_type, fmt_dag_id):
        logger.info(
            f"Remove pending dag, fmt_dag_id:"
            f"{fmt_dag_id}, slave:{cls.slave_fingerprint}"
        )
        source_name = cls._get_source_name(queue_type)
        cls.SOURCE_STORE.lrem(
            source_name, 1,
            fmt_dag_id
        )

    @classmethod
    def remove_running_dag(cls, pipe=None, queue_type=QueueType.FAST, fmt_dag_id=None):
        logger.info(f"Remove running dag, query_type:{queue_type}, fmt_dag_id:"
                    f"{fmt_dag_id}, slave:{cls.slave_fingerprint}, pipe:{pipe}")
        source_name = cls._get_source_name(queue_type, QueueState.RUNNING)
        store = pipe or cls.SOURCE_STORE
        res = store.lrem(
            source_name, 1,
            fmt_dag_id
        )
        if not res:
            logger.error(
                f"Remove running dag fail, query_type:{queue_type}, "
                f"fmt_dag_id:{fmt_dag_id}, slave:{cls.slave_fingerprint}"
            )
        return res

    @classmethod
    def push_and_remove_dag(cls, real_dag_id, fmt_dag_id,
                            remove_query_type=QueueType.FAST,
                            push_query_type=QueueType.FAST):
        with cls.SOURCE_STORE.pipeline() as pipe:
            cls.push_pending_dag(pipe, push_query_type, cls.source_with_slave_fingerprint(real_dag_id))
            cls.remove_running_dag(pipe, remove_query_type, fmt_dag_id)
            pipe.execute()

    @classmethod
    def pull_pending_dag_node(cls):
        fmt_dag_node_id = cls.SOURCE_STORE.rpoplpush(
            ProcessDataSource.PENDING_DAG_NODE,
            ProcessDataSource.RUNNING_DAG_NODE
        )
        real_dag_node_id = None
        if fmt_dag_node_id:
            real_dag_node_id = cls._real_source(fmt_dag_node_id)
            logger.info(
                f"Pull pending dag node, real_dag_node_id:{real_dag_node_id}, "
                f"fmt_dag_node_id:{fmt_dag_node_id}, slave:{cls.slave_fingerprint}"
            )
        return real_dag_node_id, fmt_dag_node_id

    @classmethod
    def push_pending_dag_node(cls, pipe=None, *fmt_dag_node_id):
        logger.info(
            f"Push pending dag node, fmt_dag_node_id:{fmt_dag_node_id},"
            f" slave:{cls.slave_fingerprint}, pipe:{pipe}"
        )
        store = pipe or cls.SOURCE_STORE
        res = store.lpush(
            ProcessDataSource.PENDING_DAG_NODE,
            *fmt_dag_node_id
        )
        if not res:
            logger.error(f"Push pending dag node fail, fmt_dat_node_id:{fmt_dag_node_id}")
        return res

    @classmethod
    def remove_pending_dag_node(cls, fmt_dag_node_id):
        logger.info(
            f"Remove pending dag node, fmt_dag_node_id:"
            f"{fmt_dag_node_id}, slave:{cls.slave_fingerprint}"
        )
        cls.SOURCE_STORE.lrem(
            ProcessDataSource.PENDING_DAG_NODE, 1,
            fmt_dag_node_id
        )

    @classmethod
    def remove_running_dag_node(cls, pipe=None, fmt_dag_node_id=None):
        logger.info(f"Remove running dag node, fmt_dag_node_id:{fmt_dag_node_id},"
                    f" slave:{cls.slave_fingerprint}, pipe:{pipe}")
        store = pipe or cls.SOURCE_STORE
        res = store.lrem(
            ProcessDataSource.RUNNING_DAG_NODE, 1,
            fmt_dag_node_id
        )
        if not res:
            logger.error(f"Remove running dag node fail, fmt_dag_node_id:"
                         f"{fmt_dag_node_id}, slave:{cls.slave_fingerprint}")
        return res

    @classmethod
    def push_and_remove_dag_node(cls, real_dag_node_id, fmt_dag_node_id):
        with cls.SOURCE_STORE.pipeline() as pipe:
            cls.push_pending_dag_node(pipe, cls.source_with_slave_fingerprint(real_dag_node_id))
            cls.remove_running_dag_node(pipe, fmt_dag_node_id)
            pipe.execute()

    @classmethod
    def _slave_set_fingerprint(cls):
        if not cls.SOURCE_STORE.sismember(ProcessDataSource.SLAVE, cls.slave_fingerprint):
            cls.SOURCE_STORE.sadd(ProcessDataSource.SLAVE, cls.slave_fingerprint)

    @classmethod
    def _remove_slave(cls, *slave):
        if slave:
            cls.SOURCE_STORE.srem(ProcessDataSource.SLAVE, *slave)

    @classmethod
    def _remove_queue(cls, queue_name, element, count=2, pipe=None):
        """针对机器或进程重启之后的异常数据删除"""
        store = pipe or cls.SOURCE_STORE
        store.lrem(
            queue_name, count,
            element
        )

    @classmethod
    def _correct_running_plan_data(cls, not_exists_slave=None):
        running_plan_inst_set = cls._running_plan_inst_list()
        for fmt_plan_inst_id in running_plan_inst_set:
            slave_fingerprint, real_plan_inst_id = cls._split_source(fmt_plan_inst_id)
            if db_operation.check_plan_inst_end(real_plan_inst_id):
                cls._remove_queue(ProcessDataSource.RUNNING_PLAN, fmt_plan_inst_id)
            else:
                if not_exists_slave is None or slave_fingerprint == not_exists_slave:
                    with cls.SOURCE_STORE.pipeline() as pipe:
                        cls._remove_queue(ProcessDataSource.RUNNING_PLAN, fmt_plan_inst_id, pipe=pipe)
                        cls._remove_queue(ProcessDataSource.PENDING_PLAN, fmt_plan_inst_id, pipe=pipe)
                        cls.push_pending_plan(pipe, cls.source_with_slave_fingerprint(real_plan_inst_id))
                        pipe.execute()

    @classmethod
    def _correct_plan_data_by_cache_clean(cls):
        db_running_plan_inst_set = db_operation.db_running_plan_inst_set()
        if db_running_plan_inst_set:
            fmt_running_plan_inst_id = [cls.source_with_slave_fingerprint(dp) for dp in db_running_plan_inst_set]
            cls.push_pending_plan(None, *fmt_running_plan_inst_id)

    @classmethod
    def _correct_running_job_data(cls, not_exists_slave=None):
        running_job_set = cls._running_job_list()
        for fmt_job_id in running_job_set:
            slave_fingerprint, real_job_id = cls._split_source(fmt_job_id)
            if db_operation.check_job_end(real_job_id):
                cls._remove_queue(ProcessDataSource.RUNNING_JOB, fmt_job_id)
            else:
                if not_exists_slave is None or slave_fingerprint == not_exists_slave:
                    with cls.SOURCE_STORE.pipeline() as pipe:
                        cls._remove_queue(ProcessDataSource.RUNNING_JOB, fmt_job_id, pipe=pipe)
                        cls._remove_queue(ProcessDataSource.PENDING_JOB, fmt_job_id, pipe=pipe)
                        cls.push_pending_job(pipe, cls.source_with_slave_fingerprint(real_job_id))
                        pipe.execute()

    @classmethod
    def _correct_job_data_by_cache_clean(cls):
        db_running_job_set = db_operation.db_running_job_set()
        if db_running_job_set:
            fmt_running_job_id = [cls.source_with_slave_fingerprint(rj) for rj in db_running_job_set]
            cls.push_pending_job(None, *fmt_running_job_id)

    @classmethod
    def _correct_running_dag_data(cls, not_exists_slave=None):
        fast_running_dag_set = cls._running_dag_list(ProcessDataSource.FAST_RUNNING_DAG)
        slow_running_dag_set = cls._running_dag_list(ProcessDataSource.SLOW_RUNNING_DAG)
        cls.__correct_running_dag_data(QueueType.FAST, fast_running_dag_set, not_exists_slave)
        cls.__correct_running_dag_data(QueueType.SLOW, slow_running_dag_set, not_exists_slave)

    @classmethod
    def __correct_running_dag_data(cls, queue_type, running_dag_set, not_exists_slave):
        _running_dag_name = cls._get_source_name(queue_type, QueueState.RUNNING)
        for fmt_dag_id in running_dag_set:
            slave_fingerprint, real_dag_id = cls._split_source(fmt_dag_id)
            if db_operation.check_job_end(dag_id=real_dag_id):
                cls._remove_queue(_running_dag_name, fmt_dag_id)
            else:
                if not_exists_slave is None or slave_fingerprint == not_exists_slave:
                    _pending_dag_name = cls._get_source_name(queue_type, QueueState.PENDING)
                    with cls.SOURCE_STORE.pipeline() as pipe:
                        cls._remove_queue(_running_dag_name, fmt_dag_id, pipe=pipe)
                        cls._remove_queue(_pending_dag_name, fmt_dag_id, pipe=pipe)
                        cls.push_pending_dag(
                            pipe,
                            queue_type,
                            cls.source_with_slave_fingerprint(real_dag_id)
                        )
                        pipe.execute()

    @classmethod
    def _correct_dag_data_by_cache_clean(cls):
        db_running_dag_set = db_operation.db_running_dag_set()
        if db_running_dag_set:
            fmt_running_dag_id = [cls.source_with_slave_fingerprint(rd) for rd in db_running_dag_set]
            cls.push_pending_dag(None, QueueType.FAST, *fmt_running_dag_id)

    @classmethod
    def _correct_running_dag_node_data(cls, not_exists_slave=None):
        running_dag_node_set = cls._running_dag_node_list()
        for fmt_dag_node_id in running_dag_node_set:
            slave_fingerprint, real_dag_node_id = cls._split_source(fmt_dag_node_id)
            if db_operation.check_dag_node_end(real_dag_node_id):
                cls._remove_queue(ProcessDataSource.RUNNING_DAG_NODE, fmt_dag_node_id)
            else:
                if not_exists_slave is None or slave_fingerprint == not_exists_slave:
                    with cls.SOURCE_STORE.pipeline() as pipe:
                        cls._remove_queue(ProcessDataSource.RUNNING_DAG_NODE, fmt_dag_node_id, pipe=pipe)
                        cls._remove_queue(ProcessDataSource.PENDING_DAG_NODE, fmt_dag_node_id, pipe=pipe)
                        cls.push_pending_dag_node(pipe, cls.source_with_slave_fingerprint(real_dag_node_id))
                        pipe.execute()

    @classmethod
    def _correct_dag_node_data_by_cache_clean(cls):
        db_running_dag_node_set = db_operation.db_running_dag_node_set()
        if db_running_dag_node_set:
            fmt_running_dag_node_id = [cls.source_with_slave_fingerprint(rdn) for rdn in db_running_dag_node_set]
            cls.push_pending_dag_node(None, *fmt_running_dag_node_id)

    @classmethod
    def _correct_pending_plan_data(cls):
        pending_plan_inst_list = cls._pending_plan_inst_list()
        pending_plan_inst_set = set()
        for fmt_plan_inst_id in pending_plan_inst_list:
            _, real_plan_inst_id = cls._split_source(fmt_plan_inst_id)
            cls._remove_queue(ProcessDataSource.PENDING_PLAN, fmt_plan_inst_id)
            pending_plan_inst_set.add(real_plan_inst_id)
        for ri in pending_plan_inst_set:
            cls.push_pending_plan(None, cls.source_with_slave_fingerprint(ri))

    @classmethod
    def _correct_pending_job_data(cls):
        pending_job_list = cls._pending_job_list()
        pending_job_set = set()
        for fmt_job_id in pending_job_list:
            _, real_job_id = cls._split_source(fmt_job_id)
            cls._remove_queue(ProcessDataSource.PENDING_JOB, fmt_job_id)
            pending_job_set.add(real_job_id)
        for rj in pending_job_set:
            cls.push_pending_job(None, cls.source_with_slave_fingerprint(rj))

    @classmethod
    def _correct_pending_dag_data(cls):
        fast_pending_dag_list = cls._pending_dag_list(ProcessDataSource.FAST_PENDING_DAG)
        slow_pending_dag_list = cls._pending_dag_list(ProcessDataSource.SLOW_PENDING_DAG)
        fast_pending_dag_set = set()
        slow_pending_dag_set = set()
        for fmt_dag_id in fast_pending_dag_list:
            _, real_dag_id = cls._split_source(fmt_dag_id)
            cls._remove_queue(ProcessDataSource.FAST_PENDING_DAG, fmt_dag_id)
            fast_pending_dag_set.add(real_dag_id)
        for fmt_dag_id in slow_pending_dag_list:
            _, real_dag_id = cls._split_source(fmt_dag_id)
            cls._remove_queue(ProcessDataSource.FAST_PENDING_DAG, fmt_dag_id)
            slow_pending_dag_set.add(real_dag_id)
        for pd in fast_pending_dag_set:
            cls.push_pending_dag(None, QueueType.FAST, cls.source_with_slave_fingerprint(pd))
        for pd in slow_pending_dag_set:
            cls.push_pending_dag(None, QueueType.SLOW, cls.source_with_slave_fingerprint(pd))

    @classmethod
    def _correct_pending_dag_node_data(cls):
        pending_dag_node_list = cls._pending_dag_node_list()
        pending_dag_node_set = set()
        for fmt_dag_node_id in pending_dag_node_list:
            _, real_dag_node_id = cls._split_source(fmt_dag_node_id)
            cls._remove_queue(ProcessDataSource.PENDING_DAG_NODE, fmt_dag_node_id)
            pending_dag_node_set.add(real_dag_node_id)
        for pdn in pending_dag_node_set:
            cls.push_pending_dag_node(None, cls.source_with_slave_fingerprint(pdn))

    @classmethod
    @DistributedLock(
        ProcessDataSource.INIT_CORRECT_DATA_LOCK,
        lock_timeout=config.INIT_CORRECT_DATA_LOCK_TIMEOUT,
        purpose="init correct data",
        is_release=False
    )
    def whether_init(cls):
        slave_list = []
        slave_set = cls._slave_set()
        slave_num = len(slave_set)
        if slave_num == 0:
            return True
        for slave in slave_set:
            if cls.SOURCE_STORE.get(slave):
                slave_list.append(slave)
            else:
                cls._remove_slave(slave)
        if len(slave_list) == 1:
            if slave_list[0] == cls.slave_fingerprint:
                return True
        return False

    @classmethod
    def correct_data_when_first_start(cls):
        cls.correct_pending_data()
        cls.correct_running_data()
        # program start running --> pending
        cls.correct_running_pending()

    @classmethod
    def correct_running_pending(cls):
        cls._correct_running_dag_data()
        cls._correct_running_dag_node_data()
        cls._correct_running_plan_data()
        cls._correct_running_job_data()

    @classmethod
    def _correct_pending_data(cls):
        cls._correct_pending_plan_data()
        cls._correct_pending_job_data()
        cls._correct_pending_dag_data()
        cls._correct_pending_dag_node_data()

    @classmethod
    def correct_pending_data(cls):
        cls.slave_send_heartbeat()
        cls._elect_master()
        if cls._is_master():
            cls._master_send_heartbeat()
            cls._correct_pending_data()

    @classmethod
    def _correct_running_data(cls):
        slave_set = cls._slave_set()
        for slave in slave_set:
            if not cls._slave_exists(slave):
                summary_logger.info(
                    f"Correct running data, not_exists_slave:"
                    f"{slave}, master:{cls.slave_fingerprint}"
                )
                cls._correct_running_plan_data(slave)
                cls._correct_running_job_data(slave)
                cls._correct_running_dag_data(slave)
                cls._correct_running_dag_node_data(slave)
                cls._remove_slave(slave)
                cls._master_send_heartbeat()

    @classmethod
    def correct_running_data(cls):
        cls.slave_send_heartbeat()
        cls._elect_master()
        if cls._is_master():
            # switch master 1. static / 2. dynamic
            cls._master_send_heartbeat()
            cls._correct_running_data()

    @classmethod
    def correct_data_by_cache_clean(cls):
        cls.clear_cache()
        cls._correct_plan_data_by_cache_clean()
        cls._correct_job_data_by_cache_clean()
        cls._correct_dag_data_by_cache_clean()
        cls._correct_dag_node_data_by_cache_clean()

    @classmethod
    def _elect_master(cls):
        """Election of the master"""
        current_master_fingerprint = cls.SOURCE_STORE.get(ProcessDataSource.MASTER_FINGERPRINT)
        if not cls.SOURCE_STORE.get(str(current_master_fingerprint)):
            summary_logger.info(f"Competition becomes the new master, master:{cls.slave_fingerprint}")
            cls.SOURCE_STORE.set(
                ProcessDataSource.MASTER_FINGERPRINT,
                cls.slave_fingerprint, ex=cls.SLAVE_EXPIRE
            )
            cls.SOURCE_STORE.expire(
                ProcessDataSource.MASTER_FINGERPRINT,
                cls.SLAVE_EXPIRE
            )

    @classmethod
    def _is_master(cls):
        return (cls.SOURCE_STORE.get(ProcessDataSource.MASTER_FINGERPRINT) ==
                cls.slave_fingerprint)

    @classmethod
    def _master_send_heartbeat(cls):
        """Master send heartbeat"""
        cls.SOURCE_STORE.expire(
            ProcessDataSource.MASTER,
            cls.SLAVE_EXPIRE
        )

    @classmethod
    def slave_send_heartbeat(cls):
        """Slave send heartbeat"""
        logger.info(f"Slave send heartbeat, slave:{cls.slave_fingerprint}")
        cls._slave_set_fingerprint()
        if not cls.SOURCE_STORE.exists(cls.slave_fingerprint):
            cls.SOURCE_STORE.set(cls.slave_fingerprint, utils.get_ip_or_host_name(), ex=cls.SLAVE_EXPIRE)
            cls.SOURCE_STORE.expire(cls.slave_fingerprint, cls.SLAVE_EXPIRE)
        else:
            cls.SOURCE_STORE.expire(cls.slave_fingerprint, cls.SLAVE_EXPIRE)

    @classmethod
    def remove_dag_node(cls, real_dag_node):
        fmt_dag_node = cls.source_with_slave_fingerprint(real_dag_node)
        cls.remove_pending_dag_node(fmt_dag_node)
        cls.remove_running_dag_node(None, fmt_dag_node)

    @classmethod
    def clear_cache(cls):
        cls.SOURCE_STORE.delete(ProcessDataSource.PENDING_PLAN)
        cls.SOURCE_STORE.delete(ProcessDataSource.RUNNING_PLAN)
        cls.SOURCE_STORE.delete(ProcessDataSource.PENDING_JOB)
        cls.SOURCE_STORE.delete(ProcessDataSource.RUNNING_JOB)
        cls.SOURCE_STORE.delete(ProcessDataSource.FAST_PENDING_DAG)
        cls.SOURCE_STORE.delete(ProcessDataSource.FAST_RUNNING_DAG)
        cls.SOURCE_STORE.delete(ProcessDataSource.SLOW_PENDING_DAG)
        cls.SOURCE_STORE.delete(ProcessDataSource.SLOW_RUNNING_DAG)
        cls.SOURCE_STORE.delete(ProcessDataSource.PENDING_DAG_NODE)
        cls.SOURCE_STORE.delete(ProcessDataSource.RUNNING_DAG_NODE)


class RemoteAllocServerSource(BaseRemoteSource):

    @classmethod
    def get_server_use_freq(cls, ws_id):
        """Gets the items that are used least frequently"""
        key = ProcessDataSource.SERVER_USE_FREQ + f"_ws_{ws_id}"
        return cls.SOURCE_STORE.zrange(key, 0, -1)

    @classmethod
    def set_server_use_freq(cls, ws_id, server_id):
        """Record machine usage frequency"""
        key = ProcessDataSource.SERVER_USE_FREQ + f"_ws_{ws_id}"
        cls.SOURCE_STORE.zincrby(key, 1, server_id)

    @classmethod
    def _get_wait_server_source_key(cls, ws_id, run_strategy, run_mode, provider, spec_object_id=None):
        source_key = f"{ws_id}_{run_strategy}_{run_mode}_{provider}"
        if spec_object_id:
            source_key += f"_{spec_object_id}"
        return source_key

    @classmethod
    def get_wait_server_source(cls, ws_id, run_strategy, run_mode, provider, spec_object_id=None):
        key = cls._get_wait_server_source_key(ws_id, run_strategy, run_mode, provider, spec_object_id)
        result = cls.SOURCE_STORE.zrange(key, 0, 0)  # 按score值从小到大取第一个，也就是最早的一个
        if result:
            return result[0]
        return None

    @classmethod
    def set_wait_server_source(cls, ws_id, object_id, run_strategy, run_mode, provider, spec_object_id=None):
        key = cls._get_wait_server_source_key(ws_id, run_strategy, run_mode, provider, spec_object_id)
        cls.SOURCE_STORE.zadd(key, {object_id: time.time()})

    @classmethod
    def remove_wait_server_source(cls, ws_id, object_id, run_strategy, run_mode, provider, spec_object_id=None):
        key = cls._get_wait_server_source_key(ws_id, run_strategy, run_mode, provider, spec_object_id)
        cls.SOURCE_STORE.zrem(key, object_id)

    @classmethod
    def add_server_to_using_cache(cls, server, job_id):
        cls.SOURCE_STORE.hset(ProcessDataSource.USING_SERVER, server, job_id)

    @classmethod
    def remove_server_from_using_cache(cls, server):
        cls.SOURCE_STORE.hdel(ProcessDataSource.USING_SERVER, server)

    @classmethod
    def check_server_in_using_by_cache(cls, server):
        if cls.SOURCE_STORE.hexists(
            ProcessDataSource.USING_SERVER,
            server
        ):
            return True, cls.SOURCE_STORE.hget(ProcessDataSource.USING_SERVER, server)
        return False, None

    @classmethod
    def set_plan_server_lock_bit(cls, key, lock_bit):
        return cls.SOURCE_STORE.hset(ProcessDataSource.PLAN_SERVER_LOCK, key, lock_bit)

    @classmethod
    def get_plan_server_lock_bit(cls, key):
        lock_bit = cls.SOURCE_STORE.hget(ProcessDataSource.PLAN_SERVER_LOCK, key)
        if lock_bit:
            return lock_bit
        return 0

    @classmethod
    def remove_plan_server_lock(cls, key):
        return cls.SOURCE_STORE.hdel(ProcessDataSource.PLAN_SERVER_LOCK, key)

    @classmethod
    def remove_all_with_job(cls, ws_id, job_id, server_provider):
        cls.remove_wait_server_source(ws_id, job_id, RunStrategy.RAND, RunMode.STANDALONE, server_provider)
        cls.remove_wait_server_source(ws_id, job_id, RunStrategy.RAND, RunMode.CLUSTER, server_provider)
        cls.remove_wait_server_source(ws_id, job_id, RunStrategy.SPEC, RunMode.STANDALONE, server_provider)
        cls.remove_wait_server_source(ws_id, job_id, RunStrategy.SPEC, RunMode.CLUSTER, server_provider)


class RemoteReleaseServerSource(BaseRemoteSource):

    @classmethod
    def check_job_release_server(cls, job_id, server_info):
        check_res = False
        job_release_info = cls.SOURCE_STORE.hget(
            ProcessDataSource.JOB_RELEASE_SERVER,
            job_id
        )
        if job_release_info:
            job_release_info = json.loads(job_release_info)
            if server_info in job_release_info:
                check_res = True

        return check_res

    @classmethod
    def add_job_release_server(cls, job_id, server_info):
        job_release_info = cls.SOURCE_STORE.hget(
            ProcessDataSource.JOB_RELEASE_SERVER,
            job_id
        )
        if job_release_info:
            job_release_info = json.loads(job_release_info)
            if server_info not in job_release_info:
                job_release_info.append(server_info)
                cls.SOURCE_STORE.hset(
                    ProcessDataSource.JOB_RELEASE_SERVER,
                    job_id, json.dumps(job_release_info)
                )
        else:
            cls.SOURCE_STORE.hset(
                ProcessDataSource.JOB_RELEASE_SERVER,
                job_id, json.dumps([server_info])
            )

    @classmethod
    def delete_job_release_server(cls, *job_id):
        if job_id:
            cls.SOURCE_STORE.hdel(
                ProcessDataSource.JOB_RELEASE_SERVER,
                *job_id
            )


class RemoteCronCheckServerSource(BaseRemoteSource):

    @classmethod
    def _get_source_key(cls, provider, flag="_with_real_state"):
        if provider == ServerProvider.ALI_GROUP:
            source_key = ProcessDataSource.LAST_TEST_SERVER_ID + flag
        else:
            source_key = ProcessDataSource.LAST_CLOUD_SERVER_ID + flag
        return source_key

    @classmethod
    def get_last_check_server_id(cls, provider, flag="_with_real_state"):
        last_server_id = cls.SOURCE_STORE.get(cls._get_source_key(provider, flag))
        if not last_server_id:
            last_server_id = 0
        return last_server_id

    @classmethod
    def set_last_check_server_id(cls, provider, server_id, flag="_with_real_state"):
        cls.SOURCE_STORE.set(cls._get_source_key(provider, flag), server_id)
