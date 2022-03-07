import config
from core.init_data import init_data
from core.dag.engine.engine_run import start_engine
from core.distr_lock import DistributedLock
from constant import ProcessDataSource
from tools.log_util import LoggerFactory
from .main_thread import main_thread


logger = LoggerFactory.scheduler()


def run_forever():
    dist_lock_inst = DistributedLock(
        ProcessDataSource.START_PROGRAM_LOCK,
        lock_timeout=config.START_PROGRAM_LOCK_TIMEOUT,
        purpose="start program",
        is_release=False
    )
    if dist_lock_inst.get_lock_until_complete():
        init_data()
        start_engine()
        # 主线程，非守护线程，放在最后
        main_thread()
