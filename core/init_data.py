import threading
import time

import config
from core.cache.remote_source import RemoteFlowSource
from tools.log_util import LoggerFactory


logger = LoggerFactory.program_init()


def scheduler_correct_data():
    interval = config.CORRECT_DATA_INTERVAL
    while 1:
        RemoteFlowSource.correct_running_data()
        time.sleep(interval)


def correct_data():
    threading.Thread(target=scheduler_correct_data, daemon=True).start()


def init_data():
    RemoteFlowSource.slave_send_heartbeat()
    if RemoteFlowSource.whether_init():
        logger.info(
            f"Correct data when program start, process "
            f"fingerprint:{RemoteFlowSource.slave_fingerprint}..."
        )
        RemoteFlowSource.correct_data_when_first_start()
        logger.info(
            f"Correct data complete when program start, "
            f"fingerprint:{RemoteFlowSource.slave_fingerprint}."
        )
    correct_data()
