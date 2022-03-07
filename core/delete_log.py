import os
import time

import config
from core.cache.remote_source import RemoteFlowSource


def do_delete_log():
    log_dir = config.LOG_PATH
    all_log_files = os.listdir(log_dir)
    log_class_list = ['alloc_server', 'check_machine', 'dist_lock', 'exec_channel', 'job_complete', 'job_result',
                      'notice_msg', 'plan_complete', 'program_init', 'queue', 'release_server', 'scheduler',
                      'storage', 'summary_error']
    log_group_data = {log_class: [] for log_class in log_class_list}
    for tmp_log in all_log_files:
        for compare_group in log_class_list:
            if tmp_log.startswith(compare_group):
                log_group_data[compare_group].append(tmp_log)
                break

    save_day = 5
    for log_files in log_group_data.values():
        log_file_list = sorted(log_files)[1: -save_day]
        for tmp_log in log_file_list:
            tmp_log = os.path.join(log_dir, tmp_log)
            os.remove(tmp_log)


def delete_log():
    if config.ENV_TYPE == 'local':
        return
    try:
        RemoteFlowSource.correct_running_pending()
    except Exception:
        pass
    while 1:
        do_delete_log()
        time.sleep(3600*24)
