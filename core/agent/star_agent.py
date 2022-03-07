import time
import config
from core.distr_lock import DistributedLock
from core.exception import AgentRequestException
from constant import RebootStep, ProcessDataSource
from tools.log_util import LoggerFactory


AGENT_REQUEST_MAX_RETRIES = config.AGENT_REQUEST_MAX_RETRIES
AGENT_REQUEST_INTERVAL = config.AGENT_REQUEST_INTERVAL
logger = LoggerFactory.exec_channel()
summary_logger = LoggerFactory.summary_error_log()


def _do_exec(ip_list=None, command="", args="", user="root", sync="false", timeout=60, sn_list=None):
    pass


def __do_exec(sn_list, command, args="", user="root", sync="false", timeout=60):
    pass


def _do_exec_request(data, sync="false"):
    pass


def _do_query(uid_list):
    pass


# star agent停止接口不支持批量
def _do_stop(uid, ip, sn):
    pass

def _check_status(ip, sn):
    if sn:
        success, result = do_exec(sn_list=[sn], command="uptime", sync="true", timeout=10)
    else:
        success, result = do_exec(ip_list=[ip], command="uptime", sync="true", timeout=10)
    if success:
        return result["SUCCESS"], result.get("ERRORMSG", "ERRORMSG is empty")
    else:
        return False, result


def _check_reboot(ip, sn, reboot_time):
    if sn:
        success, result = do_exec(sn_list=[sn], command="cat /proc/uptime", sync="true", timeout=5)
    else:
        success, result = do_exec(ip_list=[ip], command="cat /proc/uptime", sync="true", timeout=5)
    logger.info(f"ip: {ip} check server reboot result is: {result}, channel_type:star-agent")
    if success:
        if result["SUCCESS"]:
            boot_time = int(float(result["JOBRESULT"].strip().split(" ")[0]))  # 已经运行的时间，单位秒
            last_real_reboot_time = int(time.time()) - boot_time  # 系统最后一次启动的时间戳，单位秒
            real_reboot_time = reboot_time - RebootStep.TIMEOUT * 2
            if last_real_reboot_time > real_reboot_time:
                return True
            else:
                return False
        else:
            return False
    else:
        return False


def do_exec(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            success, result = _do_exec(**kwargs)
            if success:
                return success, result
            else:
                logger.error(f"Star-agent do_exec has error:{result}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Star-agent do_exec request has error {error_msg}, traceback:")
        check_cnt += 1
        logger.warning(f"Star-agent do_exec request is fail, Now is the {check_cnt} retry!")
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Star-agent do_exec request fail.")


def do_query(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            success, result = _do_query(**kwargs)
            logger.info(f"Star-agent query result: {result}")
            if success:
                return success, result
            else:
                logger.error(f"Star-agent do_query has error:{result}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Star-agent request has error {error_msg}, traceback:")
        check_cnt += 1
        logger.warning(f"Star-agent request is fail, Now is the {check_cnt} retry!")
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Star-agent do_query request fail.")


def do_stop(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            success, result = _do_stop(**kwargs)
            if success:
                return success, result
            else:
                logger.error(f"Star-agent do_stop has error:{result}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Star-agent request has error {error_msg}, traceback:")
        check_cnt += 1
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Star-agent do_stop request fail.")


def check_server_status(ip=None, sn=None, max_retries=2, interval=1):
    check_cnt = 0
    check_res = False
    error_msg = ""
    # 获取机器检查命令锁
    dist_lock_inst = DistributedLock(
        "{}_{}".format(ProcessDataSource.CHECK_SERVER_STATE_LOCK, ip),
        acquire_timeout=config.CHECK_SERVER_STATE_ACQUIRE_TIMEOUT,
        lock_timeout=config.CHECK_SERVER_STATE_LOCK_TIMEOUT,
        purpose="check machine status",
        is_release=True
    )
    with dist_lock_inst as Lock:
        _lock = Lock.lock
        if _lock:
            while check_cnt < max_retries:
                check_res, error_msg = check_status_once(ip, sn)
                if check_res:
                    break
                check_cnt += 1
                time.sleep(interval)
    if not _lock:
        check_res, error_msg = check_status_once(ip, sn)
    return check_res, error_msg


def check_status_once(ip, sn):
    check_res = False
    try:
        check_res, error_msg = _check_status(ip, sn)
    except Exception as error:
        error_msg = str(error)
    return check_res, error_msg


def check_reboot(ip=None, sn=None, reboot_time=None,  max_retries=5, interval=3):
    check_cnt = 0
    check_res = False
    while check_cnt < max_retries:
        try:
            check_res = _check_reboot(ip, sn, reboot_time)
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Star-agent request has error {error_msg}, traceback:")
        if check_res:
            break
        check_cnt += 1
        time.sleep(interval)
    return check_res
