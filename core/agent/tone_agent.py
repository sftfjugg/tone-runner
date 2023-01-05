import traceback

import requests
import base64
import hashlib
import hmac
import time
import config
from core.exception import AgentRequestException
from models.dag import Dag, DagStepInstance
from models.server import CloudServer, CloudServerSnapshot
from tools.config_parser import cp
from tools.log_util import LoggerFactory
__doc__ = "https://yuque.antfin-inc.com/ostest/ducrbe/sw29z7"


AGENT_REQUEST_MAX_RETRIES = config.AGENT_REQUEST_MAX_RETRIES
AGENT_REQUEST_INTERVAL = config.AGENT_REQUEST_INTERVAL
logger = LoggerFactory.exec_channel()
summary_logger = LoggerFactory.summary_error_log()


class ToneAgentRequest(object):

    def __init__(self, access_key, secret_key, domain=config.TONE_AGENT_DOMAIN):
        self._domain = domain
        self._access_key = access_key
        self._secret_key = secret_key
        self.__sign_data = {
            "key": self._access_key,
            # 防止多次请求
            # "nonce": str(uuid.uuid1()),
            # 一定时间内有效
            "timestamp": time.time(),
        }

    @property
    def _data(self):
        return self.__sign_data

    def _sign(self):
        sign_str = ""
        for k in sorted(self._data.keys()):
            sign_str += k
            sign_str += str(self._data[k])
        sign = hmac.new(self._secret_key.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha1).digest()
        base64_sign = base64.b64encode(sign).strip()
        self._data['sign'] = base64_sign.decode()
        return self._data

    def request(self, api, data):
        self.__sign_data.update(data)
        self._sign()
        url = '{domain}/{api}'.format(domain=self._domain, api=api)
        logger.info(f"request_url: {url}, request_data: {self._data}")
        result = requests.post(url, json=self._data, verify=False, headers={'Connection': 'close'})
        try:
            return result.json()
        except Exception as e:
            logger.info(f"send toneagent request failed: url:{url} | data: {self._data} | {result.text}")
            return {'SUCCESS': False, 'ERROR_MSG': f'request failed! url:{url} | data: {self._data} | {e}'}


class SendTaskRequest(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = 'api/task'
        self._request_data = dict(
            ip='',
            tsn='',
            script='',
            args='',
            env='',
            sync='false',
            script_type='shell',
            timeout=3600
        )
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_ip(self, ip):
        self._request_data['ip'] = ip

    def set_tsn(self, tsn):
        self._request_data['tsn'] = tsn

    def set_script(self, script):
        encrypt_script = base64.b64encode(script.encode('utf-8'))
        self._request_data['script'] = encrypt_script.decode()

    def set_script_type(self, script_type="shell"):
        self._request_data['script_type'] = script_type

    def set_args(self, args):
        self._request_data['args'] = args

    def set_env(self, env):
        self._request_data['env'] = env

    def set_cwd(self, cwd):
        self._request_data['cwd'] = cwd

    def set_sync(self, sync):
        self._request_data['sync'] = sync

    def set_timeout(self, timeout):
        self._request_data['timeout'] = timeout

    def send_request(self):
        return self.request(self._api, self._request_data)


class QueryTaskRequest(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = 'api/query'
        self._request_data = dict(
            tid='',
        )
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_tid(self, tid):
        self._request_data['tid'] = tid

    def send_request(self):
        return self.request(self._api, self._request_data)


class StopTaskRequest(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = 'api/stop'
        self._request_data = dict(
            tid='',
        )
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_tid(self, tid):
        self._request_data['tid'] = tid

    def send_request(self):
        return self.request(self._api, self._request_data)


class DeployToneAgent(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = "api/agent/deploy"
        self._request_data = dict()
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_deploy_args(self, ip_list, password):
        self._request_data["ips"] = ip_list
        self._request_data["mode"] = "passive"
        self._request_data["channel"] = "common"
        self._request_data["user"] = "root"
        self._request_data["password"] = password
        self._request_data["description"] = "ToneRunner创建的机器"

    def deploy(self, ip_list, password):
        self.set_deploy_args(ip_list, password)
        return self.request(self._api, self._request_data)


class RemoveAgentRequest(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = 'api/agent/remove'
        self._request_data = dict(
            ip='',
        )
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_ip(self, ip):
        self._request_data['ip'] = ip

    def set_tsn(self, tsn):
        self._request_data['tsn'] = tsn

    def send_request(self):
        return self.request(self._api, self._request_data)


class AddAgentRequest(ToneAgentRequest):
    def __init__(self, access_key, secret_key):
        self._api = 'api/agent/add'
        self._request_data = dict(
            ip='',
            public_ip='',
            mode='active',
            arch='',
        )
        super().__init__(access_key=access_key, secret_key=secret_key)

    def set_ip(self, ip):
        self._request_data['ip'] = ip

    def set_public_ip(self, public_ip):
        self._request_data['public_ip'] = public_ip

    def set_mode(self, mode):
        self._request_data['mode'] = mode

    def set_arch(self, arch):
        self._request_data['arch'] = arch

    def set_version(self, version):
        self._request_data['version'] = version

    def set_description(self, description):
        self._request_data['description'] = description

    def send_request(self):
        return self.request(self._api, self._request_data)


class ToneAgentClient:

    def __init__(self, request_flag="exec"):
        self.access_key = config.TONE_AGENT_ACCESS_KEY
        self.secret_key = config.TONE_AGENT_SECRET_KEY
        if request_flag == "exec":
            self.exec_request = SendTaskRequest(self.access_key, self.secret_key)
        elif request_flag == "query":
            self.query_request = QueryTaskRequest(self.access_key, self.secret_key)
        elif request_flag == "stop":
            self.stop_request = StopTaskRequest(self.access_key, self.secret_key)
        elif request_flag == "deploy":
            self.deploy_request = DeployToneAgent(self.access_key, self.secret_key)
        elif request_flag == 'remove':
            self.remove_request = RemoveAgentRequest(self.access_key, self.secret_key)
        elif request_flag == 'add':
            self.add_request = AddAgentRequest(self.access_key, self.secret_key)

    def do_exec(self, ip=None, command=None, args="", script_type="shell",
                env=None, cwd=None, sync="false", timeout=100, sn=None, tsn=None):
        if ip:
            self.exec_request.set_ip(ip)
        if tsn:
            self.exec_request.set_tsn(tsn)
        if env:
            if isinstance(env, dict):
                tmp_env_list = []
                for key, value in env.items():
                    tmp_env_list.append(f"{key}={value}")
                env = ",".join(tmp_env_list)
            self.exec_request.set_env(env)
        if cwd:
            self.exec_request.set_cwd(cwd)
        if timeout:
            self.exec_request.set_timeout(timeout)
        self.exec_request.set_script(command)
        if args:
            self.exec_request.set_args(args)
        self.exec_request.set_script_type(script_type)
        self.exec_request.set_sync(sync)
        return self.exec_request.send_request()

    def do_query(self, tid):
        self.query_request.set_tid(tid)
        return self.query_request.send_request()

    def do_stop(self, tid):
        self.stop_request.set_tid(tid)
        return self.stop_request.send_request()

    def deploy_agent(self, ip_list, password=config.ECS_LOGIN_PASSWORD):
        return self.deploy_request.deploy(ip_list, password)

    def add_agent(self, ip, public_ip, mode, arch, description, version=None):
        self.add_request.set_ip(ip)
        self.add_request.set_public_ip(public_ip)
        self.add_request.set_mode(mode)
        self.add_request.set_arch(arch)
        self.add_request.set_version(version)
        self.add_request.set_description(description)
        return self.add_request.send_request()

    def remove_agent(self, ip_list, tsn_list):
        self.remove_request.set_ip(ip_list)
        self.remove_request.set_tsn(tsn_list)
        logger.info(f"remove server {ip_list} | {tsn_list}")
        return self.remove_request.send_request()

    def check_status(self, ip, sn=None, tsn=None):
        result = self.do_exec(ip=ip, command="uptime", sync="true", timeout=100, sn=sn, tsn=tsn)
        logger.info(f"{ip} check server status result is: {result}")
        if result["SUCCESS"]:
            result = result["RESULT"]
            task_status = result["TASK_STATUS"]
            if task_status == "success":
                return True
            else:
                return False
        else:
            return False

    def check_reboot(self, ip, reboot_time, sn=None, tsn=None):
        result = self.do_exec(ip=ip, command="cat /proc/uptime", sync="true", timeout=100, sn=sn, tsn=tsn)
        logger.info(f"{ip} {tsn} check server reboot {result}, channel_type:tone-agent")
        if result["SUCCESS"]:
            result = result["RESULT"]
            if result["TASK_STATUS"] == "success":
                boot_time = int(float(result["TASK_RESULT"].strip().split(" ")[0]))
                last_real_reboot_time = int(time.time()) - boot_time
                if last_real_reboot_time > reboot_time:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False


def do_exec(**kwargs):
    check_cnt = 0
    sync = "false"
    if "sync" in kwargs:
        sync = kwargs["sync"]
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            result = ToneAgentClient().do_exec(**kwargs)
            success = result["SUCCESS"]
            if success:
                if sync == "true":
                    result = result["RESULT"]
                    task_status = result["TASK_STATUS"]
                    if task_status == "success":
                        return True, result
                    else:
                        return False, result["ERROR_MSG"]
                else:
                    return success, result
            else:
                error_msg = result["ERROR_MSG"]
                logger.error(f"Tone-agent do_exec has error:{error_msg}, result:{result}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone-agent request has error {error_msg}, traceback:{traceback.format_exc()}")
            summary_logger.exception(f"Tone-agent request has error, {traceback.format_exc(config.TRACE_LIMIT)}")
        logger.warning(f"Tone-agent do_exec request is fail, Now is the {check_cnt} retry!")
        check_cnt += 1
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Tone-agent do_exec request fail.")


def do_query(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            result = ToneAgentClient("query").do_query(**kwargs)
            logger.info(f"Tone-agent query result: {result}")
            success = result["SUCCESS"]
            if success:
                return result["RESULT"]
            else:
                error_msg = result["ERROR_MSG"]
                logger.error(f"Tone-agent do_query has error:{error_msg}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone agent request has error {error_msg}, traceback:")
        logger.warning(f"Tone-agent do_query request is fail, Now is the {check_cnt} retry!")
        check_cnt += 1
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Tone-agent do_query request fail.")


def do_stop(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            result = ToneAgentClient("stop").do_stop(**kwargs)
            success = result["SUCCESS"]
            if success and 'TID' in result:
                return success, result
            else:
                error_msg = result["ERROR_MSG"]
                logger.error(f"Tone-agent do_stop has error:{error_msg}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone-agent do_stop request has error {error_msg}, traceback:")
        logger.warning(f"Tone-agent do_stop request is fail, Now is the {check_cnt} retry!")
        check_cnt += 1
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Tone-agent do_stop request fail.")


def deploy_agent(**kwargs):
    check_cnt = 0
    while check_cnt < AGENT_REQUEST_MAX_RETRIES:
        try:
            result = ToneAgentClient("deploy").deploy_agent(**kwargs)
            logger.info('deploy agent kwargs:{} | result:{}'.format(kwargs, result))
            success = result["SUCCESS"]
            if success:
                result = result["RESULT"]
                fail_servers = result["FAIL_SERVERS"]
                if fail_servers:
                    err_msg = fail_servers[0]["MSG"]
                    logger.info(f"Tone-agent deploy fail, err_msg:{err_msg}")
                    check_cnt += 1
                    continue
                logger.info(f"Tone-agent deploy success, kwargs:{kwargs}")
                return success, result
            else:
                error_msg = result["ERROR_MSG"]
                logger.error(f"Tone-agent deploy has error:{error_msg}")
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone-agent deploy request has error {error_msg}, traceback:{traceback.format_exc()}")
        check_cnt += 1
        logger.warning(f"Tone-agent deploy request is fail, Now is the {check_cnt} retry!")
        time.sleep(AGENT_REQUEST_INTERVAL)
    else:
        raise AgentRequestException("Tone-agent deploy request fail.")


def _update_data_after_deploy_agent(tsn, instance_id, ip):
    CloudServer.update(tsn=tsn).where(CloudServer.instance_id == instance_id).execute()
    CloudServerSnapshot.update(tsn=tsn).where(CloudServerSnapshot.instance_id == instance_id).execute()
    job_id = CloudServerSnapshot.get(CloudServerSnapshot.instance_id == instance_id).job_id
    dag_id = Dag.get(Dag.job_id == job_id).id
    for dag_step in DagStepInstance.filter(DagStepInstance.dag_id == dag_id):
        if dag_step.server_ip == ip:
            dag_step.server_tsn = tsn


def deploy_agent_by_ecs_assistant(
        instance_id, ip, public_ip, cloud_driver,
        mode='active', arch='x86_64', os_type='linux'):
    # 1.请求agent proxy api添加机器、获取tsn
    if os_type == 'debian':
        if arch == 'x86_64':
            version = cp.get('toneagent_version_debian_x86', 'debian-x86_64-1.0.2')
        else:
            version = cp.get('toneagent_version_debian_arm', 'debian-aarch64-1.0.2')
    else:
        if arch == 'x86_64':
            version = cp.get('toneagent_version_linux_x86', 'linux-x86_64-1.0.2')
        else:
            version = cp.get('toneagent_version_linux_arm', 'linux-aarch64-1.0.2')

    logger.info(f"add agent api params:{instance_id}|{ip}|{public_ip}|{cloud_driver}|{mode}|{arch}|{os_type}")
    retry_count = 3
    for i in range(retry_count):
        add_agent_result = ToneAgentClient("add").add_agent(
            ip,
            public_ip,
            mode=mode,
            arch=arch,
            version=version,
            description='created by tone-runner system'
        )
        logger.info(f"add agent api result:{add_agent_result}")
        if add_agent_result.get("SUCCESS"):
            break
    else:
        return False, add_agent_result.get('RESULT') or add_agent_result.get('ERROR_MSG')
    tsn = add_agent_result['RESULT']['TSN']
    rpm_link = add_agent_result['RESULT']['RPM_LINK']

    # 2.更新cloud_server表中的tsn
    _update_data_after_deploy_agent(tsn, instance_id, ip)

    # 3.调用云助手部署agent
    return cloud_driver.deploy_agent(instance_id, tsn, rpm_link, os_type)


def check_server_status(ip, sn=None, max_retries=5, interval=1, tsn=None):
    check_cnt, check_res = 0, False
    error_msg = ""
    while check_cnt < max_retries:
        try:
            check_res = ToneAgentClient().check_status(ip, sn, tsn=tsn)
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone-agent request has error {error_msg}, traceback:")
        if check_res:
            break
        logger.warning(f"Tone-agent check_server_status request is fail, Now is the {check_cnt} retry!")
        check_cnt += 1
        time.sleep(interval)
    return check_res, error_msg


def check_reboot(ip=None, sn=None, tsn=None, reboot_time=None,  max_retries=5, interval=3):
    check_cnt, check_res = 0, False
    while check_cnt < max_retries:
        try:
            check_res = ToneAgentClient().check_reboot(ip, reboot_time, sn, tsn=tsn)
        except Exception as error:
            error_msg = str(error)
            logger.exception(f"Tone-agent request has error {error_msg}, traceback:")
        if check_res:
            break
        check_cnt += 1
        time.sleep(interval)
    return check_res


def check_server_os_type(ip, sn=None, retry_times=3):
    for _ in range(retry_times):
        res = _check_server_os_type(ip, sn)
        if res:
            return res
        time.sleep(3)
    return "linux"


def _check_server_os_type(ip, sn=None):
    debian_os_criterion = [
        'debian',
        'ubuntu',
        'uos',
        'kylin',
    ]
    try:
        result = ToneAgentClient().do_exec(
            ip=ip,
            command="cat /etc/os-release | grep -i id=",
            sync="true",
            timeout=100,
            sn=sn
        )
    except Exception as e:
        logger.error(f"{ip} check server os type failed! error:{e}")
        return
    logger.info(f"{ip} check server os type {result}, channel_type:tone-agent")
    if not result["SUCCESS"]:
        return
    if result["RESULT"]["TASK_STATUS"] == "success":
        os_type_str = result["RESULT"]["TASK_RESULT"]
        if os_type_str:
            os_type_str = os_type_str.lower()
        else:
            logger.warning(f"{ip} check server os type no result: {result}")
            return "linux"
        for item in debian_os_criterion:
            if item in os_type_str:
                return "debian"
        else:
            return "linux"
