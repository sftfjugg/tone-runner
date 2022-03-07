import base64
import config
import json
import requests
import time
from constant import MonitorParam
from functools import wraps
from tools.log_util import LoggerFactory

logger = LoggerFactory.scheduler()


class GoldmineMonitorException(Exception):
    pass


def goldmine_monitor_retry(func):
    """监控接口异常重试装饰器"""

    @wraps(func)
    def _wrapper(self, *args, **kwargs):
        if not self.retry_num:
            return func(self, *args, **kwargs)
        try_time = 1
        err_msg = ''
        while try_time < self.retry_num + 1:
            try:
                res = func(self, *args, **kwargs)
            except GoldmineMonitorException as error:
                err_msg = error
                try_time += 1
                time.sleep(self.retry_wait_time)
                self.logger.error(f'goldmine_monitor_retry func:{func}, try_time: {try_time}, error: {error}')
                continue
            else:
                return res
        raise GoldmineMonitorException(err_msg)

    return _wrapper


class GoldmineMonitor(object):
    def __init__(self,  monitor_config=None, monitor_logger=None, retry_num=3):
        if not monitor_config:
            monitor_config = {}
        self.hostname = monitor_config.get('hostname', config.MONITOR_HOSTNAME)
        username = monitor_config.get('goldmine_username', config.GOLDMINE_USERNAME)
        token = monitor_config.get('goldmine_token', config.GOLDMINE_TOKEN)
        self.logger = monitor_logger if monitor_logger else logger
        self.retry_num = retry_num
        self.retry_wait_time = 1
        token_info = '{}|{}|{}'.format(
            username, token, time.time()
        )
        self.secret_token = base64.b64encode(token_info.encode('utf-8')).decode('utf-8')
        self.headers = {'Content-Type': 'application/json'}

    def get(self, endpoint, parameters=None):
        if parameters is None:
            parameters = {}
        parameters.update({'token': self.secret_token})
        url = self.hostname + endpoint
        headers = self.headers
        results = requests.get(url, params=parameters, headers=headers, verify=False)
        return results

    def post(self, endpoint, parameters=None):
        if parameters is None:
            parameters = {}
        parameters.update({'token': self.secret_token})
        url = self.hostname + endpoint
        headers = self.headers
        results = requests.post(url, data=json.dumps(parameters), headers=headers, verify=False)
        return results

    def put(self, endpoint, parameters=None):
        if parameters is None:
            parameters = {}
        parameters.update({'token': self.secret_token})
        url = self.hostname + endpoint
        headers = self.headers
        results = requests.put(url, data=json.dumps(parameters), headers=headers, verify=False)
        return results

    def patch(self, endpoint, parameters=None):
        if parameters is None:
            parameters = {}
        parameters.update({'token': self.secret_token})
        url = self.hostname + endpoint
        headers = self.headers
        results = requests.patch(url, data=json.dumps(parameters), headers=headers, verify=False)
        return results

    def delete(self, endpoint, parameters=None):
        if parameters is None:
            parameters = {}
        parameters.update({'token': self.secret_token})
        url = self.hostname + endpoint
        headers = self.headers
        results = requests.delete(url, data=json.dumps(parameters), headers=headers, verify=False)
        return results

    @goldmine_monitor_retry
    def get_monitor(self, query, query_type):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type
        }
        try:
            response = self.get(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor get request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    @goldmine_monitor_retry
    def create_monitor(self, query, query_type, emp_id):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type,
            MonitorParam.EMP_ID: emp_id,
        }
        try:
            response = self.post(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor create request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    @goldmine_monitor_retry
    def create_and_open_monitor(self, query, query_type):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_cycle_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type,
        }
        try:
            response = self.post(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor create_and_open_monitor request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    @goldmine_monitor_retry
    def open_monitor(self, query, query_type):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_cycle_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type,
        }
        try:
            response = self.put(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor get request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    @goldmine_monitor_retry
    def stop_monitor(self, query, query_type):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_cycle_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type,
        }
        try:
            response = self.delete(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor stop_monitor request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    @goldmine_monitor_retry
    def delete_monitor(self, query, query_type):
        endpoint = '/api/?_to_link=service/monitor/current_call_monitor_api/'
        parameters = {
            MonitorParam.QUERY: query,
            MonitorParam.QUERY_TYPE: query_type,
        }
        try:
            response = self.delete(endpoint, parameters)
            return response.json()
        except Exception as error:
            err_msg = f'job_monitor action monitor delete_monitor request error:{error}, ' \
                      f'response: {response.__dict__}, ' \
                      f'request data:{parameters}'
            self.logger.error(err_msg)
            raise GoldmineMonitorException(err_msg)

    def create_and_start_monitor(self, parameters):
        create_monitor_res = self.create_monitor(parameters.get(MonitorParam.QUERY),
                                                 parameters.get(MonitorParam.QUERY_TYPE),
                                                 parameters.get(MonitorParam.EMP_ID))
        if create_monitor_res.get('code') not in MonitorParam.SUCCESS_CODES:
            raise GoldmineMonitorException(
                'job monitor create failed, ip: {}, msg: {}'.format(parameters.get(MonitorParam.QUERY),
                                                                    create_monitor_res.get('msg')))
        open_monitor_res = self.open_monitor(parameters.get(MonitorParam.QUERY),
                                             parameters.get(MonitorParam.QUERY_TYPE))
        if open_monitor_res.get('code') not in MonitorParam.SUCCESS_CODES:
            raise GoldmineMonitorException(
                'job monitor open failed, ip: {}, msg: {}'.format(parameters.get(MonitorParam.QUERY),
                                                                  open_monitor_res.get('msg')))
        get_monitor_res = self.get_monitor(parameters.get(MonitorParam.QUERY), parameters.get(MonitorParam.QUERY_TYPE))
        if get_monitor_res.get('code') not in MonitorParam.SUCCESS_CODES:
            raise GoldmineMonitorException(
                'job monitor get failed, ip: {}, msg: {}'.format(parameters.get(MonitorParam.QUERY),
                                                                 get_monitor_res.get('msg')))
        data = {
            'monitor_link': get_monitor_res.get('data')
        }
        return data

    def stop_and_delete_monitor(self, parameters):
        stop_res = {}
        delete_res = {}
        try:
            stop_res = self.stop_monitor(parameters.get(MonitorParam.QUERY), parameters.get(MonitorParam.QUERY_TYPE))
        except Exception:
            pass
        if stop_res.get('code') not in MonitorParam.SUCCESS_CODES:
            self.logger.error(f'job monitor stop failed, response: {stop_res}')
        try:
            delete_res = self.delete_monitor(parameters.get(MonitorParam.QUERY),
                                             parameters.get(MonitorParam.QUERY_TYPE))
        except Exception:
            pass
        if delete_res.get('code') not in MonitorParam.SUCCESS_CODES:
            self.logger.error(f'job monitor delete failed, response: {delete_res}')
        return True
