from __future__ import print_function
import base64
import hmac
import time
import urllib
from hashlib import sha1
from aliyunsdkcore.acs_exception import error_code
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


class CheckStatus(object):
    @staticmethod
    def check_status(timeout, interval, func, check_status, _id):
        for i in range(timeout):
            time.sleep(interval)
            status = func(_id)
            if status == check_status:
                return True
        return False


SERVICE_ERROR_CODE = ['ServiceUnavailable', 'InternalError']
CLIENT_ERROR_CODE = [error_code.SDK_INVALID_REQUEST]


class ExceptionHandler(object):
    @staticmethod
    def server_exception(e):
        err_type = e.get_error_type()
        request_id = str(e).split('RequestID:')[1].strip()
        http_code = e.get_http_status()
        err_code = e.get_error_code()
        err_message = e.get_error_msg()
        logger.error('cloud driver server error, error type:%s '
                     'request id:%s http code:%s error code:%s message:%s',
                     err_type, request_id, http_code, err_code, err_message)
        if error_code is not None and error_code in SERVICE_ERROR_CODE:
            return False
        else:
            return True  # biz error

    @staticmethod
    def client_exception(e):
        err_type = e.get_error_type()
        err_code = e.get_error_code()
        err_msg = e.get_error_msg()
        logger.error('cloud driver client error, type:%s code:%s message:%s',
                     err_type, err_code, err_msg)
        if err_code is not None and err_code in CLIENT_ERROR_CODE:
            return False
        else:
            return True


def param_sign(ak, parameters):
    s_parameters = sorted(parameters.items(), key=lambda params: params[0])

    param_encode = ''
    for k, v in s_parameters:
        param_encode += '&' + percent_encode(k) + '=' + percent_encode(v)

    s_sign = 'POST&%2F&' + percent_encode(param_encode[1:])

    h = hmac.new(ak + "&", s_sign, sha1)
    signature = base64.encodestring(h.digest()).strip()
    return signature


def percent_encode(s):
    s = str(s)
    res = urllib.quote(s.decode('UTF-8').encode('utf8'), '')
    res = res.replace('+', '%20')
    res = res.replace('*', '%2A')
    res = res.replace('%7E', '~')
    return res


def unify_str(s):
    if isinstance(s, str):
        return s
    else:
        return str(s)


class AliYunException(Exception):
    def __init__(self, msg='', code=None, request_id=None):
        self.code = code
        self.message = msg
        self.request_id = request_id

    def __str__(self, *args, **kwargs):
        sb = 'code=' + unify_str(self.code) + \
             ' message=' + unify_str(self.message) + \
             ' requestId=' + unify_str(self.request_id)
        return sb

    def __repr__(self):
        return self.__str__()
