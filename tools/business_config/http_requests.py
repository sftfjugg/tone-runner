# -*- coding: utf-8 -*-
import json
import traceback

import requests

from requests.adapters import HTTPAdapter
from Crypto.Cipher import AES
from tools.log_util import LoggerFactory

logger = LoggerFactory.summary_error_log()


class HttpRequests(object):

    def __init__(self, header=None):
        request_retry = HTTPAdapter(max_retries=3)
        s = requests.Session()
        s.mount('https://', request_retry)
        s.mount('http://', request_retry)
        self.session = s
        if header:
            s.headers.update(header)
        requests.adapters.DEFAULT_RETRIES = 3

    def get(self, url, value, return_type="string"):

        try:
            r = self.session.get(url, params=value)
            logger.debug("[HttpRequests] get url is: %s" % r.url)
            r.encoding = 'utf-8'
            res = r.text
            logger.debug("[HttpRequests] response is: %s" % res)

            if return_type == "json":
                return r.json()
            return res

        except Exception as e:
            err_msg = traceback.format_exc()
            logger.error("[HttpRequests] error: %s\n Detail:%s" % (str(e), err_msg))
            logger.error("[DEBUG]url:%s,\n value:%s,\n return_type:%s" % (url, value, return_type))
            return str(e)

    def post(self, url, value, post_type="json", return_type="string"):
        try:
            if post_type == "json":
                r = self.session.post(url, json=value)
            elif post_type == "string":
                r = self.session.post(url, data=json.dumps(value))
            else:
                r = self.session.post(url, data=value)
            logger.debug("[HttpRequests] post url is: %s" % r.url)
            r.encoding = 'utf-8'
            res = r.text
            logger.debug("[HttpRequests] response is: %s" % res)

            if return_type == "json":
                return r.json()
            return res

        except Exception as e:
            err_msg = traceback.format_exc()
            logger.error("[HttpRequests] error: %s \n Detail:%s" % (e, err_msg))
            return str(e)


class PyCrypt(object):
    def __init__(self, key):
        self.key = key
        self.mode = AES.MODE_ECB
        self.BS = AES.block_size
        self.pad = lambda s: s.decode() + (self.BS - len(s) % self.BS) * chr(self.BS - len(s) % self.BS)
        self.unpad = lambda s: s[0:-ord(s[-1])]

    def encrypt(self, text):
        text = self.pad(text)
        cryptor = AES.new(self.key, self.mode)
        ciphertext = cryptor.encrypt(text.encode('utf-8'))
        return ciphertext
