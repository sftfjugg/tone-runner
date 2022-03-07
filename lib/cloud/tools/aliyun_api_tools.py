import base64
import datetime
import hashlib
import hmac
import uuid
import urllib
import requests


class AliCloudAuth:

    def __init__(self, config, url='http://eci.aliyuncs.com/',
                 access_id=None,
                 access_key=None, version="2018-08-08"):
        assert config['Action'], "Value error"

        self.config = config
        self.url = url
        self.id = access_id
        self.Key = access_key
        self.__data = dict({
            "AccessKeyId": self.id,
            "SignatureMethod": 'HMAC-SHA1',
            "SignatureVersion": "1.0",
            "SignatureNonce": str(uuid.uuid1()),
            "Timestamp": datetime.datetime.utcnow().isoformat(),
            "Version": version,
            "Format": "JSON",
        }, **config)

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):
        if self.__data:
            raise AssertionError("not allow opeartion")
        self.__data = value

    @staticmethod
    def percent_encode(ens):
        if isinstance(ens, bytes):
            ens = ens.decode('utf-8')
        res = urllib.quote_plus(ens.encode('utf-8'), '')
        res = res.replace('+', '%20').replace('*', '%2A').replace('%7E', '~')
        return res

    def auth(self):
        base = sorted(self.data.items(), key=lambda data: data[0])
        cans = ''
        for k, v in base:
            cans += '&' + self.percent_encode(k) + '=' + self.percent_encode(v)
        self.Key += "&"
        data = 'GET&%2F&' + self.percent_encode(cans[1:])
        self._salt(data)
        return self.data

    def _salt(self, data):
        result = data.encode(encoding='utf-8')
        uri = hmac.new(self.Key.encode("utf-8"), result, hashlib.sha1).digest()
        key = base64.b64encode(uri).strip()
        self.data['Signature'] = key
        return self.data


class AliCloudAPIRequest:
    def __init__(self, url='http://eci.aliyuncs.com/',
                 version="2018-08-08", access_id=None, access_key=None):
        self.url = url
        self.version = version
        self.access_id = access_id
        self.access_key = access_key

    def get(self, action, config=None):
        param_config = {
            "Action": action
        }
        if config and isinstance(config, dict):
            param_config.update(config)
        auth = AliCloudAuth(
            config=param_config,
            url=self.url,
            version=self.version,
            access_id=self.access_id,
            access_key=self.access_key
        )
        token = auth.auth()
        params = urllib.urlencode(token)
        req = requests.get('%s?%s' % (self.url, params))
        res = req.content
        return res
