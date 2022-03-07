import os
import logging


class ConfigParser(object):
    def __init__(self, file_name, from_env=False):
        self._file_name = file_name
        self._from_env = from_env
        self.property_dic = {}
        self.boolean_mapping = {
            '1': True, 'yes': True, 'true': True, 'on': True,
            '0': False, 'no': False, 'false': False, 'off': False
        }

    def read(self, encoding='utf-8'):
        try:
            if self._from_env:
                self.property_dic = os.environ
            else:
                content = open(self._file_name, 'r', encoding=encoding).read()
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    str_list = line.split('=', 1)
                    if len(str_list) < 2:
                        continue
                    self.property_dic[str_list[0].strip()] = str_list[1].strip()
        except Exception as error:
            logging.error(str(error))
            self.property_dic = os.environ
        return self.property_dic

    def get(self, key, default="", read_now=False):
        if read_now:
            self.read()
        return self._get(str, key, default=default)

    def getboolean(self, key, read_now=False):
        if read_now:
            self.read()
        value = self.property_dic.get(key)
        if value.lower() not in self.boolean_mapping:
            return False
        return self.boolean_mapping[value.lower()]

    def _get(self, value_type, key, default=""):
        return value_type(self.property_dic.get(key)) if self.property_dic.get(key) else default

    def getint(self, key, default="0"):
        return self._get(int, key, default=default)

    def getfloat(self, key, default="0.0"):
        return self._get(float, key, default=default)


if os.environ.get('env', 'local') == 'local':
    cp = ConfigParser(file_name='app.properties', from_env=False)
    cp.read()
else:
    cp = ConfigParser(file_name='tone.properties', from_env=True)
    cp.read()
