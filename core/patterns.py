import functools
import json
from datetime import datetime
from tools.date_util import DateUtil


class Singleton:
    _instance = None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance:
            result = cls._instance
        else:
            result = cls(*args, **kwargs)
            cls._instance = result
        return result


def singleton(cls):
    instances = {}

    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


class ObjectDict(dict):
    def __str__(self):
        return dict(self).__str__()

    def __repr__(self):
        return self.__str__()

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __getattr__(self, attr):
        value = self[attr]
        return ObjectDict(value) if isinstance(value, dict) else value

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]


class BaseObject:
    def setup_dict(self, data_dict, set_all=False):
        for k, v in data_dict.items():
            if (not set_all and k in self.__dict__) or set_all:
                setattr(self, k, v)

    @classmethod
    def setup_dict_list(cls, data_list):
        # type: (list[dict]) -> list[BaseObject]
        result = []
        for data in data_list:
            model = cls(**data)
            result.append(model)
        return result

    def to_dict(self):
        result_dict = {}
        for k, v in self.__dict__.items():
            if isinstance(v, list):
                if v:
                    if isinstance(v[0], BaseObject):
                        result_dict[k] = v[0].__class__.to_dict_list(*v)
                    else:
                        result_dict[k] = v
                else:
                    result_dict[k] = []
            elif type(v) in (float, int, list, dict, None, tuple, str, bool) or v is None:
                result_dict[k] = v
            elif type(v) == datetime:
                result_dict[k] = DateUtil.datetime_to_str(v)
            elif isinstance(v, ObjectDict):
                result_dict[k] = v
            else:
                dict_v = v.to_dict()
                result_dict[k] = dict_v
        return result_dict

    @staticmethod
    def to_dict_list(*objs):
        return [obj.to_dict() for obj in objs]

    def to_json(self):
        return json.dumps(self.to_dict())


class DictToObject:

    def __init__(self, dct):
        if not isinstance(dct, dict):
            raise Exception("The dct mst be a dict type.")
        self.__dict__.update(dct)

    def copy(self):
        return self.__dict__.copy()

    def update(self, dct):
        if not isinstance(dct, dict):
            raise Exception("The dct mst be a dict type.")
        self.__dict__.update(dct)
