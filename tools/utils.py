import re
import logging
import traceback
import json
import six
import time
import datetime
import random
import hashlib
import socket
from decimal import Decimal
from collections import defaultdict
from functools import partial
from constant import ObjectType
from . import regexp


def is_ipv4(string):
    return True if re.match(regexp.IP_V4, string) else False


def get_key_by_value(dict_data, target_value):
    for key, value in dict_data.items():
        if value == target_value:
            return key
    return ''


def get_now():
    return datetime.datetime.now()


def inspect_callback_method(method, object_type=ObjectType.CLASS, inst_args=None, inst_kwargs=None):
    call_object = None
    inspect_method = method.__qualname__.split(".")
    if len(inspect_method) > 1:
        call_object = inspect_method[0]
    return [method.__module__, call_object, object_type, inst_args, inst_kwargs, method.__name__]


def simple_fingerprint(source: str = None):
    if source:
        salt = source + str(time.time()) + str(random.random())
    else:
        salt = str(time.time()) + str(random.random())
    return hashlib.sha1(salt.encode("u8")).hexdigest()


def get_fingerprint_from_file(file_name):
    with open(file_name, "a+") as f:
        f.seek(0, 0)
        fingerprint = f.read()
        if not fingerprint:
            fingerprint = simple_fingerprint()
            f.write(fingerprint)
    return fingerprint


def get_ip_or_host_name():
    """
    查询本机ip地址或主机地址
    :return: ip
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        res = s.getsockname()[0]
    except Exception as error:
        logging.error(error)
        res = socket.gethostname()
    finally:
        s.close()

    return res


def filter_invisible_character(s):
    """过滤掉不可见字符"""
    for i in range(0, 32):
        s = s.replace(chr(i), '')
    return s


def safe_json_loads(json_str, default=None):
    try:
        return json.loads(
            json_str,
            object_hook=partial(defaultdict, lambda: default)
        )
    except json.JSONDecodeError:
        logging.error(f"{json_str} use safe_json_loads has error: {traceback.print_exc()}")
        return default


class CachedDnsName(object):
    def __str__(self):
        return self.get_fqdn()

    def get_fqdn(self):
        if not hasattr(self, '_fqdn'):
            self._fqdn = socket.getfqdn()
        return self._fqdn


_PROTECTED_TYPES = six.integer_types + (
    type(None), float, Decimal, datetime.datetime, datetime.date, datetime.time
)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_text(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)


def force_text(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_text, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if issubclass(type(s), six.text_type):
        return s
    if strings_only and is_protected_type(s):
        return s
    try:
        if not issubclass(type(s), six.string_types):
            if six.PY3:
                if isinstance(s, bytes):
                    s = six.text_type(s, encoding, errors)
                else:
                    s = six.text_type(s)
            elif hasattr(s, '__unicode__'):
                s = six.text_type(s)
            else:
                s = six.text_type(bytes(s), encoding, errors)
        else:
            # Note: We use .decode() here, instead of six.text_type(s, encoding,
            # errors), so that if s is a SafeBytes, it ends up being a
            # SafeText at the end.
            s = s.decode(encoding, errors)
    except UnicodeDecodeError as e:
        if not isinstance(s, Exception):
            raise UnicodeDecodeError(*e.args)
        else:
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring plugin without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            s = ' '.join(force_text(arg, encoding, strings_only, errors)
                         for arg in s)
    return s


DNS_NAME = CachedDnsName()


def check_timeout_for_now(start_tm, exp_timeout):
    return (datetime.datetime.now() - start_tm).seconds >= exp_timeout


def get_kernel_version(kernel):
    if kernel.find("kernel-debug") > -1:
        anolis = re.compile(r"\.an[0-9]{1}\.")
        if re.search(anolis, kernel):
            return kernel.rsplit("/", 1)[-1][len("kernel-debug-"):][:-len(".rpm")] + "+debug"
        else:
            if kernel.find("4.19") > -1:
                return kernel.rsplit("/", 1)[-1][len("kernel-debug-"):][:-len(".rpm")] + ".debug"
            if kernel.find("5.10") > -1:
                return kernel.rsplit("/", 1)[-1][len("kernel-debug-"):][:-len(".rpm")] + "+debug"
    return kernel.rsplit("/", 1)[-1][len("kernel-"):][:-len(".rpm")]
