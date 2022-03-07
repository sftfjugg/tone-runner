import os
import random
import socket
import string
import struct
import time
from uuid import getnode as get_mac
from python_hosts import Hosts, HostsEntry
from .log_util import LoggerFactory


logger = LoggerFactory.scheduler()


def form_error_strinify(errors):
    for field, errs in errors.items():
        if field:
            return '%s %s' % (field, errs[0])


def strinify_user(user):
    if user:
        return user.first_name or user.last_name
    return str(user)


def str_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_mac_address():
    '''http://stackoverflow.com/questions/159137/getting-mac-address'''
    _mac = get_mac()
    mac = ':'.join(("%012X" % _mac)[i:i + 2] for i in range(0, 12, 2))
    return mac


def get_ip_address():
    #  get local server ip address
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('9.9.9.9', 9))
        ip = s.getsockname()[0]
    except Exception as error:
        logger.error(error)
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


def ip2int(addr):
    return struct.unpack('!I', socket.inet_aton(addr))[0]


def int2ip(addr):
    return socket.inet_ntoa(struct.pack('!I', addr))


def wait_until_socket_up(ip, port=22, timeout=60, interval=0.5):
    start_time = time.time()
    is_ok = False
    while time.time() - start_time < timeout:
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            sk.connect((ip, port))
            is_ok = True
            break
        except Exception as error:
            logger.error(error)
            time.sleep(interval)
        sk.close()
    return is_ok


def wait_until_server_up(ip, interval=0.5, timeout=60):
    start_time = time.time()
    is_ok = False
    timeout = int(timeout)
    while time.time() - start_time <= timeout:
        resp = os.system('ping -c 1 ' + ip)
        if resp == 0:
            is_ok = True
            break
        time.sleep(interval)
        continue
    return is_ok


def is_ipv4(entry):
    """
    Check if the string provided is a valid ipv4 address
    :param entry: A string representation of an IP address
    :return: True if valid, False if invalid
    """
    try:
        if socket.inet_aton(entry):
            return True
    except socket.error:
        return False


def is_ipv6(entry):
    """
    Check if the string provided is a valid ipv6 address
    :param entry: A string representation of an IP address
    :return: True if valid, False if invalid
    """
    try:
        if socket.inet_pton(socket.AF_INET6, entry):
            return True
    except socket.error:
        return False


def get_ip_ver(entry):
    if is_ipv4(entry):
        return 'ipv4'
    if is_ipv6(entry):
        return 'ipv6'
    return None


class HostBindCtx(object):
    def __init__(self, hosts_ctx):
        self.hosts_tx = hosts_ctx
        self.add_entries = list()
        self.hosts = Hosts()

    def __enter__(self):
        self.bind()

    def __exit__(self, _type, value, trace):
        self.unbind()

    def bind(self):
        for ip, names in self.hosts_tx.items():
            entry_type = get_ip_ver(ip)
            for exist_entry in self.hosts.entries:
                if exist_entry.names:
                    exist_name_line = ' '.join(exist_entry.names)
                    name_line = names
                    if exist_entry.entry_type == entry_type and exist_entry.address == ip and\
                            exist_name_line == name_line:
                        continue
            entry = HostsEntry(entry_type=entry_type, address=ip, names=names.split())
            self.hosts.add([entry])
            logger.info('bind hosts(%s %s) on this server' % (ip, names))
            self.add_entries.append(entry)
        if self.add_entries:
            self.hosts.write()

    def unbind(self):
        if not self.add_entries:
            return
        for entry in self.add_entries:
            for name in entry.names:
                self.hosts.remove_all_matching(address=entry.address, name=name)
                logger.info('unbind hosts(%s %s) on this server' % (entry.address, name))
        self.hosts.write()
