from core.patterns import BaseObject

__all__ = ['get_eci_conf']


DEFAULT_ENV = [
    {
        'Key': 'PATH',
        'Value': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
        'ValueFrom': {'FieldRef': {}}
    }
]
DEFAULT_CMDS = ['sleep', 99999]
DEFAULT_WORKDIR = '/root'


class ECIConf(BaseObject):
    def __init__(self, **kwargs):
        self.commands = DEFAULT_CMDS
        self.env = DEFAULT_ENV
        self.workdir = DEFAULT_WORKDIR
        self.ports = list()
        self.volume_mount = list()
        self.args = list()
        self.setup_dict(kwargs)


class CentosConf(ECIConf):
    def __init__(self, **kwargs):
        super(CentosConf, self).__init__()
        self.setup_dict(kwargs)


class RedisConf(ECIConf):
    def __init__(self, **kwargs):
        super(RedisConf, self).__init__()
        self.commands = ['redis-server']
        self.workdir = '/data'
        self.setup_dict(kwargs)


class NginxConf(ECIConf):
    def __init__(self, **kwargs):
        super(NginxConf, self).__init__()
        self.commands = []
        self.ports = [{'Protocol': 'TCP', 'Port': 80}]
        self.setup_dict(kwargs)


def get_eci_conf(name):
    confs = {
        'centos': CentosConf(),
        'redis': RedisConf(),
        'nginx': NginxConf(),
    }
    name = name if name in confs else 'centos'
    return confs.get(name)
