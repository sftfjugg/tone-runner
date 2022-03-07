from tools.log_util import LoggerFactory
from .eci_assistant import EciAssistant
from .eci_driver import EciDriver
from .aliyun_network import AliYunNetwork

logger = LoggerFactory.scheduler()


class EciControl(EciDriver, EciAssistant, AliYunNetwork):
    def __init__(self, access_id, access_key, region, zone, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.resource_group_id = resource_group_id
        self.region = region
        self.zone = zone
        self.provider = 'aliyun_eci'
        EciAssistant.__init__(self, self.access_id, self.access_key, self.region, self.zone)
        EciDriver.__init__(self, self.access_id, self.access_key, self.region, self.zone, self.resource_group_id)
        AliYunNetwork.__init__(self, self.access_id, self.access_key, self.region, self.zone)
