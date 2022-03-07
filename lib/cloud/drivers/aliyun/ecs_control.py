from tools.log_util import LoggerFactory
from .ecs_assistant import EcsAssistant
from .ecs_driver import EcsDriver
from .aliyun_network import AliYunNetwork


logger = LoggerFactory.scheduler()


class EcsControl(EcsAssistant, EcsDriver, AliYunNetwork):
    def __init__(self, access_id, access_key, region, zone, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.zone = zone
        self.resource_group_id = resource_group_id
        self.provider = 'aliyun_ecs'
        EcsAssistant.__init__(self, self.access_id, self.access_key, self.region, self.zone, self.resource_group_id)
        EcsDriver.__init__(self, self.access_id, self.access_key, self.region, self.zone, self.resource_group_id)
        AliYunNetwork.__init__(self, self.access_id, self.access_key, self.region, self.zone, self.resource_group_id)
