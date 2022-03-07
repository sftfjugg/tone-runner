# -*- coding: utf-8 -*-
import json
from core.patterns import BaseObject
from config import ECS_LOGIN_PASSWORD


class ProviderReleaseRule(object):
    RELEASE = 0   # release after test
    NOT_RELEASE = 1  # not release after test


class CloudEnvType(object):
    PRE = 'pre'
    PROD = 'prod'


class CloudEnvInfo(BaseObject):
    def __init__(self, **kwargs):
        self.env_type = CloudEnvType.PROD
        self.setup_dict(kwargs)


class ProviderInfo(BaseObject):
    def __init__(self, **kwargs):
        self.name = 'sam.zyc-1d-tone-default'
        self.ak_id = None
        self.provider = ''
        self.region = ''
        self.zone_id = ''
        self.instance_type = ''
        self.image_id = ''
        self.image_name = ''
        self.template_name = ''
        self.system_disk_size = 60
        self.system_disk_category = 'cloud_ssd'
        self.data_disk_category = 'cloud_ssd'
        self.data_disk_size = 80
        self.data_disk_count = 0
        self.login_username = 'root'
        self.login_password = ECS_LOGIN_PASSWORD
        self.internet_max_bandwidth = 5
        self.release_rule = ProviderReleaseRule.RELEASE
        self.instance_id = ''
        self.role = ''
        self.num = 1
        self.var_name = ''
        self.is_inoperable = False
        self.volumes = None
        self.ddh_id = None
        self.ddh_type = None
        self.extra_param = None
        self.env_info = json.loads(CloudEnvInfo().to_json())
        self.setup_dict(kwargs)
