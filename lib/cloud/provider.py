import json
import config
from core.patterns import ObjectDict
from lib.cloud.drivers.aliyun import EcsControl, EciControl
from models.server import CloudAk
from .provider_info import ProviderInfo


ECS_DEFAULT_AK = ObjectDict(access_id=config.ALY_ACCESS_ID,
                            access_key=config.ALY_ACCESS_KEY,
                            region='cn-shanghai',
                            zone='cn-shanghai-b')

ECI_DEFAULT_AK = ObjectDict(access_id=config.ALY_ACCESS_ID,
                            access_key=config.ALY_ACCESS_KEY,
                            region='cn-beijing',
                            zone='cn-beijing-g')


class Provider(object):
    ALIYUN = 'aliyun'
    ALIYUN_ECS = 'aliyun_ecs'
    ALIYUN_ECI = 'aliyun_eci'
    AWS = 'aws'
    AZURE = 'azure'
    QCLOUD = 'qcloud'
    HUAWEI = 'huawei'
    ALIGROUP = 'aligroup'

    @classmethod
    def get_all_providers(cls):
        return {k: v for k, v in cls.__dict__.items() if str(k).isupper()}

    @classmethod
    def supported_providers(cls):
        return [cls.ALIYUN_ECS, cls.ALIYUN_ECI]

    @classmethod
    def staragent_provider(cls):
        return cls.ALIGROUP

    @classmethod
    def get_driver_inst(cls, provider, *args, **kwargs):
        if provider == cls.ALIYUN_ECS:
            return EcsControl(*args, **kwargs)
        elif provider == cls.ALIYUN_ECI:
            return EciControl(*args, **kwargs)
        else:
            raise NotImplementedError()

    @classmethod
    def is_aliyun(cls, provider):
        return provider in (cls.ALIYUN_ECI, cls.ALIYUN_ECS, cls.ALIYUN)

    @classmethod
    def get_default_ak(cls, provider):
        if provider == cls.ALIYUN_ECS:
            return ECS_DEFAULT_AK
        elif provider == cls.ALIYUN_ECI:
            return ECI_DEFAULT_AK
        else:
            raise NotImplementedError()

    @classmethod
    def get_default_disk_type(cls, provider):
        if cls.is_aliyun(provider):
            disk_type = 'cloud_ssd'
        elif provider == cls.HUAWEI:
            disk_type = ''
        else:
            disk_type = None
        return disk_type

    @classmethod
    def get_default_image_id(cls, provider):
        if cls.is_aliyun(provider):
            image_id = 'centos_7_04_64_20G_alibase_201701015.vhd'
        elif provider == cls.HUAWEI:
            image_id = ''
        else:
            image_id = None
        return image_id

    @classmethod
    def get_default_provider_info(cls):
        provider = cls.ALIYUN_ECS
        return ObjectDict(provider=provider,
                          image_id=cls.get_default_image_id(provider))


def get_cloud_driver(provider_info):
    if isinstance(provider_info, ProviderInfo):
        provider_info = ProviderInfo(**provider_info.to_dict())
    elif isinstance(provider_info, str):
        provider_info = ProviderInfo(**json.loads(provider_info))
    else:
        provider_info = ProviderInfo(**provider_info)
    ak = CloudAk.get_by_id(provider_info.ak_id)
    driver_info = {'region': provider_info.region,
                   'zone': provider_info.zone_id,
                   'provider': provider_info.provider,
                   'access_id': ak.access_id,
                   'access_key': ak.access_key,
                   'resource_group_id': ak.resource_group_id}
    return Provider.get_driver_inst(**driver_info)
