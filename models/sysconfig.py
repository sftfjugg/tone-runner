from lib import peewee
from .base import BaseModel


class BaseConfig(BaseModel):
    config_type = peewee.CharField(help_text='配置类型')
    config_key = peewee.CharField(help_text='配置KEY')
    config_value = peewee.TextField(help_text='配置VALUE')
    bind_stage = peewee.CharField(help_text='绑定步骤')
    description = peewee.CharField(help_text='描述')
    enable = peewee.BooleanField(help_text='启用状态')
    creator = peewee.IntegerField(help_text='创建者')
    update_user = peewee.IntegerField(help_text='修改者')
    ws_id = peewee.CharField(help_text='WS ID')

    class Meta:
        db_table = 'base_config'


class OperationLogs(BaseModel):
    operation_type = peewee.CharField(help_text='操作类型')
    operation_object = peewee.CharField(help_text='操作模块')
    db_table = peewee.CharField(help_text='操作的数据表')
    db_pid = peewee.CharField(help_text='操作的数据行主键id')
    old_values = peewee.TextField(help_text='变更前字段map')
    new_values = peewee.TextField(help_text='变更后字段map')
    operation_sn = peewee.CharField(help_text='操作号uuid')
    creator = peewee.CharField(help_text='操作人')

    class Meta:
        db_table = 'operation_logs'


class KernelInfo(BaseModel):
    version = peewee.CharField()
    kernel_link = peewee.CharField()
    devel_link = peewee.CharField()
    headers_link = peewee.CharField()
    release = peewee.BooleanField()
    enable = peewee.BooleanField()
    creator = peewee.IntegerField()
    update_user = peewee.IntegerField()
    description = peewee.CharField()

    class Meta:
        db_table = 'kernel_info'
