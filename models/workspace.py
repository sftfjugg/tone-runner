from lib import peewee
from .base import BaseModel


class Workspace(BaseModel):
    id = peewee.CharField(help_text='WS唯一id')
    name = peewee.CharField(help_text='名称')
    show_name = peewee.CharField(help_text='显示名称')
    description = peewee.CharField(help_text='描述')
    owner = peewee.IntegerField(help_text='Owner')
    is_public = peewee.BooleanField(help_text='是否公开')
    is_approved = peewee.BooleanField(default=False, help_text='审核是否通过')
    logo = peewee.CharField(default='', help_text='logo图片')
    creator = peewee.IntegerField(help_text='创建者')

    class Meta:
        db_table = 'workspace'


class Project(BaseModel):
    ws_id = peewee.CharField()
    name = peewee.CharField()
    description = peewee.CharField()
    product_id = peewee.IntegerField()
    product_version = peewee.CharField()

    class Meta:
        db_table = 'project'


class Product(BaseModel):
    ws_id = peewee.CharField()
    name = peewee.CharField()
    description = peewee.CharField()
    command = peewee.CharField()

    class Meta:
        db_table = 'product'
