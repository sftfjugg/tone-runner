from lib import peewee
from .base import BaseModel


class TestSuite(BaseModel):
    name = peewee.CharField()
    test_type = peewee.CharField()
    run_mode = peewee.CharField("standalone cluster")
    doc = peewee.CharField()
    description = peewee.CharField()

    class Meta:
        table_name = "test_suite"


class TestCase(BaseModel):
    name = peewee.CharField()
    test_suite_id = peewee.IntegerField()
    repeat = peewee.IntegerField()
    timeout = peewee.IntegerField()
    doc = peewee.CharField()
    description = peewee.CharField()
    short_name = peewee.CharField()
    alias = peewee.CharField()

    class Meta:
        table_name = "test_case"


class TestMetric(BaseModel):
    name = peewee.CharField(help_text='指标名')
    object_type = peewee.CharField(help_text='关联对象类型')
    object_id = peewee.IntegerField(help_text='关联对象ID')
    cv_threshold = peewee.FloatField(help_text='变异系数阈值')
    cmp_threshold = peewee.FloatField(help_text='指标跟基线的对比的阈值')
    direction = peewee.CharField(help_text='方向')

    class Meta:
        db_table = 'test_track_metric'


class TestBusiness(BaseModel):
    name = peewee.CharField(help_text='Business名称')
    description = peewee.CharField(help_text='描述')
    creator = peewee.IntegerField(help_text='创建者')
    update_user = peewee.IntegerField(help_text='修改者')

    class Meta:
        db_table = 'test_business'


class BusinessSuiteRelation(BaseModel):
    business_id = peewee.IntegerField(help_text='关联业务ID')
    test_suite_id = peewee.IntegerField(help_text='关联对象ID')

    class Meta:
        db_table = 'business_suite_relation'


class AccessCaseConf(BaseModel):
    test_case_id = peewee.IntegerField()
    ci_type = peewee.CharField()
    host = peewee.CharField()
    user = peewee.CharField()
    token = peewee.CharField()
    pipeline_id = peewee.CharField()
    project_name = peewee.CharField()
    params = peewee.CharField()

    class Meta:
        db_table = 'access_case_conf'
