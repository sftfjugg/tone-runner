from lib import peewee
from .base import BaseModel


class Baseline(BaseModel):
    name = peewee.CharField(help_text='基线名称')
    version = peewee.CharField(help_text='产品版本')
    description = peewee.TextField(help_text='基线描述')
    test_type = peewee.CharField(help_text='测试类型')
    server_provider = peewee.CharField(help_text='机器类型')
    ws_id = peewee.CharField(help_text='所属Workspace')

    class Meta:
        db_table = 'baseline'


class FuncBaselineDetail(BaseModel):
    baseline_id = peewee.IntegerField(help_text='基线Id')
    test_job_id = peewee.IntegerField(help_text='关联JOB ID')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE ID')
    test_case_id = peewee.IntegerField(help_text='关联CASE ID')
    sub_case_name = peewee.CharField(help_text='SUB CASE')
    bug = peewee.CharField(help_text='Aone记录')
    description = peewee.TextField(help_text='问题描述')
    source_job_id = peewee.IntegerField(help_text='来源job')
    note = peewee.CharField(help_text='NOTE')
    impact_result = peewee.BooleanField(help_text="是否影响基线")

    class Meta:
        db_table = 'func_baseline_detail'


class PerfBaselineDetail(BaseModel):
    baseline_id = peewee.IntegerField(help_text='基线Id')
    test_job_id = peewee.IntegerField(help_text='关联JOB ID')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE ID')
    test_case_id = peewee.IntegerField(help_text='关联CASE ID')
    server_ip = peewee.CharField(null=True)
    server_sn = peewee.CharField(null=True)
    # 集团内
    server_sm_name = peewee.CharField(help_text='机型')
    # 云上
    server_instance_type = peewee.CharField(help_text='规格')
    server_image = peewee.CharField(help_text='镜像')
    server_bandwidth = peewee.IntegerField(help_text='带宽')
    run_mode = peewee.CharField(help_text='测试类型')
    source_job_id = peewee.IntegerField(help_text='来源job')
    metric = peewee.CharField(help_text='指标')
    test_value = peewee.CharField(help_text='测试值')
    cv_value = peewee.CharField(help_text='精确值高')
    max_value = peewee.CharField(help_text='最大值')
    min_value = peewee.CharField(help_text='最小值')
    value_list = peewee.TextField(help_text='多次测试值')
    note = peewee.CharField(help_text='NOTE')

    class Meta:
        db_table = 'perf_baseline_detail'
