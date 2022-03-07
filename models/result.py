from lib import peewee
from constant import TrackResult
from .base import BaseModel


class FuncResult(BaseModel):
    test_job_id = peewee.IntegerField(help_text='关联JOB')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE')
    test_case_id = peewee.IntegerField(help_text='关联CASE')
    sub_case_name = peewee.CharField(help_text='SUB CASE')
    sub_case_result = peewee.CharField(help_text='SUB CASE结果')
    match_baseline = peewee.BooleanField(help_text='是否匹配基线')
    note = peewee.CharField(help_text='NOTE')
    bug = peewee.CharField(help_text='Aone记录')
    description = peewee.TextField(help_text='基线问题描述')
    dag_step_id = peewee.IntegerField(help_text='关联DAG步骤id')

    class Meta:
        db_table = 'func_result'


class PerfResult(BaseModel):
    test_job_id = peewee.IntegerField(help_text='关联JOB')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE')
    test_case_id = peewee.IntegerField(help_text='关联CASE')
    metric = peewee.CharField(help_text='指标')
    test_value = peewee.CharField(help_text='测试值')
    cv_value = peewee.CharField(help_text='cv测试值')
    max_value = peewee.CharField(help_text='最大值')
    min_value = peewee.CharField(help_text='最小值')
    value_list = peewee.TextField(help_text='多次测试值，从matrix.json中获取', default="[]")
    unit = peewee.CharField()
    repeat = peewee.IntegerField(help_text='应等于value_list的长度')
    compare_result = peewee.CharField(
        help_text='比较结果,计算公式：change_rate = '
                  '(test_value- baseline_value) / baseline_value if baseline_value else 0.00'
    )
    # track_result：用compare_result 和 test_track_metric中对应记录中的cmp_threshold(把它看成正负区间)对比，
    # 如果大于区间上界则increase，小于区间则decline，在区间范围则normal,
    # 其它各种无法对比则为na （test_track_metric 中找那个不到对应test_case则找test_suite)
    track_result = peewee.CharField(help_text='跟踪结果', default=TrackResult.NA)
    match_baseline = peewee.BooleanField(help_text='是否匹配基线')
    compare_baseline = peewee.IntegerField(help_text='匹配基线的id')
    baseline_value = peewee.CharField(help_text='perf_baseline_detail表里value值')
    baseline_cv_value = peewee.CharField(help_text='perf_baseline_detail表的cv_value值')
    dag_step_id = peewee.IntegerField()

    class Meta:
        db_table = 'perf_result'


class ResultFile(BaseModel):
    test_job_id = peewee.IntegerField(help_text='关联JOB')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE')
    test_case_id = peewee.IntegerField(help_text='关联CASE')
    result_path = peewee.CharField(help_text='结果路径')
    result_file = peewee.CharField(help_text='结果文件')
    archive_file_id = peewee.IntegerField(help_text='ARCHIVE ID')

    class Meta:
        db_table = 'result_file'


class ArchiveFile(BaseModel):
    ws_name = peewee.CharField(help_text='Workspace Name')
    project_name = peewee.CharField(help_text='Project Name')
    test_plan_id = peewee.IntegerField(help_text='关联Plan')
    test_job_id = peewee.IntegerField(help_text='关联JOB')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE')
    test_case_id = peewee.IntegerField(help_text='关联CASE')
    arch = peewee.CharField(help_text='ARCH')
    kernel = peewee.CharField(help_text='内核')
    sn = peewee.CharField(help_text='SN')
    archive_link = peewee.CharField(help_text='LINK')
    test_date = peewee.DateField(help_text='测试日期')

    class Meta:
        db_table = 'archive_file'


class BusinessResult(BaseModel):
    test_job_id = peewee.IntegerField(help_text='关联JOB')
    test_business_id = peewee.IntegerField(help_text='关联BUSINESS')
    test_suite_id = peewee.IntegerField(help_text='关联SUITE')
    test_case_id = peewee.IntegerField(help_text='关联CASE')
    link = peewee.CharField(help_text='ci_project')
    ci_system = peewee.CharField(help_text='ci_system')
    ci_result = peewee.CharField(help_text='ci_result')
    ci_detail = peewee.TextField(help_text='结果详情')
    note = peewee.CharField(help_text='备注')
    dag_step_id = peewee.IntegerField(help_text='关联DAG步骤id')

    class Meta:
        db_table = 'business_result'
