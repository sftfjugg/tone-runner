from lib import peewee
from constant import PlanRunMode, ExecState
from models.user import User
from models.job import BuildJob
from .base import BaseModel


class TestPlan(BaseModel):
    name = peewee.CharField(help_text='计划名称')
    project_id = peewee.IntegerField()
    baseline_info = peewee.TextField(help_text='基线信息')
    test_obj = peewee.CharField(default='kernel', help_text='被测对象')
    kernel_version = peewee.CharField(help_text='内核版本')
    kernel_info = peewee.TextField(help_text='内核信息')
    rpm_info = peewee.TextField(help_text='RPM信息')
    build_pkg_info = peewee.TextField(help_text='build内核信息')
    env_info = peewee.TextField(help_text='环境信息')
    notice_info = peewee.TextField(help_text='通知信息')
    cron_schedule = peewee.BooleanField(default=False, help_text='是否周期触发')
    cron_info = peewee.CharField(help_text='定时信息')
    blocking_strategy = peewee.IntegerField(help_text='阻塞策略')
    description = peewee.CharField(help_text='描述信息')
    build_job_id = peewee.IntegerField(help_text='build kernel id')
    ws_id = peewee.CharField()
    enable = peewee.BooleanField(help_text='是否可用')
    creator = peewee.IntegerField(help_text='创建者')
    update_user = peewee.IntegerField(help_text='修改者', )
    last_time = peewee.DateTimeField(help_text='最后一次运行时间')
    next_time = peewee.DateTimeField(help_text='下次运行时间')

    class Meta:
        db_table = 'test_plan'


class PlanStageRelation(BaseModel):
    plan_id = peewee.IntegerField()
    stage_name = peewee.CharField(help_text='阶段名称')
    stage_index = peewee.IntegerField(help_text='阶段顺序')
    stage_type = peewee.CharField(help_text='阶段类型')
    impact_next = peewee.BooleanField(help_text='是否影响后续步骤')

    class Meta:
        db_table = 'plan_stage_relation'


class PlanStageTestRelation(BaseModel):
    plan_id = peewee.IntegerField()
    run_index = peewee.IntegerField(help_text='触发顺序')
    stage_id = peewee.IntegerField()
    tmpl_id = peewee.IntegerField()

    class Meta:
        db_table = 'plan_stage_test_relation'


class PlanStagePrepareRelation(BaseModel):
    plan_id = peewee.IntegerField()
    run_index = peewee.IntegerField()
    stage_id = peewee.IntegerField()
    prepare_info = peewee.TextField(help_text='脚本信息、监控信息等配置')

    class Meta:
        db_table = 'plan_stage_prepare_relation'


class PlanInstance(BaseModel):
    plan_id = peewee.IntegerField()
    run_mode = peewee.CharField(default=PlanRunMode.AUTO)
    state = peewee.CharField(default=ExecState.PENDING)
    state_desc = peewee.TextField()
    statistics = peewee.CharField(help_text='统计信息')
    name = peewee.CharField(help_text='计划名称')
    baseline_info = peewee.TextField(help_text='基线信息')
    test_obj = peewee.CharField(default='kernel', help_text='被测对象')
    kernel_version = peewee.CharField(help_text='内核版本')
    kernel_info = peewee.TextField(help_text='内核信息')
    rpm_info = peewee.TextField(help_text='RPM信息')
    build_pkg_info = peewee.TextField(help_text='build内核信息')
    build_job_id = peewee.IntegerField(help_text='build kernel id')
    env_info = peewee.TextField(help_text='环境信息')
    notice_info = peewee.TextField(help_text='通知信息')
    ws_id = peewee.CharField()
    project_id = peewee.IntegerField()
    creator = peewee.IntegerField(help_text='创建者')
    start_time = peewee.DateTimeField(help_text='开始时间')
    end_time = peewee.DateTimeField(help_text='结束时间')
    note = peewee.CharField(help_text='备注')

    class Meta:
        db_table = 'plan_instance'

    @property
    def username(self):
        return User.get_by_id(self.creator).username

    @property
    def token(self):
        return User.get_by_id(self.creator).token

    @property
    def build_state(self):
        if self.build_job_id:
            return BuildJob.get_by_id(self.build_job_id).state
        return None

    @property
    def build_msg(self):
        if self.build_job_id:
            return BuildJob.get_by_id(self.build_job_id).build_msg
        return None


class PlanInstanceStageRelation(BaseModel):
    plan_instance_id = peewee.IntegerField()
    stage_name = peewee.CharField(help_text='阶段名称')
    stage_index = peewee.IntegerField(help_text='阶段顺序')
    stage_type = peewee.CharField(help_text='阶段类型')
    impact_next = peewee.BooleanField(help_text='是否影响后续步骤')

    class Meta:
        db_table = 'plan_instance_stage_relation'


class PlanInstancePrepareRelation(BaseModel):
    plan_instance_id = peewee.IntegerField()
    run_index = peewee.IntegerField()
    instance_stage_id = peewee.IntegerField()
    state = peewee.CharField(default=ExecState.PENDING)
    state_desc = peewee.TextField()
    ip = peewee.CharField()
    sn = peewee.CharField()
    channel_type = peewee.CharField()
    tid = peewee.CharField()
    script_info = peewee.TextField()
    extend_info = peewee.TextField()
    result = peewee.TextField(help_text='脚本结果、监控结果、tid等信息')

    class Meta:
        db_table = 'plan_instance_prepare_relation'

    @property
    def server_sn(self):
        if self.sn:
            return self.sn
        else:
            return self.ip + "_" + "plan_inst_prepare"


class PlanInstanceTestRelation(BaseModel):
    plan_instance_id = peewee.IntegerField()
    run_index = peewee.IntegerField(help_text='触发顺序')
    instance_stage_id = peewee.IntegerField()
    test_type = peewee.CharField()
    tmpl_id = peewee.IntegerField()
    job_id = peewee.IntegerField(help_text='关联的job id')
    state = peewee.CharField(default=ExecState.PENDING)
    state_desc = peewee.TextField()

    class Meta:
        db_table = 'plan_instance_test_relation'
