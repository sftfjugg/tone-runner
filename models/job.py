import json
from lib import peewee
from constant import ExecState, ServerProvider, BuildKernelFrom
from tools import utils
from .base import BaseModel
from .case import TestCase, TestSuite
from .dag import DagStepInstance
from .workspace import Workspace, Project, Product
from .user import User


class TestJob(BaseModel):
    name = peewee.CharField()
    server_provider = peewee.CharField()
    created_from = peewee.CharField()
    creator = peewee.IntegerField()
    test_type = peewee.CharField()
    need_reboot = peewee.BooleanField()
    console = peewee.SmallIntegerField()
    iclone_info = peewee.TextField()
    build_pkg_info = peewee.TextField()
    kernel_info = peewee.TextField()
    product_version = peewee.TextField()
    script_info = peewee.TextField()
    cleanup_info = peewee.TextField()
    monitor_info = peewee.TextField()
    rpm_info = peewee.TextField()
    env_info = peewee.TextField()
    notice_info = peewee.TextField()
    state = peewee.CharField()
    baseline_id = peewee.IntegerField()
    baseline_job_id = peewee.IntegerField()
    report_name = peewee.CharField()
    report_template_id = peewee.IntegerField()
    report_is_saved = peewee.BooleanField()
    kernel_version = peewee.CharField()
    show_kernel_version = peewee.CharField()
    project_id = peewee.IntegerField()
    plan_id = peewee.IntegerField()
    product_id = peewee.IntegerField()
    build_job_id = peewee.IntegerField()
    source_job_id = peewee.IntegerField()
    tmpl_id = peewee.IntegerField()
    job_type_id = peewee.IntegerField()
    test_result = peewee.CharField()
    state_desc = peewee.TextField()
    ws_id = peewee.CharField()
    start_time = peewee.DateTimeField(default=utils.get_now())
    end_time = peewee.DateTimeField(default=utils.get_now())

    class Meta:
        table_name = "test_job"

    @property
    def ws_name(self):
        return Workspace.get_by_id(self.ws_id).name

    @property
    def project_name(self):
        if self.project_id:
            return Project.get_by_id(self.project_id).name
        return ""

    @property
    def product_name(self):
        if self.product_id:
            return Product.get_by_id(self.product_id).name
        return ""

    @property
    def plan_name(self):
        if self.plan_id:
            pass
        return ""

    @property
    def creator_username(self):
        return User.get_by_id(self.creator).username


class TestJobConfig(BaseModel):
    object_type = peewee.CharField()
    object_id = peewee.IntegerField()
    device_type = peewee.CharField()
    sm_name = peewee.CharField()
    kernel_info = peewee.CharField()
    hotfix_info = peewee.CharField()
    variables_info = peewee.CharField()
    baseline_id = peewee.IntegerField()
    script_info = peewee.TextField()
    rpm_info = peewee.TextField()
    iclone_info = peewee.CharField()
    notice_info = peewee.TextField()
    extend_info = peewee.TextField()

    class Meta:
        table_name = "test_job_config"


class CloudJobConfig(BaseModel):
    object_type = peewee.CharField()
    object_id = peewee.IntegerField()
    provider = peewee.CharField()
    release_rule = peewee.BooleanField(default=True)
    ak = peewee.CharField()
    image = peewee.CharField()
    region = peewee.CharField()
    zone = peewee.CharField()
    instance_id = peewee.CharField(max_length=32)
    max_bandwidth = peewee.CharField()
    volumes_info = peewee.CharField()
    ddh_info = peewee.CharField()
    login_info = peewee.CharField()
    env_info = peewee.CharField()
    extend_info = peewee.TextField()

    class Meta:
        table_name = "cloud_job_config"


class TagRelation(BaseModel):
    object_type = peewee.CharField()
    object_id = peewee.IntegerField()
    tag_id = peewee.IntegerField()

    class Meta:
        table_name = "tag_relation"


class TestJobSuite(BaseModel):
    job_id = peewee.IntegerField()
    test_suite_id = peewee.IntegerField()
    need_reboot = peewee.BooleanField()
    console = peewee.SmallIntegerField()
    setup_info = peewee.TextField()
    cleanup_info = peewee.TextField()
    monitor_info = peewee.TextField()
    priority = peewee.IntegerField()
    state = peewee.CharField(default=ExecState.PENDING)
    start_time = peewee.DateTimeField(default=utils.get_now())
    end_time = peewee.DateTimeField(default=utils.get_now())

    class Meta:
        table_name = "test_job_suite"


class TestJobCase(BaseModel):
    job_id = peewee.IntegerField()
    test_suite_id = peewee.IntegerField()
    test_case_id = peewee.IntegerField()
    setup_info = peewee.TextField()
    cleanup_info = peewee.TextField()
    env_info = peewee.TextField()
    monitor_info = peewee.TextField()
    console = peewee.SmallIntegerField()
    need_reboot = peewee.BooleanField()
    priority = peewee.IntegerField()
    repeat = peewee.IntegerField()
    run_mode = peewee.CharField()
    note = peewee.CharField()
    server_provider = peewee.CharField(default=ServerProvider.ALI_CLOUD)
    server_object_id = peewee.IntegerField(default=0)
    server_tag_id = peewee.CharField()
    state = peewee.CharField(default=ExecState.PENDING)
    server_snapshot_id = peewee.IntegerField()
    start_time = peewee.DateTimeField(default=utils.get_now())
    end_time = peewee.DateTimeField(default=utils.get_now())

    class Meta:
        table_name = "test_job_case"

    @property
    def test_type(self):
        return TestSuite.get_by_id(self.test_suite_id).test_type

    @property
    def ws_id(self):
        return TestJob.get(id=self.job_id).ws_id

    @property
    def test_case(self):
        return TestCase.get_by_id(self.test_case_id)

    @property
    def test_suite(self):
        return TestSuite.get_by_id(self.test_suite_id)

    @property
    def server_tag_id_list(self):
        if self.server_tag_id:
            return [int(tag_id) for tag_id in str(self.server_tag_id).split(",")]
        return []


class TestStep(BaseModel):
    job_id = peewee.CharField()
    tid = peewee.CharField(default=None)
    stage = peewee.CharField()
    state = peewee.CharField(default=ExecState.RUNNING)
    job_suite_id = peewee.IntegerField()
    job_case_id = peewee.IntegerField()
    cluster_id = peewee.IntegerField()
    server = peewee.CharField()
    dag_step_id = peewee.IntegerField()
    result = peewee.TextField(default=None)
    log_file = peewee.CharField()

    class Meta:
        table_name = "test_step"

    @property
    def test_type(self):
        meta_data = json.loads(DagStepInstance.get(id=self.dag_step_id).step_data)
        return meta_data["test_type"]

    @property
    def server_ip(self):
        dag_step = DagStepInstance.get_by_id(self.dag_step_id)
        return dag_step.server_ip

    @property
    def private_ip(self):
        dag_step = DagStepInstance.get_by_id(self.dag_step_id)
        return dag_step.private_ip

    @property
    def env_info(self):
        dag_step = DagStepInstance.get_by_id(self.dag_step_id)
        return dag_step.env_info

    @property
    def server_sn(self):
        dag_step = DagStepInstance.get_by_id(self.dag_step_id)
        return dag_step.server_sn

    @property
    def dag_step(self):
        return DagStepInstance.get_by_id(self.dag_step_id)


class BuildJob(BaseModel):
    name = peewee.CharField(help_text='the build job name')
    state = peewee.CharField(default=ExecState.PENDING)
    build_from = peewee.CharField(default=BuildKernelFrom.MANUAL, help_text='build from')
    product_id = peewee.IntegerField(help_text='ProductModel.id')
    project_id = peewee.IntegerField(help_text='ProjectModel.id')
    arch = peewee.CharField(help_text='arch')
    build_env = peewee.CharField(help_text='json info about build environment')
    build_config = peewee.CharField(help_text='build configure')
    build_log = peewee.CharField(help_text='build log')
    build_file = peewee.CharField(help_text='build file')
    build_url = peewee.CharField(help_text='build url')
    git_repo = peewee.CharField(help_text='git_repo')
    git_branch = peewee.CharField(help_text='git_branch')
    git_commit = peewee.CharField(help_text='git_commit')
    git_url = peewee.CharField(help_text='git_url')
    commit_msg = peewee.CharField(help_text='commit_msg')
    committer = peewee.CharField(help_text='committer')
    compiler = peewee.CharField(help_text='compiler')
    description = peewee.CharField(help_text='the job description')
    cbp_id = peewee.IntegerField()
    tid = peewee.CharField()
    build_msg = peewee.TextField()
    rpm_list = peewee.TextField()
    creator = peewee.IntegerField()

    class Meta:
        db_table = 'build_job'


class MonitorInfo(BaseModel):
    state = peewee.BooleanField(default=False, help_text='监控是否成功')
    monitor_link = peewee.CharField(max_length=32, null=True, help_text='monitor link')
    is_open = peewee.BooleanField(default=True, help_text='监控是否开启')
    monitor_level = peewee.CharField(max_length=64, default='job', help_text='level')
    monitor_objs = peewee.TextField()
    object_id = peewee.IntegerField(null=True, help_text='Job id or JobCase id')
    server = peewee.CharField(max_length=32, null=True, help_text='sn or ip')
    remark = peewee.TextField(default='', help_text='备注')

    class Meta:
        db_table = 'monitor_info'


class TestTemplate(BaseModel):
    name = peewee.CharField()
    schedule_info = peewee.TextField()
    description = peewee.CharField()
    job_name = peewee.CharField()
    job_type_id = peewee.IntegerField()
    project_id = peewee.IntegerField()
    product_id = peewee.IntegerField()
    baseline_id = peewee.IntegerField()
    baseline_job_id = peewee.IntegerField()
    iclone_info = peewee.TextField()
    kernel_info = peewee.TextField()
    build_pkg_info = peewee.TextField()
    need_reboot = peewee.BooleanField()
    rpm_info = peewee.TextField()
    script_info = peewee.TextField()
    monitor_info = peewee.TextField()
    cleanup_info = peewee.TextField()
    notice_info = peewee.TextField()
    console = peewee.BooleanField()
    kernel_version = peewee.CharField()
    callback_api = peewee.CharField()
    report_name = peewee.CharField()
    report_template_id = peewee.IntegerField()
    env_info = peewee.TextField()
    enable = peewee.BooleanField()
    server_provider = peewee.CharField()
    creator = peewee.IntegerField()
    update_user = peewee.IntegerField()
    ws_id = peewee.CharField()

    class Meta:
        db_table = 'test_tmpl'


class JobType(BaseModel):

    name = peewee.CharField()
    enable = peewee.BooleanField()
    is_default = peewee.BooleanField()
    test_type = peewee.CharField()
    business_type = peewee.CharField()
    server_type = peewee.CharField()
    description = peewee.CharField()
    creator = peewee.IntegerField()
    ws_id = peewee.CharField()
    priority = peewee.IntegerField()
    is_first = peewee.BooleanField()

    class Meta:
        db_table = 'job_type'
