import uuid

from lib import peewee
from constant import ServerState, RunMode, ServerProvider, ChannelType
from .base import BaseModel
from .sysconfig import OperationLogs


class TestServer(BaseModel):
    ip = peewee.CharField()
    sn = peewee.CharField()
    tsn = peewee.CharField()
    name = peewee.CharField()
    hostname = peewee.CharField()
    # 逻辑
    app_group = peewee.CharField()
    security_domain = peewee.CharField(max_length=64)
    idc = peewee.CharField()
    parent_server_id = peewee.IntegerField()
    # 硬件型号
    manufacturer = peewee.CharField()
    device_type = peewee.CharField()
    device_mode = peewee.CharField()
    sm_name = peewee.CharField()
    # 硬件配置
    arch = peewee.CharField()
    cpu = peewee.CharField()
    memory = peewee.CharField()
    storage = peewee.CharField()
    cpu_device = peewee.CharField()
    memory_device = peewee.CharField()
    network = peewee.CharField()
    net_device = peewee.CharField()
    # 软件
    kernel = peewee.CharField()
    platform = peewee.CharField()
    uname = peewee.CharField()
    # 状态及其它
    use_type = peewee.CharField(max_length=64)
    state = peewee.CharField(default=ServerState.AVAILABLE)
    channel_type = peewee.CharField()
    private_ip = peewee.CharField()
    in_pool = peewee.BooleanField(default=True, help_text="是否在资源池")
    spec_use = peewee.SmallIntegerField(default=0,
                                        help_text="是否被集群或job使用, "
                                                  "1被集群使用，2被job使用")
    owner = peewee.IntegerField()
    description = peewee.CharField()
    real_state = peewee.CharField()
    history_state = peewee.CharField(default=ServerState.AVAILABLE, help_text='历史状态')
    check_state_time = peewee.DateTimeField()
    occupied_job_id = peewee.IntegerField()
    ws_id = peewee.CharField()
    broken_job_id = peewee.IntegerField()
    broken_at = peewee.DateTimeField()
    broken_reason = peewee.TextField()

    class Meta:
        table_name = "test_server"

    @property
    def server_ip(self):
        return self.ip

    @property
    def server_sn(self):
        if self.channel_type == ChannelType.OTHER_AGENT:
            return self.sn
        else:
            return self.tsn if self.tsn else self.sn

    @property
    def server_tsn(self):
        return self.tsn

    @property
    def server_provider(self):
        return ServerProvider.ALI_GROUP


class CloudServer(BaseModel):
    job_id = peewee.IntegerField()
    provider = peewee.CharField()
    region = peewee.CharField()
    zone = peewee.CharField()
    manufacturer = peewee.CharField(help_text='云厂商')
    ak_id = peewee.IntegerField(default=1, help_text='AK id')
    image = peewee.CharField()
    image_name = peewee.CharField()
    bandwidth = peewee.CharField()
    storage_type = peewee.CharField(help_text='存储类型')
    storage_size = peewee.CharField(help_text='存储大小')
    storage_number = peewee.CharField(help_text='存储数量')
    system_disk_category = peewee.CharField(help_text='系统盘类型', default='cloud_ssd')
    system_disk_size = peewee.CharField(help_text='系统盘大小', default=50)
    extra_param = peewee.TextField(help_text='扩展信息')
    release_rule = peewee.IntegerField(default=1)
    # 模板
    template_name = peewee.CharField(help_text='模板名称')
    # 1、instance:用户指定，2、模板：3、由模板产生的实例，根据以下两个字段区分
    instance_id = peewee.CharField()
    parent_server_id = peewee.IntegerField(help_text='父级id')
    instance_name = peewee.CharField()
    instance_type = peewee.CharField()
    is_instance = peewee.BooleanField(default=True, help_text='是否实例')
    pub_ip = peewee.CharField()
    private_ip = peewee.CharField()
    hostname = peewee.CharField()
    port = peewee.CharField()
    kernel_version = peewee.CharField()
    state = peewee.CharField(default=ServerState.AVAILABLE)
    channel_type = peewee.CharField()
    sn = peewee.CharField()
    tsn = peewee.CharField()
    spec_use = peewee.SmallIntegerField(default=0,
                                        help_text="是否被集群或job使用, "
                                                  "1被集群使用，2被job使用")
    owner = peewee.IntegerField()
    description = peewee.CharField()
    real_state = peewee.CharField()
    history_state = peewee.CharField(default=ServerState.AVAILABLE, help_text='历史状态')
    check_state_time = peewee.DateTimeField()
    occupied_job_id = peewee.IntegerField()
    ws_id = peewee.CharField()
    in_pool = peewee.BooleanField(default=True, help_text="是否在资源池")
    broken_job_id = peewee.IntegerField()
    broken_at = peewee.DateTimeField()
    broken_reason = peewee.TextField()

    class Meta:
        table_name = "cloud_server"

    @property
    def server_ip(self):
        return self.private_ip

    @property
    def server_sn(self):
        if self.instance_id:
            return self.instance_id
        return ""

    @property
    def server_tsn(self):
        return self.tsn

    @property
    def server_provider(self):
        return ServerProvider.ALI_CLOUD


class TestCluster(BaseModel):
    name = peewee.CharField()
    cluster_type = peewee.CharField()
    is_occpuied = peewee.BooleanField(help_text='是否被占用', default=False)
    occupied_job_id = peewee.IntegerField()
    ws_id = peewee.CharField()
    owner = peewee.IntegerField(null=True, help_text='Owner')
    description = peewee.CharField(help_text='描述')

    class Meta:
        table_name = "test_cluster"


class TestClusterServer(BaseModel):
    cluster_id = peewee.IntegerField()
    cluster_type = peewee.CharField()
    server_id = peewee.IntegerField()
    role = peewee.CharField()
    baseline_server = peewee.BooleanField(default=False)
    kernel_install = peewee.BooleanField(help_text='是否安装内核')
    var_name = peewee.CharField(max_length=64, help_text='变量名')

    class Meta:
        table_name = "test_cluster_server"


class TestServerSnapshot(BaseModel):
    ip = peewee.CharField()
    sn = peewee.CharField()
    tsn = peewee.CharField()
    name = peewee.CharField()
    hostname = peewee.CharField()
    # 逻辑
    app_group = peewee.CharField()
    app_state = peewee.CharField()
    security_domain = peewee.CharField()
    idc = peewee.CharField()
    parent_server_id = peewee.IntegerField()
    # 硬件型号
    manufacturer = peewee.CharField()
    device_type = peewee.CharField()
    device_mode = peewee.CharField()
    sm_name = peewee.CharField()
    # 硬件配置
    arch = peewee.CharField()
    cpu = peewee.CharField()
    memory = peewee.CharField()
    storage = peewee.CharField()
    cpu_device = peewee.CharField()
    memory_device = peewee.CharField()
    network = peewee.CharField()
    net_device = peewee.CharField()
    # 软件
    kernel = peewee.CharField()
    platform = peewee.CharField()
    uname = peewee.CharField()
    # 状态及其它
    state = peewee.CharField()
    real_state = peewee.CharField()
    history_state = peewee.CharField(default=ServerState.AVAILABLE, help_text='历史状态')
    check_state_time = peewee.DateTimeField()
    # state=1 use_type:平台或者用户手动reserve
    use_type = peewee.CharField()
    description = peewee.CharField()
    channel_type = peewee.CharField()
    channel_state = peewee.BooleanField(help_text='控制通道状态，是否部署完成')
    private_ip = peewee.CharField()
    owner = peewee.IntegerField()
    ws_id = peewee.CharField()
    # 机器管理只展示单机池中的机器
    in_pool = peewee.BooleanField(default=True, help_text='是否在单机池中')
    console_type = peewee.CharField()
    console_conf = peewee.CharField()
    spec_use = peewee.SmallIntegerField(default=0,
                                        help_text="是否被job或集群指定使用, "
                                                  "1被集群使用，2被job使用")
    occupied_job_id = peewee.IntegerField(help_text='被哪个任务所占用')
    source_server_id = peewee.IntegerField(help_text='来源机器id')
    job_id = peewee.IntegerField()
    gcc = peewee.CharField()
    distro = peewee.CharField()
    rpm_list = peewee.TextField()
    glibc = peewee.TextField()
    memory_info = peewee.TextField()
    disk = peewee.TextField()
    cpu_info = peewee.TextField()
    ether = peewee.TextField()
    kernel_version = peewee.CharField()
    product_version = peewee.CharField()
    broken_job_id = peewee.IntegerField()
    broken_at = peewee.DateTimeField()
    broken_reason = peewee.TextField()

    class Meta:
        db_table = 'test_server_snapshot'

    @property
    def server_ip(self):
        return self.ip

    @property
    def server_sn(self):
        if self.channel_type == ChannelType.OTHER_AGENT:
            return self.sn
        else:
            return self.tsn if self.tsn else self.sn

    @property
    def server_tsn(self):
        return self.tsn

    @property
    def server_provider(self):
        return ServerProvider.ALI_GROUP


class CloudServerSnapshot(BaseModel):
    job_id = peewee.IntegerField(help_text='关联Job')
    provider = peewee.CharField()
    region = peewee.CharField()
    zone = peewee.CharField()
    manufacturer = peewee.CharField()
    ak_id = peewee.IntegerField()

    image = peewee.CharField()
    image_name = peewee.CharField()
    bandwidth = peewee.IntegerField()
    storage_type = peewee.CharField()
    storage_size = peewee.CharField()
    storage_number = peewee.CharField()
    system_disk_category = peewee.CharField(help_text='系统盘类型', default='cloud_ssd')
    system_disk_size = peewee.CharField(help_text='系统盘大小', default=50)
    extra_param = peewee.TextField(help_text='扩展信息')
    sn = peewee.CharField()
    tsn = peewee.CharField()
    release_rule = peewee.IntegerField()
    # 模板
    template_name = peewee.CharField()

    # 实例
    instance_id = peewee.CharField()
    instance_name = peewee.CharField()

    instance_type = peewee.CharField()
    # 1、instance:用户指定，2、模板：3、由模板产生的实例，根据以下两个字段区分
    is_instance = peewee.BooleanField(default=True, help_text='是否实例')
    parent_server_id = peewee.IntegerField(help_text='父级id')
    owner = peewee.IntegerField()
    description = peewee.CharField()
    # 待定字段
    private_ip = peewee.CharField()
    pub_ip = peewee.CharField()
    hostname = peewee.CharField()
    port = peewee.IntegerField()
    kernel_version = peewee.CharField(max_length=64, help_text='内核版本')
    state = peewee.CharField()
    channel_type = peewee.CharField()
    ws_id = peewee.CharField()
    console_type = peewee.CharField()
    console_conf = peewee.CharField()
    spec_use = peewee.SmallIntegerField(default=0,
                                        help_text="是否被job或集群指定使用, "
                                                  "1被集群使用，2被job使用")
    real_state = peewee.CharField()
    history_state = peewee.CharField(default=ServerState.AVAILABLE, help_text='历史状态')
    check_state_time = peewee.DateTimeField()
    occupied_job_id = peewee.IntegerField(help_text='被哪个任务所占用')
    in_pool = peewee.BooleanField(default=True, help_text='是否在单机池中')
    source_server_id = peewee.IntegerField(help_text='来源机器id')
    arch = peewee.CharField()
    gcc = peewee.CharField()
    distro = peewee.CharField()
    rpm_list = peewee.TextField()
    glibc = peewee.TextField()
    memory_info = peewee.TextField()
    disk = peewee.TextField()
    cpu_info = peewee.TextField()
    ether = peewee.TextField()
    product_version = peewee.CharField()
    broken_job_id = peewee.IntegerField()
    broken_at = peewee.DateTimeField()
    broken_reason = peewee.TextField()

    class Meta:
        db_table = 'cloud_server_snapshot'

    @property
    def server_ip(self):
        return self.pub_ip

    @property
    def server_sn(self):
        if self.instance_id:
            return self.instance_id
        return ""

    @property
    def server_tsn(self):
        return self.tsn

    @property
    def server_provider(self):
        return ServerProvider.ALI_CLOUD


class TestClusterSnapshot(BaseModel):
    name = peewee.CharField(help_text='名称')
    cluster_type = peewee.CharField(help_text='集群类型')
    is_occpuied = peewee.BooleanField(help_text='是否被占用', default=False)
    ws_id = peewee.CharField(help_text='ws_id')
    owner = peewee.IntegerField(help_text='Owner')
    description = peewee.CharField(help_text='描述')
    source_cluster_id = peewee.IntegerField(help_text='来源集群id')
    job_id = peewee.IntegerField()

    class Meta:
        db_table = 'test_cluster_snapshot'


class TestClusterServerSnapshot(BaseModel):
    cluster_id = peewee.IntegerField(help_text='集群id')
    server_id = peewee.IntegerField(help_text='关联单机id')
    cluster_type = peewee.CharField(help_text='集群类型')
    role = peewee.CharField(help_text='集群角色')
    baseline_server = peewee.BooleanField(help_text='是否是基线机器', default=False)
    kernel_install = peewee.BooleanField(help_text='是否安装内核', default=False)
    var_name = peewee.CharField(help_text='变量名')
    source_cluster_server_id = peewee.IntegerField(help_text='来源集群机器id')
    job_id = peewee.IntegerField()

    class Meta:
        db_table = 'test_cluster_server_snapshot'


class ServerTag(BaseModel):
    name = peewee.CharField()
    description = peewee.CharField()

    class Meta:
        table_name = "server_tag"


class ServerTagRelation(BaseModel):
    run_environment = peewee.CharField(default=ServerProvider.ALI_GROUP)
    object_type = peewee.CharField(default=RunMode.STANDALONE)
    object_id = peewee.IntegerField()
    server_tag_id = peewee.IntegerField()

    class Meta:
        table_name = "server_tag_relation"


class CloudAk(BaseModel):
    name = peewee.CharField(max_length=64)
    provider = peewee.CharField(max_length=64)
    access_id = peewee.CharField(max_length=128)
    access_key = peewee.CharField(max_length=1024)
    resource_group_id = peewee.CharField(max_length=32)
    vm_quota = peewee.CharField(max_length=8)
    description = peewee.CharField(max_length=1024)
    ws_id = peewee.CharField(max_length=8)

    class Meta:
        db_table = 'cloud_ak'


def get_server_model_by_provider(provider):
    if provider == ServerProvider.ALI_GROUP:
        return TestServer
    else:
        return CloudServer


def update_server_state_log(provider, server_id, to_broken=True):
    server_model = get_server_model_by_provider(provider)
    operation_sn = str(uuid.uuid4())
    table_name = server_model._meta.table_name
    old_values = '{"state": "Available"}'
    new_values = '{"state": "Broken"}'
    if not to_broken:
        old_values, new_values = new_values, old_values
    operation_type = 'update'
    OperationLogs.create(operation_type=operation_type,
                         operation_object='machine_{}'.format(table_name),
                         db_table=table_name,
                         db_pid=server_id, operation_sn=operation_sn,
                         old_values=old_values,
                         new_values=new_values)


def get_snapshot_server_by_provider(provider):
    if provider == ServerProvider.ALI_GROUP:
        return TestServerSnapshot
    else:
        return CloudServerSnapshot


def get_server_or_snapshot_server(in_pool, provider):
    if in_pool:
        if provider == ServerProvider.ALI_GROUP:
            return TestServer
        else:
            return CloudServer
    else:
        if provider == ServerProvider.ALI_GROUP:
            return TestServerSnapshot
        else:
            return CloudServerSnapshot


class ServerRecoverRecord(BaseModel):
    sn = peewee.CharField(max_length=64, help_text='SN')
    ip = peewee.CharField(max_length=64, help_text='IP')
    reason = peewee.TextField(null=True, help_text='故障恢复原因')
    broken_at = peewee.DateTimeField(null=True, help_text='故障时间')
    recover_at = peewee.DateTimeField(null=True, help_text='恢复时间')
    broken_job_id = peewee.IntegerField(null=True, help_text='关联机器故障job')

    class Meta:
        db_table = 'server_recover_record'


class ReleaseServerRecord(BaseModel):
    server_id = peewee.IntegerField(unique=True, help_text='机器ID')
    server_instance_id = peewee.CharField(help_text='机器实例ID')
    estimated_release_at = peewee.DateTimeField(help_text='预计释放时间')
    is_release = peewee.BooleanField(default=False, help_text='是否已释放')
    release_at = peewee.DateTimeField(help_text='释放时间')

    class Meta:
        db_table = 'release_server_record'