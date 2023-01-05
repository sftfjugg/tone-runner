import json
from enum import Enum
import re

from tools.config_parser import cp

APP_NAME = "tone-runner"


class BaseSourceData(type):
    def __new__(mcs, name, bases, attributes):
        for attr, value in attributes.items():
            attributes[attr] = APP_NAME + "-" + value
        return type.__new__(mcs, name, bases, attributes)


class NotifyType:
    DING_DING = 'ding'
    EMAIL = 'email'


class RunMode:
    STANDALONE = "standalone"
    CLUSTER = "cluster"


class RunStrategy:
    RAND = "rand"
    SPEC = "spec"
    TAG = "tag"


class StepStage:
    # 购买云机器、云上测试必须
    INIT_CLOUD = "init_cloud"
    # 构建内核包
    BUILD_PKG = "build_package"
    # 初始化、非必须
    INITIAL = "initial"
    # 重装机、非必须
    RE_CLONE = "re_clone"
    # 重启前安装rpm
    INSTALL_RPM_BEFORE_REBOOT = "install_rpm_before_reboot"
    # 重启前执行脚本
    SCRIPT_BEFORE_REBOOT = "script_before_reboot"
    # 重启，非必须
    REBOOT = "reboot"

    # 以下是安装内核阶段具体细分步骤
    # --------------安装内核开始---------------
    # 安装内核前脚本、非必需
    SCRIPT_BEFORE_INSTALL_KERNEL = "script_before_install_kernel"
    # 安装内核、非必需
    INSTALL_KERNEL = "install_kernel"
    # 安装 hot fix、非必需
    INSTALL_HOT_FIX = "install_hot" + "fix"
    # 安装内核后脚本、非必需
    SCRIPT_AFTER_INSTALL_KERNEL = "script_after_install_kernel"
    # 安装内核的重启
    REBOOT_FOR_INSTALL_KERNEL = "reboot_for_install_kernel"
    # 内核安装成功后检测内核
    CHECK_KERNEL_INSTALL = "check_kernel_install"
    # --------------安装内核结束---------------

    # 重启后安装rpm包、非必须
    INSTALL_RPM_AFTER_REBOOT = "install_rpm_after_reboot"
    # 重启后执行脚本、非必须
    SCRIPT_AFTER_REBOOT = "script_after_reboot"
    # 测试准备、必须
    PREPARE = "prepare"
    # job级别console
    JOB_CONSOLE = "job_console"
    # job级别监控
    JOB_MONITOR = "job_monitor"

    # 以下是TEST阶段具体细分步骤
    # ---------------suite开始---------------
    # suite开始前的重启
    REBOOT_BEFORE_SUITE = "reboot_before_suite"
    # suite级别的监控
    SUITE_MONITOR = "suite_monitor"
    # suite的setup脚本
    SCRIPT_BEFORE_SUITE = "script_before_suite"
    # 运行case前重启
    REBOOT_BEFORE_CASE = "reboot_before_case"
    # case级别监控
    CASE_MONITOR = "case_monitor"
    # case运行前的脚本
    SCRIPT_BEFORE_CASE = "script_before_case"
    # 执行case
    RUN_CASE = "run_case"
    # case运行后的脚本
    SCRIPT_AFTER_CASE = "script_after_case"
    # suite的teardown脚本
    SCRIPT_AFTER_SUITE = "script_after_suite"
    # ---------------suite结束---------------
    JOB_CLEANUP = "job_cleanup"

    TEST = (
        REBOOT_BEFORE_SUITE,
        SUITE_MONITOR,
        SCRIPT_BEFORE_SUITE,
        REBOOT_BEFORE_CASE,
        CASE_MONITOR,
        SCRIPT_BEFORE_CASE,
        RUN_CASE,
        SCRIPT_AFTER_CASE,
        SCRIPT_AFTER_SUITE
    )  # 整体仍可以统称test阶段

    JOB_END_POINT_WITH_SUITE = (
        REBOOT_BEFORE_SUITE,
        SCRIPT_BEFORE_SUITE,
    )

    SUITE_SET = (
        REBOOT_BEFORE_SUITE,
        SCRIPT_BEFORE_SUITE,
        SCRIPT_BEFORE_CASE,
        REBOOT_BEFORE_CASE,
        RUN_CASE
    )

    SUITE_END_POINT = (
        REBOOT_BEFORE_SUITE,
        SCRIPT_BEFORE_SUITE,
    )

    ONE_CASE_END_POINT = (
        SCRIPT_BEFORE_CASE,
        REBOOT_BEFORE_CASE
    )

    TEST_END_POINT = (
        SCRIPT_BEFORE_CASE,
        REBOOT_BEFORE_CASE,
        SCRIPT_BEFORE_CASE,
        REBOOT_BEFORE_CASE
    )

    ONE_CASE_SET = (
        SCRIPT_BEFORE_CASE,
        REBOOT_BEFORE_CASE,
        RUN_CASE
    )

    ONE_CASE_PRE_SET = (
        SCRIPT_BEFORE_CASE,
        SCRIPT_AFTER_CASE
    )

    REBOOT_SET = (
        REBOOT,
        REBOOT_FOR_INSTALL_KERNEL,
        REBOOT_BEFORE_SUITE,
        REBOOT_BEFORE_CASE
    )

    KERNEL_INSTALL_SET = (
        INITIAL,
        REBOOT,
        SCRIPT_BEFORE_INSTALL_KERNEL,
        INSTALL_KERNEL,
        INSTALL_HOT_FIX,
        SCRIPT_AFTER_INSTALL_KERNEL,
        REBOOT_FOR_INSTALL_KERNEL,
        CHECK_KERNEL_INSTALL
    )

    STEPS = (
        BUILD_PKG,
        INIT_CLOUD,
        INITIAL,
        RE_CLONE,
        INSTALL_RPM_BEFORE_REBOOT,
        SCRIPT_BEFORE_REBOOT,
        REBOOT,
        SCRIPT_BEFORE_INSTALL_KERNEL,
        INSTALL_KERNEL,
        INSTALL_HOT_FIX,
        SCRIPT_AFTER_INSTALL_KERNEL,
        REBOOT_FOR_INSTALL_KERNEL,
        CHECK_KERNEL_INSTALL,
        INSTALL_RPM_AFTER_REBOOT,
        SCRIPT_AFTER_REBOOT,
        PREPARE,
        JOB_CONSOLE,
        JOB_MONITOR,
        REBOOT_BEFORE_SUITE,
        SUITE_MONITOR,
        SCRIPT_BEFORE_SUITE,
        REBOOT_BEFORE_CASE,
        CASE_MONITOR,
        SCRIPT_BEFORE_CASE,
        RUN_CASE,
        SCRIPT_AFTER_CASE,
        SCRIPT_AFTER_SUITE,
        JOB_CLEANUP
    )

    NEED_UPLOAD_LOG_STAGE_SET = (
        INITIAL,
        PREPARE,
        INSTALL_KERNEL,
        INSTALL_RPM_BEFORE_REBOOT,
        INSTALL_RPM_AFTER_REBOOT,
    )

    @classmethod
    def check_steps(cls, steps) -> bool:
        if not steps:
            return False
        return all(map(cls._check_step, steps))

    @classmethod
    def _check_step(cls, step):
        return step in cls.STEPS

    @classmethod
    def wrong_steps(cls, steps):
        return set(steps) - set(cls.STEPS)


class ExecState:
    PENDING = "pending"
    PENDING_Q = "pending_q"
    RUNNING = "running"
    SKIP = "skip"
    STOP = "stop"
    SUCCESS = "success"
    FAIL = "fail"
    end = (SKIP, STOP, SUCCESS, FAIL)
    no_test_end = (SKIP, STOP, FAIL)
    skip_or_stop = (SKIP, STOP)
    no_end_set = {PENDING, RUNNING}
    all_set = {PENDING, PENDING_Q, RUNNING, SKIP, STOP, SUCCESS, FAIL}


class ClusterRole:
    LOCAL = 'local'
    REMOTE = "remote"


class AgentTaskState:
    INIT = 'init'
    RUNNING = 'running'
    FINISHED = 'finished'
    STOPPED = 'stopped'
    TIMEOUT = 'timeout'
    ERROR = 'error'


class DeviceType(object):
    CLOUD_SERVER = 'cloud_server'
    VM = 'vm'
    PHY_SERVER = 'phy_server'
    DOCKER = 'docker'


class ServerProvider(object):
    ALI_GROUP = "ali" + "group"
    ALI_CLOUD = 'ali' + "yun"


class TestType(object):
    PERFORMANCE = 'performance'
    FUNCTIONAL = 'functional'
    BUSINESS = 'business'
    STABILITY = 'stability'


class BaselineType(object):
    PERFORMANCE = 'perf_baseline'
    FUNCTIONAL = 'func_baseline'


class BaselineProvider(object):
    ALI_GROUP = 'ali' + "group"
    CLOUD_CLUSTER = 'cloud_cluster'
    ALI_CLOUD_ECS = 'ali' + "_yun" + "_ecs"
    ALI_CLOUD_ECI = 'ali' + "_yun" + "_eci"

    @staticmethod
    def get_provider(case_cluster, provider):
        if case_cluster:
            if provider == BaselineProvider.ALI_GROUP:
                return BaselineProvider.ALI_GROUP
            elif provider in (BaselineProvider.ALI_CLOUD_ECI, BaselineProvider.ALI_CLOUD_ECS):
                return BaselineProvider.CLOUD_CLUSTER
            else:
                raise NotImplementedError('provider: %s is not implemented!' % provider)
        return provider


class ServerState:
    AVAILABLE = "Available"
    OCCUPIED = "Occupied"
    BROKEN = "Broken"
    RESERVED = "Reserved"
    UNUSABLE = "Unusable"
    NO_AVAILABLE = (OCCUPIED, BROKEN)


class TaskExchange:
    EXEC_JOB_STEP = 'tone_exec_job_step'
    CHECK_JOB_STEP = 'tone_check_job_step'
    GENERAL = 'tone_general'


class ProcessDataSource(metaclass=BaseSourceData):
    # 1.远程
    # 1.1流程相关
    PENDING_PLAN = "pending_plan"  # 队列
    RUNNING_PLAN = "running_plan"  # 队列
    PENDING_JOB = "pending_job"  # 队列
    RUNNING_JOB = "running_job"  # 队列
    FAST_PENDING_DAG = "fast_pending_dag"  # 队列
    FAST_RUNNING_DAG = "fast_running_dag"  # 队列
    SLOW_PENDING_DAG = "slow_pending_dag"  # 队列
    SLOW_RUNNING_DAG = "slow_running_dag"  # 队列
    PENDING_DAG_NODE = "pending_dag_node"  # 队列
    RUNNING_DAG_NODE = "running_dag_node"  # 队列
    MASTER = "scheduler_master"  # 集合，只有一个master
    SLAVE = "scheduler_slave"  # 集合, 可以有多个slave
    MASTER_FINGERPRINT = "master_fingerprint"  # 字符串
    # 1.2机器分配或释放相关
    SERVER_USE_FREQ = "server_use_freq"  # 有序集合
    JOB_RELEASE_SERVER = "job_release_server"  # 哈希表
    USING_SERVER = "using_server"  # 哈希表
    PLAN_SERVER_LOCK = "plan_server_lock"  # 哈希表
    # 1.3 计划相关
    PLAN_INST_CTX = "plan_inst_ctx"  # 哈希表
    # 2.本地
    # 2.1对象实例等
    PLAN_INST = "plan_inst"  # 对象实例
    DAG_INST = "dag_inst"  # 对象实例
    DAG_NODE_INST = "dag_node_inst"  # 对象实例
    UPDATE_DAG_INST = "update_dag_inst"  # 对象实例
    FINGERPRINT = "fingerprint"  # 设备指纹
    # 3.检测机器状态的锁标识
    CHECK_REAL_STATE_LOCK = "check_real_state_lock"
    CHECK_SERVER_STATE_LOCK = "check_server_state_lock"
    # 4.最后检测到的机器id
    LAST_TEST_SERVER_ID = "last_test_server_id"
    LAST_CLOUD_SERVER_ID = "last_cloud_server_id"
    # 5.初始化锁
    # 5.1 程序初始化锁
    START_PROGRAM_LOCK = "start_program_lock"
    # 5.2 数据校正初始化锁
    INIT_CORRECT_DATA_LOCK = "init_correct_data_lock"


class OtherCache:
    plugin_version_name = "last_plugin_version"
    is_send_msg = "is_send_msg"


class OtherAgentResStatus:
    FINISH = "finish"
    NOT_FOUND = "not" + "found"

    end = (FINISH, NOT_FOUND)

    @classmethod
    def check_end(cls, status):
        return status in cls.end


class ToneAgentResStatus:
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"
    STOP = "stop"

    end = (SUCCESS, FAIL, STOP)
    complete_and_not_fail = (SUCCESS, STOP)

    @classmethod
    def check_end(cls, status):
        return status in cls.end


class OtherAgentRes:
    TID = "UID"
    SUCCESS = "SUCCESS"
    STATUS = "STATUS"
    ERROR_MSG = "ERROR" + "MSG"
    ERROR_CODE = "ERROR" + "CODE"
    JOB_RESULT = "JOB" + "RESULT"
    ERROR_CODE_LIST = ('a-501', 'a-507', 'a-508', 'a-510', 'a-512', 'a-515')
    AGENT_NOT_AVAILABLE = "Agent is not available"
    AGENT_DOWN = "task terminated because agent down"

    @classmethod
    def check_success(cls, success):
        return success


class ToneAgentRes:
    TID = "TID"
    SUCCESS = "TASK_STATUS"
    STATUS = "TASK_STATUS"
    ERROR_MSG = "ERROR_MSG"
    ERROR_CODE = "ERROR_CODE"
    JOB_RESULT = "TASK_RESULT"
    ERROR_URL = "ERROR_URL"
    ERROR_CODE_LIST = ('a-501', 'a-507', 'a-508', 'a-510', 'a-512', 'a-515')

    @classmethod
    def check_success(cls, success):
        return success in ToneAgentResStatus.complete_and_not_fail


class ICloneStatus:
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"


class BuildPackageStatus:
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"


class ObjectType:
    CLASS = "class"
    INSTANCE = "instance"


class CeleryTaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

    END = (SUCCESS, FAILURE)


class DbField:
    FALSE = 0
    TRUE = 1
    NULL = None
    EMPTY = ""


class SpecUseType:
    NO_SPEC_USE = 0
    USE_BY_CLUSTER = 1
    USE_BY_JOB = 2


class ServerFlowFields:
    JOB_SUITE_ID = "job_suite_id"
    JOB_CASE_ID = "job_case_id"
    TEST_TYPE = "test_type"
    BASELINE_ID = "baseline_id"
    BASELINE_JOB_ID = "baseline_job_id"
    SERVER_PROVIDER = "server_provider"
    RUN_MODE = "run_mode"
    RUN_STRATEGY = "run_strategy"
    OLD_SERVER_ID = "old_server_id"  # 用于云上测试模板，当生成新的实例后会把原来的server_id的值给old_server_id，新实例的id给server_id
    SERVER_ID = "server_id"
    SERVER_SNAPSHOT_ID = "server_snapshot_id"
    TAG_ID_LIST = "tag_id_list"
    SERVER_IP = "ip"
    PRIVATE_IP = "private_ip"
    SERVER_SN = "sn"
    SERVER_TSN = "tsn"
    IS_INSTANCE = "is_instance"
    CHANNEL_TYPE = "channel_type"
    READY = "ready"
    SERVER_STATE = "server_state"
    CLUSTER_ID = "cluster_id"
    SNAPSHOT_CLUSTER_ID = "snapshot_cluster_id"
    CLUSTER_SERVERS = "cluster_servers"
    ROLE = "role"
    BASELINE_SERVER = "baseline_server"
    KERNEL_INSTALL = "kernel_install"
    VAR = "var_name"
    WS_ID = "ws_id"
    JOB_ID = "job_id"
    USING_ID = "using_id"  # plan or job
    DAG_ID = "dag_id"
    STEP = "step"
    ORDER = "order"
    DAG_STEP_ID = "dag_step_id"
    CLOUD_INST_META = "cloud_inst_meta"
    IN_POOL = "in_pool"
    BE_USING = "be_using"
    BROKEN = "broken"
    SERVER_OS_TYPE = 'server_os_type'


class JobCfgFields:
    CREATOR = "creator"
    ENV_INFO = "env_info"
    ICLONE_INFO = "i" + "clone_info"
    RPM_INFO = "rpm_info"
    SCRIPT = "script"
    BUILD_PKG_INFO = "build_pkg_info"
    KERNEL_INFO = "kernel_info"
    SCRIPTS = "scripts"
    POS = "pos"
    RPM = "rpm"
    KERNEL = "kernel"
    DEV = "dev" + "el"
    HEADERS = "headers"
    KERNEL_PACKAGES = "kernel_packages"
    HOT_FIX = "hot" + "fix" + 'install'
    CONSOLE = "console"
    MONITOR_INFO = "monitor_info"
    KERNEL_VERSION = "kernel_version"
    REBOOT_TIME = "reboot_time"


class ServerReady:
    READY = 1
    NO_READY = 0


class ChannelType:
    OTHER_AGENT = "other" + "agent"
    TONE_AGENT = "tone" + "agent"
    CLOUD_AGENT = "cloud" + "agent"  # 过渡阶段使用，后期可能会统一为TONE_AGENT


class OtherAgentScript:
    SSH_PUB_KEY = "tone/SSH_PUB_KEY"
    INITIAL = "tone/TONE_CLEANUP"
    INSTALL_RPM = "tone/TONE_INSTALL_RPM"
    INSTALL = "tone/TONE_INSTALL"
    INSTALL_HOTFIX = "tone/TONE_INSTALL_HOTFIX"
    PREPARE = "tone/TONE_PREPARE"
    REBOOT = "tone/REBOOT"
    SYNC = "tone/TONE_SYNC"
    RUN_TEST = "tone/TONE_TEST"
    UPLOAD = "tone/TONE_UPLOAD"


class OtherScriptTone:
    SSH_PUB_KEY = "tone/SSH_PUB_KEY"
    INITIAL = "tone/TONE_CLEANUP"
    INSTALL_RPM = "tone/TONE_INSTALL_RPM"
    INSTALL = "tone/TONE_INSTALL"
    INSTALL_HOTFIX = "tone/TONE_INSTALL_HOTFIX"
    PREPARE = "tone/TONE_PREPARE_NEW"
    REBOOT = "tone/REBOOT"
    SYNC = "tone/TONE_SYNC"
    RUN_TEST = "tone/TONE_TEST_NEW"
    UPLOAD = "tone/TONE_UPLOAD_NEW"


class ToneAgentScript:
    SSH_PUB_KEY = "SSH_PUB_KEY"
    INITIAL = "TONE_INITIAL"
    INSTALL_RPM = "TONE_INSTALL_RPM"
    INSTALL = "TONE_INSTALL"
    INSTALL_HOTFIX = "TONE_INSTALL_HOTFIX"
    PREPARE = "TONE_PREPARE"
    REBOOT = "REBOOT"
    RUN_TEST = "TONE_TEST"
    RUN_TEST_DEBIAN = "RUN_TEST_DEBIAN"
    UPLOAD = "TONE_UPLOAD"


class ToneAgentScriptTone:
    SSH_PUB_KEY = "SSH_PUB_KEY"
    INITIAL = "INITIAL"
    INITIAL_DEBIAN = "INITIAL_DEBIAN"
    INSTALL_RPM = "INSTALL_RPM"
    INSTALL_RPM_DEBIAN = "INSTALL_RPM_DEBIAN"
    INSTALL = "INSTALL_KERNEL"
    INSTALL_DEBIAN = "INSTALL_KERNEL_DEBIAN"
    INSTALL_HOTFIX_DEBIAN = "INSTALL_HOTFIX_DEBIAN"
    PREPARE = "PREPARE"
    PREPARE_DEBIAN = "PREPARE_DEBIAN"
    REBOOT = "REBOOT"
    RUN_TEST = "RUN_TEST"
    RUN_TEST_DEBIAN = "RUN_TEST_DEBIAN"
    RUN_TEST_CLOUD = "RUN_TEST"
    UPLOAD = "UPLOAD"
    UPLOAD_CLOUD = "UPLOAD"
    UPLOAD_FILE = "FILE"
    DEPLOY_AGENT = "DEPLOY_AGENT"
    DEPLOY_AGENT_DEBIAN = "DEPLOY_AGENT_DEBIAN"


class JobSysVar:
    TONE_JOB_ID = "TONE_JOB_ID"
    KHOTFIX_INSTALL = "KHOTFIX_INSTALL"


class Position:
    BEFORE = "before"
    AFTER = "after"


class SuiteCaseFlag:
    FIRST = "first"
    LAST = "last"


class TidTypeFlag:
    AGENT_TID = "API"
    I_CLONE_TID = "IcloneTid"
    BUILD_PKG_TID = "BuildPkgTid"
    BUSINESS_TEST_TID = "BusinessTestTid_"


FUNC_CASE_RESULT_TYPE_MAP = {
    'PASS': 1, 'FAIL': 2, 'CONF': 3, 'BLOCK': 4, 'SKIP': 5,
}


class CaseResult(Enum):
    PASS = 1
    FAIL = 2
    CONF = 3
    BLOCK = 4
    SKIP = 5

    @classmethod
    def member_map(cls):
        return cls._member_map_

    @classmethod
    def success(cls):
        return "SUCCESS"


class CaseResultTone(Enum):
    Pass = 1
    Fail = 2
    Skip = 5
    Warning = 6


class PerfResultType:
    INDICATOR = "indicator"


def get_agent_res_obj(channel_type):
    if channel_type == ChannelType.OTHER_AGENT:
        return OtherAgentRes
    else:
        return ToneAgentRes


def get_agent_res_status_obj(channel_type):
    if channel_type == ChannelType.OTHER_AGENT:
        return OtherAgentResStatus
    else:
        return ToneAgentResStatus


class TestMetricType:
    SUITE = "suite"
    CASE = "case"


class TrackResult:
    INCREASE = "increase"
    DECLINE = "decline"
    NORMAL = "normal"
    INVALID = "invalid"
    NA = "na"


class BaseConfigType:
    SYS = "sys"
    SCRIPT = "script"


class BaseConfigKey:
    TEST_FRAMEWORK = "TEST_FRAMEWORK"
    AUTO_RECOVER_SERVER = "AUTO_RECOVER_SERVER"
    RECOVER_SERVER_PROTECT_DURATION = "RECOVER_SERVER_PROTECT_DURATION"


class TestFrameWork:
    TONE = "tone"
    AKTF = "aktf"


class NoticeMsgType:
    JOB_COMPLETE = "job_complete"


class MsgServersForDev:
    SERVERS = json.loads(cp.get('kafka_servers_list'))


class MsgServersForPro:
    SERVERS = json.loads(cp.get('kafka_servers_list'))


class MsgTopics:
    JOB_TOPIC = 'tone-job-topic'
    PLAN_TOPIC = 'tone-plan-topic'
    MACHINE_TOPIC = 'tone-machine-topic'
    REPORT_TOPIC = 'tone-report-topic'


class MsgGroups:
    JOB_GROUP = 'tone-job-group'
    PLAN_GROUP = 'tone-plan-group'
    MACHINE_GROUP = 'tone-machine-group'
    REPORT_GROUP = 'tone-report-group'


class MsgKey:
    JOB_STATE_CHANGE = "job_state_change"
    MACHINE_BROKEN = "machine_broken"
    PLAN_STATE_CHANGE = "plan_state_change"
    JOB_SAVE_REPORT = "job_save_report"
    JOB_START_RUNNING = "job_start_running"


class PlanRunMode:
    AUTO = "auto"
    MANUAL = "manual"


class PlanStage:
    BUILD_KERNEL = "build_kernel"
    PREPARE_ENV = "prepare_env"
    TEST_STAGE = "test_stage"

    STAGE_ENUM = (BUILD_KERNEL, PREPARE_ENV, TEST_STAGE)

    @classmethod
    def check_stage(cls, stage):
        assert stage in cls.STAGE_ENUM, f"{stage} is not support!"


class BuildKernelFrom:
    CBP = "cbp"
    JENKINS = "jenkins"
    MANUAL = "manual"
    JOB = "job"
    PLAN = "plan"


class UsingIdFlag:
    PLAN_INST = "_plan_inst"


class PlanServerLockBit:
    NULL = 0
    LOCK = 1
    UNLOCK = 2


class QueueType:
    FAST = "fast"
    SLOW = "slow"


class QueueState:
    PENDING = "pending"
    RUNNING = "running"


class QueueName:
    PLAN = "plan"
    JOB = "job"
    DAG = "dag"
    DAG_NODE = "dag_node"


class RebootStep:
    TIMEOUT = 60


class ServerAttribute:
    IP_PATTEN = re.compile(r'^(\d+\.\d+\.\d+\.\d+)')
    IP = 'ip'
    SN = 'sn'


class MonitorParam:
    QUERY = "query"
    QUERY_TYPE = "query_type"
    EMP_ID = "emp_id"
    SUCCESS_CODES = [200, 201]


class MonitorType:
    CASE_MACHINE = 'case_machine'
    CUSTOM_MACHINE = 'custom_machine'


class ReleaseRule:
    DONT_RELEASE = 0
    RELEASE = 1
    DELAY_RELEASE = 2


class LogFilePath:
    PREPARE = '/tmp/prepare_tone.log'
    INITIAL = '/tmp/initial.log'
    INSTALL_KERNEL = '/tmp/install_kernel.log'
    INSTALL_RPM = '/tmp/install_rpm.log'
    INIT_CLOUD = '/tmp/deploy_agent.log'
