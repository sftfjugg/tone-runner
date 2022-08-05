from tools.config_parser import cp

# env
ENV_TYPE = cp.get("env")

# mysql
DB = cp.get("db_name")
HOST = cp.get("db_host")
PORT = 3306
USER = cp.get("db_user")
PASSWORD = cp.get("db_password")
DATABASE = {
    "database": DB,
    "host": HOST,
    "port": PORT,
    "user": USER,
    "passwd": PASSWORD
}

# redis
REDIS_CACHE_DB = cp.get("cache_db")
REDIS_HOST = cp.get("redis_host")
REDIS_PORT = cp.get("redis_port")
REDIS_PASSWORD = cp.get("redis_password")
# REDIS_PASSWORD = (
#     "{0}:{1}".format(
#         cp.get("redis_username"),
#         cp.get("redis_password")
#     )
# )
CELERY_BROKER_DB = cp.get("celery_broker_db")
CELERY_BACKEND_DB = cp.get("celery_backend_db")

# celery
CELERY_BROKER_ADDRESS = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/{CELERY_BROKER_DB}"
CELERY_BACKEND_ADDRESS = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/{CELERY_BACKEND_DB}"

# tone agent
TONE_AGENT_DOMAIN = cp.get("toneagent_domain")
TONE_AGENT_ACCESS_KEY = cp.get("toneagent_access_key")
TONE_AGENT_SECRET_KEY = cp.get("toneagent_secret_key")
AGENT_MAX_TIMEOUT = 60 * 60 * 60 * 2

# agent common
AGENT_REQUEST_MAX_RETRIES = int(cp.get("agent_request_max_retries", 5))
AGENT_REQUEST_INTERVAL = int(cp.get("agent_request_interval", 3))

# log
LOG_LEVEL = cp.get("log_level")
LOG_PATH = cp.get("log_path")

# ali cloud
ALY_ACCESS_ID = cp.get("oss_access_key")
ALY_ACCESS_KEY = cp.get("oss_secret_key")
ALY_OSS_BUCKET = cp.get("oss_bucket")
ALY_OSS_ENDPOINT = cp.get("oss_endpoint")
ALIY_OSS_HEAD = "http://" + ALY_OSS_BUCKET + '.' + ALY_OSS_ENDPOINT

TONE_STORAGE_HOST = cp.get('tone_storage_host')
TONE_STORAGE_PROXY_PORT = cp.get('tone_storage_proxy_port')
TONE_STORAGE_SFTP_PORT = cp.get('tone_storage_sftp_port')
TONE_STORAGE_BUCKET = cp.get('tone_storage_bucket', 'results')

ECS_LOGIN_PASSWORD = cp.get("ecs_login_password")
DEFAULT_VPC_CIDR_BLOCK = "172.16.0.0/16"
DEFAULT_VSWITCH_CIDR = "172.16.0.0/24"
DEFAULT_VPC_NAME = "ToneDefaultVPC"
DEFAULT_VSWITCH_NAME = "ToneDefaultVSWITCH"
DEFAULT_EIP_NAME = "ToneDefaultEIP"
WORK_ACCESS_CIDR_BLOCK_LIST = [
    "0.0.0.0/0"
]
SECURITY_GROUP_DROP_CIDR_BLOCK_LIST = []

# scheduler
PROCESS_PLAN_NUM = int(cp.get("process_job_num", 6))
PROCESS_JOB_NUM = int(cp.get("process_job_num", 6))
PROCESS_SCHEDULER_INTERVAL = int(cp.get("process_scheduler_interval", 15))
PLAN_CACHE_INST_EXPIRE = int(cp.get("plan_cache_inst_expire", 3600 * 24 * 3))

# dag engine
MAX_SAME_PULL_TIMES = int(cp.get("max_same_pull_times", 2))
GENERATE_DAG_NUM = int(cp.get("generate_dag_num", 5))
FAST_PROCESS_DAG_NUM = int(cp.get("fast_process_dag_num", 5))
SLOW_PROCESS_DAG_NUM = int(cp.get("slow_process_dag_num", 300))
PROCESS_DAG_NODE_NUM = int(cp.get("process_dag_node_num", 15))
PULL_DAG_MAX_RETRIES = int(cp.get("pull_dag_max_retries", 2))
PULL_DAG_NODE_MAX_RETRIES = int(cp.get("pull_dag_node_max_retries", 3))
FAST_ENGINE_INTERVAL = int(cp.get("fast_engine_interval", 15))
SLOW_ENGINE_INTERVAL = int(cp.get("slow_engine_interval", 900))
CORRECT_DATA_INTERVAL = int(cp.get("correct_data_interval", 15))  # 间隔时间及节点过期时间可能需要根据不同的部署模式适当调整
SLAVE_EXPIRE = int(cp.get("slave_expire", 300))  # 过期时间需要大于CORRECT_DATA_INTERVAL时间
CACHE_INST_EXPIRE = int(cp.get("cache_inst_expire", 3600 * 24))
CHECK_DAG_NODE_TIMEOUT = int(cp.get("check_dag_node_timeout", 60 * 5))
CHECK_DAG_STEP_EXCEPTION_TIMEOUT = int(cp.get("check_dag_step_exception_timeout", 90))
FINGERPRINT_FILE = cp.get("fingerprint_file")
INIT_CORRECT_DATA_LOCK_TIMEOUT = int(cp.get("init_correct_data_lock_timeout", 300))
START_PROGRAM_LOCK_TIMEOUT = int(cp.get("start_program_lock_timeout", 15))

# step
PARALLEL = int(cp.get("parallel", 3))
REPEAT_BASE = int(cp.get("repeat_base", 10000))
INSTALL_TIMEOUT = int(cp.get("install_timeout", 60 * 60))
CLOUD_INSTALL_TIMEOUT = int(cp.get("cloud_install_timeout", 5))
CLEANUP_TIMEOUT = int(cp.get("step_timeout", 60 * 5))
SSH_FREE_TIMEOUT = int(cp.get("ssh_free_timeout", 30))
PREPARE_TIMEOUT = int(cp.get("prepare_timeout", 60 * 20))
CUSTOM_SCRIPT_TIMEOUT = int(cp.get("custom_script_timeout", 300))
SYNC_SCRIPT_TIMEOUT = int(cp.get("sync_script_timeout", 180))
PERFORMANCE_REPEAT = int(cp.get("performance_repeat", 3))
REBOOT_TIMEOUT = int(cp.get("reboot_timeout", 2))
UPLOAD_LOG_TIMEOUT = int(cp.get("upload_log_timeout", 60 * 5))
CHECK_REBOOT_RETRIES = int(cp.get("check_reboot_retries", 10))
CHECK_REBOOT_INTERVAL = int(cp.get("check_reboot_interval", 60))
TONE_PATH = cp.get("tone_path", "/tmp/tone")
SSH_FREE_AUTH_FILE = cp.get("ssh_free_auth_file", "/root/.ssh/authorized_keys")
EXEC_RESULT_LIMIT = int(cp.get("exec_result_limit", 5000))

# Custom module
ALLOC_SERVER_MODULE = "core.server.alibaba.alloc_server"

# notice
DEFAULT_CHARSET = "utf-8"
EMAIL_USE_LOCALTIME = False
EMAIL_BACKEND = cp.get("email_backend")
EMAIL_HOST_USER = DEFAULT_FROM_EMAIL = cp.get("email_host_user")
EMAIL_HOST = cp.get("email_host")
EMAIL_PORT = int(cp.get("email_port", 25))
EMAIL_HOST_PASSWORD = cp.get("email_password")
EMAIL_USE_TLS = False
EMAIL_USE_SSL = False
EMAIL_TIMEOUT = 900
EMAIL_SSL_KEY_FILE = None
EMAIL_SSL_CERT_FILE = None
DEV_MAIL_LIST = cp.get("dev_mail_list", "").split(',')
NOTIFY_MAIL_LIST = cp.get("notify_mail_list", "").split(',')
ALI_KERNEL_DEV_MAIL = cp.get("ali_kernel_dev_mail")

# sn cache
H_CACHE_IP2SN = "h_ip2sn"
H_CACHE_SN2IP = "h_sn2ip"

# build kernel package
BUILD_PKG_API_DOMAIN = cp.get("build_pkg_api_domain")
BUILD_PKG_API_TOKEN = cp.get("build_pkg_api_token")

# check machine real state
CHECK_MACHINE_PROCESS_NUM = int(cp.get("check_machine_process_num", 90))
CHECK_MACHINE_INTERVAL_START = int(cp.get("check_machine_interval_start", 120))
CHECK_MACHINE_INTERVAL_END = int(cp.get("check_machine_interval_end", 180))
CHECK_SERVER_STATE_ACQUIRE_TIMEOUT = int(cp.get("check_machine_state_acquire_timeout", 10))
CHECK_SERVER_STATE_LOCK_TIMEOUT = int(cp.get("check_machine_state_lock_timeout", 10))
CHECK_MACHINE_PROCESS_ACQUIRE_TIMEOUT = int(cp.get("check_machine_process_acquire_timeout", 10))
CHECK_MACHINE_PROCESS_LOCK_TIMEOUT = int(cp.get("check_machine_process_lock_timeout", 300))

# kafka
KAFKA_API_VERSION = (0, 10, 2)
KAFKA_MSG_RETRIES = int(cp.get("kafka_msg_retries", 5))
KAFKA_MAX_BLOCK_MS = int(cp.get("kafka_max_block_ms", 15000))
KAFKA_MAX_POLL_RECORDS = int(cp.get("kafka_max_poll_records", 100))
KAFKA_FETCH_MAX_BYTES = int(cp.get("kafka_fetch_max_bytes", 1 * 1024 * 1024))
KAFKA_SESSION_TIMEOUT_MS = int(cp.get("kafka_session_timeout_ms", 25000))

# tone home
TONE_DOMAIN = cp.get("tone_domain")

# other
TRACE_LIMIT = int(cp.get("trace_limit", 20))
API_SECRET_KEY = cp.get("api_secret_key")

# monitor
MONITOR_HOSTNAME = cp.get("monitor_hostname")
DEFAULT_EMP_ID = cp.get("default_emp_id")
GOLDMINE_USERNAME = cp.get("goldmine_username")
GOLDMINE_TOKEN = cp.get("goldmine_token")
