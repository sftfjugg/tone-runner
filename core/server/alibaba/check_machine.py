import time
import random

import config
from core.exec_channel import ExecChannel
from core.distr_lock import DistributedLock
from core.exception import ExecChanelException
from core.cache.remote_source import RemoteCronCheckServerSource as Rc
from constant import ServerProvider, ServerState, ProcessDataSource, BaseConfigKey
from models.server import TestServer, CloudServer, get_server_model_by_provider, ServerRecoverRecord, \
    update_server_state_log
from models.sysconfig import BaseConfig
from tools.log_util import LoggerFactory
from scheduler.job.alicloud import AliCloudStep
from tools import utils


logger = LoggerFactory.check_machine()
summary_logger = LoggerFactory.summary_error_log()


class RealStateCorrection:

    BATCH_PROCESS_NUM = config.CHECK_MACHINE_PROCESS_NUM

    def __init__(self, lock_name, acquire_timeout=15, lock_timeout=3):
        self.lock_name = lock_name
        self.acquire_timeout = acquire_timeout
        self.lock_timeout = lock_timeout
        self.last_test_server_id = 0
        self.last_cloud_server_id = 0
        self.test_server_list = []
        self.cloud_server_list = []
        self.process_flag = "_with_real_state"

    def get_new_check_servers_from_db(self):
        test_server_list = TestServer.filter(
            (TestServer.state == ServerState.BROKEN) | (TestServer.real_state == ServerState.BROKEN),
            TestServer.id > self.last_test_server_id
        ).limit(self.BATCH_PROCESS_NUM)
        if test_server_list:
            self.test_server_list = list(test_server_list)
        cloud_server_list = CloudServer.filter((CloudServer.is_instance == 1) &
                                               ((CloudServer.state == ServerState.BROKEN) |
                                                (CloudServer.real_state == ServerState.BROKEN)),
                                               CloudServer.id > self.last_test_server_id).limit(self.BATCH_PROCESS_NUM)
        if cloud_server_list:
            self.cloud_server_list = list(cloud_server_list)

    def get_last_check_server_id(self):
        self.last_test_server_id = Rc.get_last_check_server_id(
            ServerProvider.ALI_GROUP,
            self.process_flag
        )
        self.last_cloud_server_id = Rc.get_last_check_server_id(
            ServerProvider.ALI_CLOUD,
            self.process_flag
        )

    def update_last_check_server_id(self):
        if self.test_server_list:
            last_test_server_id = self.test_server_list[-1].id
        else:
            last_test_server_id = 0
        if self.cloud_server_list:
            last_cloud_server_id = self.cloud_server_list[-1].id
        else:
            last_cloud_server_id = 0
        Rc.set_last_check_server_id(
            ServerProvider.ALI_GROUP,
            last_test_server_id,
            self.process_flag
        )
        Rc.set_last_check_server_id(
            ServerProvider.ALI_CLOUD,
            last_cloud_server_id,
            self.process_flag
        )

    @staticmethod
    def _get_span_hour(start_time):
        timedelta_res = utils.get_now() - start_time
        day = timedelta_res.days
        span = timedelta_res.seconds
        minutes, second = divmod(span, 60)
        minutes += (day * 24 * 60)
        return minutes

    @staticmethod
    def _get_span_min(start_time):
        seconds = (utils.get_now() - start_time).seconds
        minutes = int(seconds / 60)
        return minutes

    @staticmethod
    def _get_server_use_state(server, check_res):
        state = server.state
        need_update_server_state_config = BaseConfig.filter(
            ws_id=server.ws_id,
            config_key=BaseConfigKey.AUTO_RECOVER_SERVER
        )
        need_update_server_state = False if (need_update_server_state_config and
                                             need_update_server_state_config[0].config_value == '0') else True
        if need_update_server_state:
            protect_duration_config = BaseConfig.filter(
                ws_id=server.ws_id,
                config_key=BaseConfigKey.RECOVER_SERVER_PROTECT_DURATION
            )
            protect_duration = 5 if len(protect_duration_config) == 0 \
                else float(protect_duration_config[0].config_value)
            duration_check = \
                RealStateCorrection._get_span_min(server.broken_at) > protect_duration
            if all([check_res, need_update_server_state, duration_check]):
                if state == ServerState.BROKEN:
                    state = server.history_state
                else:
                    state = ServerState.AVAILABLE
        return state

    @staticmethod
    def _update_server_real_state(provider, server_list):
        check_res = False
        server_ip = ''
        server_model = get_server_model_by_provider(provider)
        for server in server_list:
            try:
                channel_type, server_ip = server.channel_type, server.server_ip
                check_res, error_msg = ExecChannel.check_server_status(channel_type, server_ip, max_retries=1)
            except ExecChanelException as error:
                check_res = False
                summary_logger.error('update server real state, check server error: {}'.format(error))
            finally:
                state = RealStateCorrection._get_server_use_state(server, check_res)
                real_state = ServerState.AVAILABLE if check_res else ServerState.BROKEN
                if provider == ServerProvider.ALI_CLOUD and not AliCloudStep.existed_instance(server):
                    state = ServerState.UNUSABLE
                server_model.update(
                    state=state,
                    real_state=real_state,
                    check_state_time=utils.get_now()
                ).where(
                    server_model.id == server.id
                ).execute()
                if state == ServerState.AVAILABLE or state == ServerState.RESERVED:
                    ServerRecoverRecord.create(sn=server.sn,
                                               ip=server.ip if provider == ServerProvider.ALI_GROUP else server.pub_ip,
                                               reason='check is available and out of duration',
                                               broken_at=server.broken_at, recover_at=utils.get_now(),
                                               broken_job_id=server.broken_job_id)
                    update_server_state_log(provider, server.id, to_broken=False)
                logger.info(
                    f"Check server({server_ip}), scheduler state is {server.state}, "
                    f"real state is {real_state}."
                )

    def update_server_real_state(self):
        server_list = self.test_server_list + self.cloud_server_list
        if not server_list:
            logger.info("No machines need to be checked this time.")
        self._update_server_real_state(ServerProvider.ALI_GROUP, self.test_server_list)
        self._update_server_real_state(ServerProvider.ALI_CLOUD, self.cloud_server_list)

    def process(self):
        with DistributedLock(
            self.lock_name,
            self.acquire_timeout,
            self.lock_timeout,
            purpose="check_machine"
        ) as Lock:
            _lock = Lock.lock
            if _lock:
                self.get_last_check_server_id()
                self.get_new_check_servers_from_db()
                self.update_last_check_server_id()
                self.update_server_real_state()


def _check_machine_real_state():
    rand_start = config.CHECK_MACHINE_INTERVAL_START
    rand_end = config.CHECK_MACHINE_INTERVAL_END
    while 1:
        RealStateCorrection(ProcessDataSource.CHECK_REAL_STATE_LOCK,
                            acquire_timeout=config.CHECK_MACHINE_PROCESS_ACQUIRE_TIMEOUT,
                            lock_timeout=config.CHECK_MACHINE_PROCESS_LOCK_TIMEOUT).process()
        time.sleep(random.randint(rand_start, rand_end))


def check_machine():
    _check_machine_real_state()
