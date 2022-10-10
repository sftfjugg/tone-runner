from functools import wraps

from core.exception import ExecStepException
from core.exec_channel import ExecChannel
from core.decorator import error_detect
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from core.op_acc_msg import OperationAccompanyMsg as Oam
from constant import ServerFlowFields, ServerProvider, ExecState
from models.base import db
from models.job import TestStep
from models.server import get_server_model_by_provider, TestServer
from tools.log_util import LoggerFactory
from .aligroup import AliGroupStep
from .alicloud import AliCloudStep


logger = LoggerFactory.scheduler()
except_info = {
    "except_class": ExecStepException,
    "except_logger": logger
}


def check_server_before_exec_step(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        meta_data = args[1]
        job_id = meta_data[ServerFlowFields.JOB_ID]
        step = meta_data[ServerFlowFields.STEP]
        server_provider = meta_data[ServerFlowFields.SERVER_PROVIDER]
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        is_instance = meta_data.get(ServerFlowFields.IS_INSTANCE)
        server_id = meta_data[ServerFlowFields.SERVER_ID]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        in_pool = meta_data.get(ServerFlowFields.IN_POOL)
        server_ip = meta_data[ServerFlowFields.SERVER_IP]
        server_sn = meta_data[ServerFlowFields.SERVER_SN]
        server_tsn = meta_data[ServerFlowFields.SERVER_TSN]
        run_mode = meta_data[ServerFlowFields.RUN_MODE]
        server_model = get_server_model_by_provider(server_provider)
        stage = meta_data[ServerFlowFields.STEP]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        job_suite_id = meta_data[ServerFlowFields.JOB_SUITE_ID]
        cluster_id = meta_data.get(ServerFlowFields.CLUSTER_ID)
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        if server_provider == ServerProvider.ALI_CLOUD and not is_instance:
            return func(*args, **kwargs)
        if in_pool:
            server = server_model.filter(id=server_id).first()
            check_res = None
            if server:
                check_res, error_msg = ExecChannel.check_server_status(
                    channel_type, server_ip, sn=server_sn, tsn=server_tsn)
                if not check_res:
                    Oam.set_server_broken_and_send_msg(job_id, server, error_msg=error_msg,
                                                       run_mode=run_mode, cluster_id=cluster_id)
                else:
                    TestServer.update(state='Occupied', spec_use=2, occupied_job_id=job_id).where(
                        TestServer.id == server_id,
                        TestServer.state == 'Available'
                    ).execute()
                    logger.info(f"reset server state, job_id:{job_id}, server_id:{server_id}")
        else:
            check_res, error_msg = ExecChannel.check_server_status(
                channel_type, server_ip, sn=server_sn, tsn=server_sn)
            snapshot_server = Cs.get_snapshot_server_by_provider(snapshot_server_id, server_provider)
            if not check_res:
                Oam.set_server_broken_and_send_msg(
                    job_id, snapshot_server, error_msg=error_msg,
                    in_pool=False, run_mode=run_mode, cluster_id=cluster_id
                )
        logger.info(f"check server<{server_ip}> status before exec step<{stage}>, check_res: {check_res}")
        if not check_res:
            result = (f"check server status fail before exec step<{step}>,"
                      f" job_id: {job_id}, dag_step_id: {dag_step_id}, "
                      f"server_id: {server_id}, server_ip: {server_ip}")
            TestStep.create(
                job_id=job_id,
                state=ExecState.FAIL,
                stage=stage,
                job_suite_id=job_suite_id,
                job_case_id=meta_data.get(ServerFlowFields.JOB_CASE_ID, 0),
                server=snapshot_server_id,
                dag_step_id=dag_step_id,
                result=result,
                cluster_id=cluster_id or snapshot_cluster_id
            )
            test_step = TestStep.get_or_none(dag_step_id=dag_step_id)
            from core.dag.plugin.job_complete import JobComplete
            JobComplete.set_job_case_state_by_test_step(test_step, stage, ExecState.FAIL, server_broken=True)
        else:
            return func(*args, **kwargs)
    return wrapper


class BaseTest(AliCloudStep, AliGroupStep):

    TEST_TYPE = None

    @classmethod
    @error_detect(**except_info)
    @check_server_before_exec_step
    def init_cloud(cls, meta_data):
        logger.info(f"Init cloud, meta_data:{meta_data}")
        return cls._init_cloud(meta_data)

    @classmethod
    @error_detect(**except_info)
    @check_server_before_exec_step
    def initial(cls, meta_data):
        logger.info(f"Initial, meta_data:{meta_data}")
        return cls._initial(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def build_package(cls, meta_data):
        logger.info(f"Build package, meta_data:{meta_data}")
        return cls._build_pkg(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def re_clone(cls, meta_data):
        logger.info(f"Re clone, meta_data:{meta_data}")
        cls._re_clone(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def install_rpm_before_reboot(cls, meta_data):
        logger.info(f"Install rpm before reboot, meta_data:{meta_data}")
        return cls._install_rpm(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_before_reboot(cls, meta_data):
        logger.info(f"Script before reboot, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @error_detect(**except_info)
    @check_server_before_exec_step
    def reboot(cls, meta_data):
        logger.info(f"Reboot, meta_data:{meta_data}")
        return cls._reboot(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_before_install_kernel(cls, meta_data):
        logger.info(f"Script before install kernel, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def install_kernel(cls, meta_data):
        logger.info(f"Install kernel, meta_data:{meta_data}")
        return cls._install_kernel(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def install_hotfix(cls, meta_data):
        logger.info(f"Install hot fix, meta_data:{meta_data}")
        return cls._install_hot_fix(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_after_install_kernel(cls, meta_data):
        logger.info(f"Script after install kernel, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def reboot_for_install_kernel(cls, meta_data):
        logger.info(f"Reboot for install kernel, meta_data:{meta_data}")
        return cls._reboot(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def check_kernel_install(cls, meta_data):
        logger.info(f"Check kernel install, meta_data:{meta_data}")
        return cls._check_kernel_install(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def install_rpm_after_reboot(cls, meta_data):
        logger.info(f"Install rpm after reboot, meta_data:{meta_data}")
        return cls._install_rpm(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_after_reboot(cls, meta_data):
        logger.info(f"Script after reboot, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @error_detect(**except_info)
    @check_server_before_exec_step
    def prepare(cls, meta_data):
        logger.info(f"Prepare, meta_data:{meta_data}")
        cls._prepare(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def job_console(cls, meta_data):
        pass

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def job_monitor(cls, meta_data):
        logger.info(f"Job monitor, meta_data:{meta_data}")
        cls._job_monitor(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def reboot_before_suite(cls, meta_data):
        logger.info(f"Reboot before_suite, meta_data:{meta_data}")
        return cls._reboot(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def suite_monitor(cls, meta_data):
        pass

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_before_suite(cls, meta_data):
        logger.info(f"Script before suite, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def reboot_before_case(cls, meta_data):
        logger.info(f"Reboot before case, meta_data:{meta_data}")
        return cls._reboot(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def case_monitor(cls, meta_data):
        pass

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_before_case(cls, meta_data):
        logger.info(f"Script before case, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def run_case(cls, meta_data):
        logger.info(f"Run case, meta_data:{meta_data}")
        return cls._run_case(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_after_case(cls, meta_data):
        logger.info(f"Script after case, meta_data:{meta_data}")
        return cls._custom_script(meta_data)

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def script_after_suite(cls, meta_data):
        logger.info(f"Script after suite, meta_data:{meta_data}")
        return cls._custom_script(meta_data)
