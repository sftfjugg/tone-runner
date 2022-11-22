import json
import config

from constant import (
    ExecState,
    ServerFlowFields,
    get_agent_res_obj,
    JobCfgFields,
    TidTypeFlag,
    APP_NAME,
    ChannelType,
    ServerAttribute,
    MonitorParam,
    QueueName,
    RebootStep, BuildKernelFrom
)
from core.client.goldmine_monitor import GoldmineMonitor
from core.exec_channel import ExecChannel
from models.job import TestStep, BuildJob, TestJob, MonitorInfo
from models.server import TestServer
from tools.log_util import LoggerFactory
from .step_common import StepCommon
from .step_common import check_step_exists
from constant import OtherAgentRes, MonitorType
from ..plan.plan_executor import PlanExecutor

logger = LoggerFactory.scheduler()


class AliGroupStep(StepCommon):

    @classmethod
    def _ssh_free_login(cls, channel_type, *servers):
        auth_file = config.SSH_FREE_AUTH_FILE
        script_flag = cls.get_agent_script_obj(channel_type).SSH_PUB_KEY
        pub_cmd = cls.get_agent_script(channel_type, script_flag)
        agent_res = get_agent_res_obj(channel_type)
        pub_keys = {}
        for server in servers:
            if server:
                success, result = ExecChannel.do_exec(
                    channel_type,
                    ip=server.get('ip'),
                    tsn=server.get('tsn'),
                    command=pub_cmd,
                    sync=True,
                    timeout=config.SSH_FREE_TIMEOUT
                )
                if success:
                    pub_key = result.get(agent_res.JOB_RESULT)
                    if pub_key:
                        pub_keys[server.get('ip')] = {'tsn': server.get('tsn'), 'keys': pub_key.strip()}
                    else:
                        raise RuntimeError(f"generate ssh pub key on {server} ! details: {result}")
                else:
                    raise RuntimeError(f"generate ssh pub key failed on {server} ! details: {result}")
        for server in servers:
            if server:
                for pub_key_dict in pub_keys.values():
                    pub_key = pub_key_dict.get('keys')
                    send_cmd = f'grep -q "{pub_key}" {auth_file} || echo "{pub_key}" >> {auth_file}'
                    logger.info(f"ssh free login, server: {server} cmd: {send_cmd}")
                    success, result = ExecChannel.do_exec(
                        channel_type,
                        ip=server.get('ip'),
                        tsn=server.get('tsn'),
                        command=send_cmd,
                        sync=True,
                        timeout=config.SSH_FREE_TIMEOUT - 20
                    )
                    if success:
                        logger.info(f"send pub key on server: {server} succeed !")
                    else:
                        raise RuntimeError(
                            f"send pub key on server: {server} failed ! details: {result}"
                        )

    @classmethod
    def __re_clone(cls, create_from, iclone_info, job_id, dag_step_id, cluster_id, server, job_suite_id):
        pass

    @classmethod
    @check_step_exists
    def _re_clone(cls, meta_data):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        sn = meta_data[ServerFlowFields.SERVER_SN]
        iclone_info = meta_data[JobCfgFields.ICLONE_INFO]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        job_suite_id = meta_data[ServerFlowFields.JOB_SUITE_ID]
        cluster_id = meta_data.get(ServerFlowFields.CLUSTER_ID, 0)
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        create_from = iclone_info.get("create_from") or APP_NAME
        iclone_info["sn"] = sn
        cls.__re_clone(create_from, iclone_info, job_id, dag_step_id, cluster_id, snapshot_server_id, job_suite_id)

    @classmethod
    @check_step_exists
    def _initial(cls, meta_data):
        sn = meta_data[ServerFlowFields.SERVER_SN]
        args = "in" + "pool" if TestServer.filter(sn=sn).exists() else ""
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.INITIAL_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.INITIAL
        logger.info(f'run initial step info:{agent_script_obj}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=config.CLEANUP_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _install_rpm(cls, meta_data):
        rpm_info = meta_data[JobCfgFields.RPM_INFO]
        args = " ".join(rpm_info)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.INSTALL_RPM_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.INSTALL_RPM
        logger.info(f'run install rpm step info:{agent_script_obj}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=config.INSTALL_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _build_pkg(cls, meta_data):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        username = meta_data[JobCfgFields.CREATOR]
        build_pkg_info = meta_data[JobCfgFields.BUILD_PKG_INFO].copy()
        build_pkg_info["build_task_name"] = f"build-pkg-for-job-{job_id}-by-tone-runner"
        test_job_obj = TestJob.get_by_id(job_id)
        build_name = f"build-pkg-for-plan-{job_id}-by-tone-runner"
        build_kernel = BuildJob.create(
            name=build_name,
            creator=test_job_obj.creator,
            build_from=BuildKernelFrom.JOB,
            **PlanExecutor()._convert_build_info(build_pkg_info)
        )
        build_job_id = build_kernel.id
        TestJob.update(build_job_id=build_job_id).where(TestJob.id == job_id).execute()
        job_case_id = 0
        job_id = meta_data[ServerFlowFields.JOB_ID]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        stage = meta_data[ServerFlowFields.STEP]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        state = ExecState.RUNNING
        tid = "_".join([TidTypeFlag.BUILD_PKG_TID, username, str(job_id), str(build_job_id)])
        result = "tone do_exec build job task"
        TestStep.create(
            job_id=job_id,
            dag_step_id=dag_step_id,
            state=state,
            stage=stage,
            tid=tid,
            job_suite_id=job_suite_id,
            job_case_id=job_case_id,
            cluster_id=snapshot_cluster_id,
            server=snapshot_server_id,
            result=result,
            log_file=None,
        )
        return ''

    @classmethod
    @check_step_exists
    def _install_kernel(cls, meta_data):
        kernel_info = meta_data[JobCfgFields.KERNEL_INFO]
        args = cls._get_install_kernel_args(kernel_info)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.INSTALL_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.INSTALL
        logger.info(f'run install kernel step info:{agent_script_obj}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=config.INSTALL_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _install_hot_fix(cls, meta_data):
        kernel_info = meta_data[JobCfgFields.KERNEL_INFO]
        args = cls._get_install_kernel_args(kernel_info)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.INSTALL_HOTFIX_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.INSTALL_HOTFIX
        logger.info(f'run install hotfix step info:{agent_script_obj}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=config.INSTALL_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _custom_script(cls, meta_data):
        success, result = cls.exec_custom_script(meta_data, timeout=config.CUSTOM_SCRIPT_TIMEOUT)
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _job_monitor(cls, meta_data):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        stage = meta_data[ServerFlowFields.STEP]
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        monitor_info = meta_data.get(JobCfgFields.MONITOR_INFO, list())
        emp_id = config.DEFAULT_EMP_ID
        case_machine_ip = meta_data.get(ServerFlowFields.SERVER_IP)
        # 所有已申请过的监控
        monitor_ips, monitor_fail_ips = cls.get_monitor_applied_ips(job_id, QueueName.JOB)

        try:
            goldmine_monitor_client = GoldmineMonitor()
            for _monitor in monitor_info:
                metric_category = _monitor.get('metric_category')
                monitor_type = _monitor.get('monitor_type')
                if monitor_type == MonitorType.CUSTOM_MACHINE:
                    ip = _monitor.get('ip')
                else:
                    ip = case_machine_ip
                req_data = {
                    MonitorParam.QUERY_TYPE: ServerAttribute.IP,
                    MonitorParam.QUERY: ip,
                    MonitorParam.EMP_ID: emp_id,
                }
                if ip not in monitor_ips:
                    try:
                        create_and_start_monitor_res = goldmine_monitor_client.create_and_start_monitor(req_data)
                    except Exception as error:
                        monitor_fail_ips.append(ip)
                        MonitorInfo.create(
                            state=False,
                            monitor_objs=json.dumps(metric_category),
                            is_open=False,
                            object_id=job_id,
                            monitor_level=QueueName.JOB,
                            server=ip,
                            remark=error
                        )
                    else:
                        MonitorInfo.create(
                            state=True,
                            monitor_link=create_and_start_monitor_res.get('monitor_link'),
                            monitor_objs=json.dumps(metric_category),
                            is_open=True,
                            object_id=job_id,
                            monitor_level=QueueName.JOB,
                            server=ip
                        )
                    finally:
                        monitor_ips.append(ip)
        except Exception as error:
            logger.error(f'job_monitor error: {error}, job id:{job_id}')
        TestStep.create(
            job_id=job_id,
            state=ExecState.SUCCESS,
            stage=stage,
            job_suite_id=job_suite_id,
            server=snapshot_server_id,
            dag_step_id=dag_step_id,
        )
        return meta_data

    @classmethod
    @check_step_exists
    def _reboot(cls, meta_data):
        args = "&"
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        script_flag = cls.get_agent_script_obj(channel_type).REBOOT
        sync = True if channel_type == ChannelType.OTHER_AGENT else False
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=RebootStep.TIMEOUT,
            sync=sync,
        )
        if success and channel_type == ChannelType.OTHER_AGENT:
            if OtherAgentRes.AGENT_NOT_AVAILABLE in result.get(OtherAgentRes.ERROR_MSG) \
                            or OtherAgentRes.AGENT_DOWN in result.get(OtherAgentRes.ERROR_MSG):
                result[OtherAgentRes.ERROR_MSG] = ''
                result[OtherAgentRes.SUCCESS] = True
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _check_kernel_install(cls, meta_data):
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        expect_kernel_version = meta_data[JobCfgFields.KERNEL_VERSION]
        success, result = cls.exec_custom_script(meta_data, sync=True)
        agent_res = get_agent_res_obj(channel_type)
        real_kernel_version = result[agent_res.JOB_RESULT].strip()
        if expect_kernel_version == real_kernel_version:
            state = ExecState.SUCCESS
        else:
            state = ExecState.FAIL
        result_ = f"expect kernel version:{expect_kernel_version},\nreal kernel version:{real_kernel_version}"
        return cls._update_step(meta_data, success, result, state_=state, result_=result_)

    @classmethod
    @check_step_exists
    def _prepare(cls, meta_data):
        cls._cluster_ssh_free_login(meta_data, cls._ssh_free_login)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.PREPARE_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.PREPARE
        logger.info(f'prepare step info:{agent_script_obj}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=config.TONE_PATH,
            timeout=config.PREPARE_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    @check_step_exists
    def _run_case(cls, meta_data):
        args, timeout, log_file = cls._get_test_exec_param(meta_data)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        logger.info('_run_case -> SERVER_PROVIDER:{}'.format(meta_data[ServerFlowFields.SERVER_PROVIDER]))
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.RUN_TEST_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.RUN_TEST
        logger.info(f'run test step info:{agent_script_obj}|{meta_data}')

        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=timeout,
            run_case_step=True
        )
        return cls._update_step(meta_data, success, result, log_file)
