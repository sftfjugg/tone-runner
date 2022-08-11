import json
import random

import config
import time
from functools import wraps
from core.exception import ExecStepException
from core.exec_channel import ExecChannel
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from constant import (
    JobSysVar,
    ClusterRole,
    ServerFlowFields,
    TestType,
    JobCfgFields,
    RunMode,
    StarAgentScript,
    StarAgentScriptTone,
    ToneAgentScript,
    ToneAgentScriptTone,
    ChannelType,
    ExecState,
    CaseResult,
    TestFrameWork,
    BaseConfigKey,
    StepStage,
    get_agent_res_obj,
    ServerProvider
)
from lib.cloud.provider_info import ProviderInfo
from models.job import TestJob, TestJobSuite, TestJobCase, TestStep, MonitorInfo
from models.result import FuncResult, ResultFile, ArchiveFile
from models.server import TestServer, CloudServer, CloudServerSnapshot
from models.sysconfig import BaseConfig
from tools import utils
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()
result_logger = LoggerFactory.scheduler()


class StepCommon:

    @classmethod
    def _exec_spec_script(cls, meta_data, script_flag, args="", timeout=100, sync=False, run_case_step=False):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        provider = meta_data[ServerFlowFields.SERVER_PROVIDER]
        run_mode = meta_data[ServerFlowFields.RUN_MODE]
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        stage = meta_data[ServerFlowFields.STEP]
        kernel_info = meta_data.get(JobCfgFields.KERNEL_INFO, dict())
        hot_fix = kernel_info.get(JobCfgFields.HOT_FIX, False)
        ip = meta_data.get(ServerFlowFields.SERVER_IP)
        sn = meta_data.get(ServerFlowFields.SERVER_SN)
        tsn = meta_data.get(ServerFlowFields.SERVER_TSN)
        env_info = meta_data.get(JobCfgFields.ENV_INFO, dict())
        if run_case_step:
            job_case_id = meta_data[ServerFlowFields.JOB_CASE_ID]
            test_job_case = TestJobCase.get_by_id(job_case_id)
            job_case_env_info = test_job_case.env_info
            env_info.update(json.loads(job_case_env_info))
        cluster_id = meta_data.get(ServerFlowFields.CLUSTER_ID)
        script = cls.get_agent_script(
            channel_type, script_flag, stage, job_id, run_mode,
            env_info, hot_fix, cluster_id, provider
        )
        if channel_type == ChannelType.TONE_AGENT:
            cls._get_tone_agent_env(
                job_id, env_info, stage,
                run_mode, cluster_id, provider, hot_fix
            )
            if provider == ServerProvider.ALI_CLOUD:
                ip = meta_data.get(ServerFlowFields.PRIVATE_IP) or ip
        success, result = ExecChannel.do_exec(
            channel_type,
            ip=ip,
            sn=sn,
            tsn=tsn,
            command=script,
            args=args,
            timeout=timeout,
            sync=sync,
            env=env_info
        )
        return success, result

    @classmethod
    def exec_custom_script(cls, meta_data, sync=False, timeout=100):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        ip = meta_data[ServerFlowFields.SERVER_IP]
        sn = meta_data.get(ServerFlowFields.SERVER_SN)
        script = meta_data[JobCfgFields.SCRIPT]
        env_info = meta_data.get(JobCfgFields.ENV_INFO, dict())
        script = cls._get_ag_script(script, job_id, env_info)
        if channel_type == ChannelType.TONE_AGENT:
            cls._get_tone_agent_env(job_id, env_info)
            provider = meta_data[ServerFlowFields.SERVER_PROVIDER]
            if provider == ServerProvider.ALI_CLOUD:
                ip = meta_data.get(ServerFlowFields.PRIVATE_IP) or meta_data.get(ServerFlowFields.SERVER_IP)
        success, result = ExecChannel.do_exec(
            channel_type,
            ip=ip,
            sn=sn,
            command=script,
            env=env_info,
            sync=sync,
            timeout=timeout
        )
        return success, result

    @classmethod
    def _update_step(cls, meta_data, success, result, log_file=None, tid=None, state_=None, result_=None):
        job_case_id = 0
        job_id = meta_data[ServerFlowFields.JOB_ID]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        stage = meta_data[ServerFlowFields.STEP]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_res = get_agent_res_obj(channel_type)
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        if stage in StepStage.TEST:
            job_case_id = meta_data.get(ServerFlowFields.JOB_CASE_ID, 0)
        if success:
            state = ExecState.RUNNING
            if not tid:
                tid = result[agent_res.TID]
            result = None
        else:
            state = ExecState.FAIL
            result = str(result)[:config.EXEC_RESULT_LIMIT]

        if state_:
            state = state_
        if result_:
            result = result_
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
            log_file=log_file,
        )
        if stage in StepStage.TEST:
            cls.set_job_suite_or_case_state_when_step_created(state, job_suite_id, job_case_id)
        exec_res_info = f"{stage} execution -> job_id:{job_id} success: {success} result: {result}"
        logger.info(exec_res_info)
        return exec_res_info

    @classmethod
    def get_agent_script_obj(cls, channel_type):
        test_frame = BaseConfig.get(
            config_key=BaseConfigKey.TEST_FRAMEWORK
        ).config_value
        if channel_type == ChannelType.STAR_AGENT:
            if test_frame == TestFrameWork.AKTF:
                return StarAgentScript
            else:
                return StarAgentScriptTone
        else:
            return cls.get_tone_agent_script_obj(test_frame)

    @classmethod
    def get_tone_agent_script_obj(cls, test_frame=None):
        if not test_frame:
            test_frame = BaseConfig.get(
                config_key=BaseConfigKey.TEST_FRAMEWORK
            ).config_value
        if test_frame == TestFrameWork.AKTF:
            return ToneAgentScript
        else:
            return ToneAgentScriptTone

    @classmethod
    def get_agent_script(cls, channel_type, script_flag, stage=None, job_id=None, run_mode=None,
                         env_info=None, hot_fix=None, cluster_id=None, provider=None):
        if channel_type == ChannelType.STAR_AGENT:
            return cls._get_star_agent_script(
                job_id, run_mode, script_flag, stage,
                env_info, hot_fix, cluster_id, provider
            )
        else:
            script = cls._get_tone_agent_script(script_flag)
            script = cls._get_ag_script(script, job_id, env_info, hot_fix)
            if run_mode == RunMode.CLUSTER and \
                    stage in [StepStage.RUN_CASE, StepStage.INITIAL,
                              StepStage.INSTALL_KERNEL, StepStage.CHECK_KERNEL_INSTALL]:
                if provider == ServerProvider.ALI_GROUP:
                    cluster_server = Cs.get_cluster_server(cluster_id, provider, is_run=True)
                    script = cls._inject_cluster_env(script, cluster_server)
            return script

    @classmethod
    def _get_tone_agent_script(cls, script_flag):
        return BaseConfig.get(config_type="script", config_key=script_flag).config_value

    @classmethod
    def _get_star_agent_script(cls, job_id, run_mode, script_flag, stage, env_info, hot_fix, cluster_id, provider):
        script = f"script={script_flag}"
        if stage != StepStage.REBOOT:
            script = cls._get_ag_script(script, job_id, env_info, hot_fix)
        if run_mode == RunMode.CLUSTER and stage in \
                [StepStage.RUN_CASE, StepStage.INITIAL, StepStage.INSTALL_KERNEL, StepStage.CHECK_KERNEL_INSTALL]:
            cluster_server = Cs.get_cluster_server(cluster_id, provider, is_run=True)
            script = cls._inject_cluster_env(script, cluster_server)
        return script

    @classmethod
    def get_tone_agent_global_env(cls, job_id, meta_env, hot_fix=False):
        env = cls._get_job_env(job_id, meta_env, hot_fix)
        env = cls._job_env_convert_to_json(env)
        meta_env.update(env)

    @classmethod
    def _get_tone_agent_env(
            cls, job_id, meta_env, stage=None,
            run_mode=None, cluster_id=None, provider=None, hot_fix=False
    ):
        cls.get_tone_agent_global_env(job_id, meta_env, hot_fix)
        if stage == StepStage.RUN_CASE and run_mode == RunMode.CLUSTER:
            cluster_server = Cs.get_cluster_server(cluster_id, provider, is_run=True)
            cls._update_cluster_env(meta_env, cluster_server, job_id)

    @classmethod
    def __covert2dict(cls, ln):
        if ln:
            k, v = ln.split("=", 1)
            if k.startswith("export"):
                k = k.replace("export", "", 1)
            k = k.strip()
            return {k: v}
        else:
            return dict()

    @classmethod
    def _job_env_convert_to_json(cls, job_vars):
        result_dict = dict()
        for item in map(cls.__covert2dict, job_vars.splitlines()):
            result_dict.update(item)
        return result_dict

    @classmethod
    def _get_job_env(cls, job_id, env_info, hot_fix=False):
        job_vars = f"export {JobSysVar.TONE_JOB_ID}={job_id}"
        if env_info:
            for k, v in env_info.items():
                job_vars += "\n" + f"export {k}={v}"
        if hot_fix:
            job_vars += "\n" + "export KHOTFIX_INSTALL=y"
        return job_vars

    @classmethod
    def _inject_cmd_into_script(cls, script, cmd_line):
        if not cmd_line:
            return script
        inject_cmd = cmd_line.splitlines()
        lines = script.splitlines()
        start = 0
        if lines[0].startswith("#!"):
            start = 1
        for cmd in inject_cmd:
            lines.insert(start, cmd)
            start += 1
        return "\n".join(lines)

    @classmethod
    def _inject_cluster_env(cls, script, cluster_server):
        inject_cmd, server_list = [], []
        for cs in cluster_server:
            test_server = cs.testserver
            pub_ip = test_server.ip
            pri_ip = test_server.private_ip
            value = pri_ip if pri_ip else pub_ip
            env_cmd = "export {name}={value}".format(name=cs.var_name, value=value)
            if cs.role == ClusterRole.LOCAL:
                server_list.insert(0, value)
            else:
                server_list.append(value)
            inject_cmd.append(env_cmd)
        inject_cmd.append('export CLUSTER_SERVERS="%s"' % " ".join(server_list))
        lines = script.splitlines()
        start = 0
        if lines[0].startswith("#!"):
            start = 1
        for cmd in inject_cmd:
            lines.insert(start, cmd)
            start += 1
        return "\n".join(lines)

    @classmethod
    def _update_cluster_env(cls, meta_env, cluster_server, job_id):
        server_list = []
        for cs in cluster_server:
            server = cls.get_server(cs, job_id)
            meta_env.update({cs.var_name.upper(): server})
            if cs.role == ClusterRole.LOCAL:
                server_list.insert(0, server)
            else:
                server_list.append(server)
            server_list_str = " ".join(server_list)
            meta_env.update({"CLUSTER_SERVERS": f'{server_list_str}'})
        return meta_env

    @classmethod
    def get_server(cls, cs, job_id):
        if cs.cluster_type == ServerProvider.ALI_CLOUD:
            cloud_server = CloudServer.get_by_id(cs.server_id)
            if cloud_server.is_instance:
                test_server = cloud_server.private_ip
            else:
                test_server = CloudServer.get(parent_server_id=cloud_server.id, job_id=job_id).private_ip
        else:
            test_server = TestServer.get_by_id(cs.server_id).ip
        return test_server

    @classmethod
    def _extract_cloud_cluster_env(cls, cluster_server, cloud_inst_meta):
        result = dict()
        ips = set()
        service_hosts = list()
        for cs in cluster_server:
            cloud_server = cs.cloudserver
            tmp_info = ProviderInfo(**cloud_inst_meta)
            result.update({cs.varname: cloud_server.private_ip})
            ips.add(cloud_server.private_ip)
            service_hosts.append(cloud_server.private_ip)
            result.setdefault('{}_NUM'.format(tmp_info.var_name), tmp_info.num)
        service_provider = cluster_server[0].cloudserver.provider
        role = cluster_server[0].role
        result.update({'CLUSTER_SERVERS': ' '.join(ips),
                       'SERVICE_PROVIDER': service_provider,
                       'SERVICE_HOSTS': ' '.join(service_hosts),
                       'ROLE': role
                       })
        return result

    @classmethod
    def _get_ag_script(cls, meta_script, job_id, env_info, hot_fix=False):
        job_vars = cls._get_job_env(job_id, env_info, hot_fix)
        script = cls._inject_cmd_into_script(meta_script, job_vars)
        return script

    @classmethod
    def _get_test_conf_log(cls):
        test_frame = BaseConfig.get(
            config_key=BaseConfigKey.TEST_FRAMEWORK
        ).config_value
        if test_frame == TestFrameWork.TONE:
            return "tone.log"
        return "lkp.log"

    @classmethod
    def _get_test_exec_param(cls, meta_data):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        test_type = meta_data[ServerFlowFields.TEST_TYPE]
        job_case_id = meta_data[ServerFlowFields.JOB_CASE_ID]
        test_job_case = TestJobCase.get_by_id(job_case_id)
        try:
            test_suite = test_job_case.test_suite
        except Exception:
            raise ExecStepException("The Suite has been removed.")
        try:
            test_case = test_job_case.test_case
        except Exception:
            raise ExecStepException("The Case has been removed.")
        if test_job_case.repeat:
            repeat = test_job_case.repeat % config.REPEAT_BASE or 1
        elif test_type == TestType.PERFORMANCE:
            repeat = config.PERFORMANCE_REPEAT or 1
        else:
            repeat = 1
        short_name_folder = test_case.short_name.replace("%", "X")

        ts_sec = time.time()
        ts_ms = int((ts_sec - int(ts_sec)) * 1000)
        result_folder = f"{short_name_folder}_{int(ts_sec)}{ts_ms}{test_job_case.id}"
        log_file = f"http://{config.TONE_STORAGE_HOST}:{config.TONE_STORAGE_PROXY_PORT}/" \
                   f"{config.TONE_STORAGE_BUCKET}/{job_id}/{result_folder}/{cls._get_test_conf_log()}"
        arg = f"{config.TONE_PATH} {test_suite.name} {test_case.name} {repeat} {result_folder} " \
              f"{test_case.short_name} {config.TONE_STORAGE_BUCKET}"
        if repeat <= 3:
            timeout = test_case.timeout
        else:
            timeout = int((test_case.timeout / 3) * repeat)
        if timeout > config.AGENT_MAX_TIMEOUT:
            timeout = config.AGENT_MAX_TIMEOUT
        return arg, timeout, log_file

    @classmethod
    def _get_install_kernel_args(cls, kernel_info):
        kernel = kernel_info[JobCfgFields.KERNEL]
        dev = kernel_info[JobCfgFields.DEV]
        headers = kernel_info[JobCfgFields.HEADERS]
        args = " ".join([kernel, dev, headers])
        return args

    @classmethod
    def _cluster_ssh_free_login(cls, meta_data, ssh_free_login_method):
        run_mode = meta_data[ServerFlowFields.RUN_MODE]
        if run_mode == RunMode.CLUSTER:
            server_ips = []
            channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
            private_ip = meta_data[ServerFlowFields.PRIVATE_IP] or meta_data[ServerFlowFields.SERVER_IP]
            server_tsn = meta_data[ServerFlowFields.SERVER_TSN]
            if not server_tsn:
                server_tsn = CloudServerSnapshot.get(id=meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]).server_tsn
            remote = meta_data.get(ClusterRole.REMOTE)
            if remote:
                server_ips.append({'ip': private_ip, 'tsn': server_tsn})
                remote_ip_list = list()
                for rm in remote:
                    ip = rm.get(ServerFlowFields.PRIVATE_IP) or rm.get(ServerFlowFields.SERVER_IP)
                    tsn = rm.get(ServerFlowFields.SERVER_TSN)
                    snapshot_server = CloudServerSnapshot.get(id=rm['server_snapshot_id'])
                    if not ip:
                        ip = snapshot_server.private_ip
                    if not tsn:
                        tsn = snapshot_server.server_tsn
                    remote_ip_list.append({'ip': ip, 'tsn': tsn})
                server_ips.extend(remote_ip_list)
                ssh_free_login_method(channel_type, *server_ips)

    @classmethod
    def _save_result_file(cls, job_id, test_suite_id, test_case_id, sn, result_path, result_file):
        result_logger.info(
            f"Save result file, job_id:{job_id}, test_suite_id: {test_suite_id}, "
            f"test_case_id: {test_case_id}, sn: {sn}, result_path: {result_path}, "
            f"result_file: {result_file}"
        )
        test_job = TestJob.get_by_id(job_id)
        kernel = test_job.kernel_version or ""
        if ResultFile.filter(
            test_job_id=job_id,
            test_suite_id=test_suite_id,
            test_case_id=test_case_id,
            result_path=result_path,
            result_file=result_file
        ).exists():
            result_logger.warning(
                f"Repeat to save result file, job_id:{job_id}, test_suite_id: {test_suite_id}, "
                f"test_case_id: {test_case_id}"
            )
        else:
            ResultFile.create(
                test_job_id=job_id,
                test_suite_id=test_suite_id,
                test_case_id=test_case_id,
                result_path=result_path,
                result_file=result_file
            )
            ArchiveFile.create(
                ws_name=test_job.ws_name,
                project_name=test_job.project_name,
                test_plan_id=test_job.plan_id,
                test_job_id=job_id,
                test_suite_id=test_suite_id,
                test_case_id=test_case_id,
                sn=sn if sn else "",
                kernel=kernel,
                arch="",
                archive_link="",
                test_date=utils.get_now()
            )

    @classmethod
    def _calc_conf_result(cls, job_id, test_case_id):
        conf_result = None
        sc_results = {
            int(fr.sub_case_result) for fr in
            FuncResult.select(
                FuncResult.sub_case_result
            ).filter(
                test_job_id=job_id,
                test_case_id=test_case_id
            ) if fr
        }
        if CaseResult.FAIL.value in sc_results:
            conf_result = CaseResult.FAIL.name
        if CaseResult.PASS.value in sc_results and CaseResult.FAIL.value not in sc_results:
            conf_result = CaseResult.PASS.name
        return conf_result

    @classmethod
    def _get_job_func_test_result(cls, job_id):
        job_cases = TestJobCase.filter(job_id=job_id)
        test_case_ids = {job_case.test_case_id for job_case in job_cases}
        total = job_cases.count()
        func_result_dict = {}
        for test_case_id in test_case_ids:
            conf_res = cls._calc_conf_result(job_id, test_case_id)
            func_result_dict[test_case_id] = conf_res
        fail, success = cls._merger_conf_run_state_and_test_state(func_result_dict, job_cases)
        return {"total": total, "pass": success, "fail": fail}

    @classmethod
    def _merger_conf_run_state_and_test_state(cls, func_result_dict, job_cases):
        """
        统计conf成功/失败数量时，优先检查conf执行状态，再检查conf测试结果状态
        """
        success = fail = 0
        for case in job_cases:
            if case.state == ExecState.FAIL:
                fail += 1
            else:
                conf_test_res = func_result_dict.get(case.test_case_id)
                if conf_test_res == CaseResult.PASS.name:
                    success += 1
                if conf_test_res == CaseResult.FAIL.name:
                    fail += 1
        return fail, success

    @classmethod
    def _get_job_perf_test_result(cls, job_id):
        job_cases = TestJobCase.filter(job_id=job_id)
        total = job_cases.count()
        success = job_cases.filter(state=ExecState.SUCCESS).count()
        fail = job_cases.filter(state=ExecState.FAIL).count()
        return {"total": total, "pass": success, "fail": fail}

    @classmethod
    def get_job_result(cls, job_id, test_type):
        if test_type == TestType.FUNCTIONAL:
            test_result = cls._get_job_func_test_result(job_id)
        else:
            test_result = cls._get_job_perf_test_result(job_id)
        return test_result

    @classmethod
    def set_job_suite_or_case_state_when_step_created(cls, state, job_suite_id, job_case_id):
        TestJobSuite.update(
            state=ExecState.RUNNING,
            start_time=utils.get_now()
        ).where(
            TestJobSuite.id == job_suite_id,
            TestJobSuite.state == ExecState.PENDING
        ).execute()
        TestJobCase.update(
            state=state,
            start_time=utils.get_now()
        ).where(
            TestJobCase.id == job_case_id,
            TestJobCase.state == ExecState.PENDING
        ).execute()

    @classmethod
    def get_monitor_applied_ips(cls, object_id, monitor_level):
        # 所有已申请过的监控
        monitor_ips = []
        # 申请过的失败的监控，用于重新申请，为了效率暂时不做处理
        monitor_fail_ips = []
        for _monitor_info in MonitorInfo.select(MonitorInfo.server).filter(object_id=object_id,
                                                                           monitor_level=monitor_level):
            monitor_ips.append(_monitor_info.server)
            if not _monitor_info.state:
                monitor_fail_ips.append(_monitor_info.server)
        return monitor_ips, monitor_fail_ips


def check_step_exists(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        meta_data = args[1]
        stage = meta_data[ServerFlowFields.STEP]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        time.sleep(random.uniform(0, 1))
        test_step = TestStep.filter(stage=stage, dag_step_id=dag_step_id).first()
        if test_step:
            logger.error(f"Step is exists! step_id: {test_step.id}")
        else:
            return func(*args, **kwargs)
    return wrapper
