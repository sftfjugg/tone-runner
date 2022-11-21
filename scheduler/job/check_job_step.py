import json
import os
import time
import traceback
import importlib
import config
from core.exception import CheckStepException, ExecChanelException
from core.exec_channel import ExecChannel
from core.op_acc_msg import OperationAccompanyMsg as Oam
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs, CommonDbServerOperation
from lib.cloud import Provider
from scheduler.job.step_common import StepCommon
from constant import (
    ExecState,
    StepStage,
    ToneAgentResStatus,
    get_agent_res_obj,
    get_agent_res_status_obj,
    BuildPackageStatus,
    TidTypeFlag,
    ChannelType,
    OtherAgentRes, ToneAgentScriptTone, LogFilePath
)
from models.base import db
from models.job import TestStep, TestJobCase, TestJob, BuildJob
from models.case import TestSuite, TestCase, AccessCaseConf
from models.dag import Dag, DagStepInstance
from models.server import get_server_or_snapshot_server
from scheduler.job import get_test_class
from tools.business_config.ci_factory import get_ci_inst
from tools.log_util import LoggerFactory
from tools import utils

alloc_server_module = importlib.import_module(config.ALLOC_SERVER_MODULE)
logger = LoggerFactory.scheduler()


class CheckJobStep:

    @classmethod
    @db.atomic()
    def check_step_with_ali_group(cls, test_step, dag_step_id, tid, correct=False):
        try:
            error_msg = None
            dag_step = DagStepInstance.get_by_id(dag_step_id)
            channel_type = dag_step.channel_type
            stage, state = test_step.stage, test_step.state
            if state in ExecState.end and not correct:
                is_end = True
            else:
                is_end, error_msg = cls._check_step_with_ali_group(
                    test_step, dag_step, channel_type, tid
                )
                if is_end:
                    test_step = TestStep.get_or_none(dag_step_id=dag_step_id)
                    state = test_step.state
            if is_end:
                if stage == StepStage.RUN_CASE:
                    cls._upload_run_case_log(test_step, channel_type)
                elif stage in StepStage.NEED_UPLOAD_LOG_STAGE_SET:
                    log_file = cls._upload_prepare_log(test_step, stage)
                    test_step.log_file = log_file
                    test_step.save()
            logger.info(
                f"check node({dag_step_id}) with check_step_with_ali_group,"
                f" is_end: {is_end}, state: {state}, error_msg: {error_msg}"
            )
        except Exception as error:
            raise CheckStepException(error)

    @classmethod
    @db.atomic()
    def check_step_with_i_clone(cls, test_step, dag_step_id, tid):
        pass

    @classmethod
    @db.atomic()
    def check_step_with_business_test(cls, test_step, dag_step_id, tid):
        try:
            state = test_step.state
            if state not in ExecState.end:
                ticket_id = tid.strip(TidTypeFlag.BUSINESS_TEST_TID)
                test_job_case = TestJobCase.get_by_id(test_step.job_case_id)
                conf_obj = AccessCaseConf.filter(test_case_id=test_job_case.test_case_id).first()
                conf = {
                    'ci_type': conf_obj.ci_type,
                    'host': conf_obj.host,
                    'user': conf_obj.user,
                    'token': conf_obj.token,
                    'pipeline_id': conf_obj.pipeline_id,
                    'job_name': conf_obj.project_name,
                    'params': conf_obj.params
                }
                ci = get_ci_inst(conf)
                conf.update(json.loads(ticket_id))
                resp = ci.query(**conf)
                if resp['status'] == 'done':
                    test_class = get_test_class(test_step.test_type)
                    test_class.save_result(test_step, resp)

            logger.info(f"check node({dag_step_id}) with check_step_with_business_test, state: {state}")
        except Exception as error:
            raise CheckStepException(error)

    @classmethod
    @db.atomic()
    def check_step_with_build_package(cls, test_step, dag_step_id, tid):
        try:
            job_id = test_step.job_id
            state = test_step.state
            if state not in ExecState.end:
                tid_split_list = tid.split("_")
                build_task_id = int(tid_split_list[3])
                build_job_obj = BuildJob.get_by_id(build_task_id)
                build_job_state = build_job_obj.state
                if build_job_state == BuildPackageStatus.SUCCESS:
                    test_step.state = ExecState.SUCCESS
                    test_step.result = "build package success."
                    cls.update_kernel_info(job_id, {"rpm_list": json.loads(build_job_obj.rpm_list)})
                if build_job_state == BuildPackageStatus.FAIL:
                    test_step.state = ExecState.FAIL
                    test_step.result = build_job_obj.build_msg
                if build_job_obj.tid and test_step.tid != build_job_obj.tid:
                    test_step.tid = build_job_obj.tid
                test_step.save()
            logger.info(f"check node({dag_step_id}) with check_step_with_build_package, state: {state}")
        except Exception as error:
            raise CheckStepException(error)

    @classmethod
    def update_kernel_info(cls, job_id, build_result):
        dag_id = Dag.get(job_id=job_id).id
        headers, dev, kernel = build_result["rpm_list"]
        install_kernel_steps = DagStepInstance.filter(stage=StepStage.INSTALL_KERNEL, dag_id=dag_id)
        check_install_kernel_steps = DagStepInstance.filter(stage=StepStage.CHECK_KERNEL_INSTALL, dag_id=dag_id)
        for ik in install_kernel_steps:
            kernel_info = ik.kernel_info
            kernel_info["headers"] = headers
            kernel_info["dev" + "el"] = dev
            kernel_info["kernel"] = kernel
            ik.kernel_info = kernel_info
            ik.save()
        for ck in check_install_kernel_steps:
            ck.kernel_version = utils.get_kernel_version(kernel)
            ck.save()

    @classmethod
    def _check_step_with_ali_group(cls, test_step, dag_step, channel_type, tid):
        job_id = test_step.job_id
        server_id = dag_step.server_id
        is_end, stage = False, test_step.stage
        run_mode = dag_step.run_mode
        in_pool = dag_step.in_pool
        cluster_id = dag_step.cluster_id
        result = ExecChannel.do_query(channel_type, tid=tid)
        if stage in StepStage.REBOOT_SET and channel_type == ChannelType.OTHER_AGENT and \
                (OtherAgentRes.AGENT_NOT_AVAILABLE in result.get(OtherAgentRes.ERROR_MSG)
                 or OtherAgentRes.AGENT_DOWN in result.get(OtherAgentRes.ERROR_MSG)):
            result[OtherAgentRes.SUCCESS] = True
        server_provider = dag_step.server_provider
        agent_res = get_agent_res_obj(channel_type)
        agent_res_status = get_agent_res_status_obj(channel_type)
        server_ip, server_sn, server_tsn = dag_step.server_ip, dag_step.server_sn, dag_step.server_tsn
        status = result[agent_res.STATUS]
        success, job_result = result[agent_res.SUCCESS], result[agent_res.JOB_RESULT] or ""
        error_msg, error_code = result.get(agent_res.ERROR_MSG), result[agent_res.ERROR_CODE]
        if agent_res_status.check_end(status):
            is_end = True
            result = job_result[:config.EXEC_RESULT_LIMIT] or error_msg
            if not server_tsn:
                server_tsn = Cs.get_server_tsn(server_id, server_provider)
            if agent_res.check_success(success):
                if stage in StepStage.REBOOT_SET:
                    reboot_time = int(test_step.gmt_created.timestamp())
                    dag_step.reboot_time = reboot_time
                    cls._check_reboot(test_step, channel_type, dag_step.server_ip, reboot_time, tsn=server_tsn)
                else:
                    result_ = None
                    test_step.state = ExecState.SUCCESS
                    if status == ToneAgentResStatus.STOP:
                        test_step.state = ExecState.STOP
                        result_ = "step is stop."
                    test_step.result = result_ or "run case done" if stage == StepStage.RUN_CASE else result
            else:
                cls._check_server_broken(
                    job_id, server_provider, channel_type, server_id,
                    server_ip, server_sn, error_code, agent_res,
                    in_pool=in_pool, run_mode=run_mode, cluster_id=cluster_id, tsn=server_tsn
                )
                test_step.state = ExecState.FAIL
                test_step.result = result
            cls._run_case_step_save_results(test_step, dag_step, job_result, channel_type)
            test_step.save()
        return is_end, error_msg

    @classmethod
    def _check_reboot(cls, test_step, channel_type, server_ip, reboot_time, tsn=None):
        try:
            check_success = ExecChannel.check_reboot(
                channel_type, server_ip,
                max_retries=config.CHECK_REBOOT_RETRIES,
                interval=config.CHECK_REBOOT_INTERVAL,
                reboot_time=reboot_time,
                tsn=tsn
            )
        except ExecChanelException:
            check_success = False
        if check_success:
            test_step.state = ExecState.SUCCESS
            test_step.result = f"server({server_ip}) reboot success."
        else:
            test_step.state = ExecState.FAIL
            test_step.result = f"server({server_ip}) reboot fail."
        test_step.save()

    @classmethod
    def _run_case_step_save_results(cls, test_step, dag_step, job_result, channel_type):
        if test_step.stage == StepStage.RUN_CASE:
            logger.info(f"run case complete, job_result:{job_result}")
            cls._upload_run_case_log(test_step, channel_type)
            job_result = cls._clear_test_result(job_result)
            tmp_result = job_result.strip()
            if tmp_result:
                test_class = get_test_class(test_step.test_type)
                test_class.save_result(test_step, job_result)
                test_class.compare_baseline(dag_step)
            else:
                logger.error(f"test result is empty! step_id: {test_step.id}")

    @classmethod
    def _upload_prepare_log(cls, step, stage):
        test_job = TestJob.get_by_id(step.job_id)
        server = CommonDbServerOperation.get_snapshot_server_by_provider(
            step.server, test_job.server_provider
        )
        if not hasattr(LogFilePath, stage.upper()):
            logger.info(f'stage({stage}) no log file')
            return
        source_file = getattr(LogFilePath, stage.upper())
        new_file_name = f'{stage}_{int(time.time())}.log'
        cls.__upload_prepare_log(
            source_file, new_file_name,
            step.server_ip, step.job_id,
            tsn=server.tsn
        )
        return f"{config.TONE_STORAGE_HOST}:{config.TONE_STORAGE_PROXY_PORT}/" \
               f"{config.TONE_STORAGE_BUCKET}/{step.job_id}/{new_file_name}"

    @classmethod
    def __upload_prepare_log(cls, source_file, new_file_name, ip, job_id, tsn=None):
        # 主要是上传 prepare 等步骤的日志文件
        try:
            arg = f"{config.TONE_STORAGE_BUCKET} {source_file} {new_file_name}"
            script = StepCommon.get_agent_script(
                ChannelType.TONE_AGENT,
                ToneAgentScriptTone.UPLOAD_FILE,
                job_id=job_id
            )
            success, result = ExecChannel.do_exec(
                ChannelType.TONE_AGENT, ip=ip, command=script,
                args=arg, sync=False,
                timeout=config.UPLOAD_LOG_TIMEOUT, tsn=tsn
            )
            if success:
                logger.info(f"upload file success, source_file:{source_file}, ip:{ip}, tsn:{tsn},"
                            f"new file name:{new_file_name},result: {result}")
            else:
                logger.error(f"upload file failed, source_file:{source_file}, ip:{ip}, tsn:{tsn},"
                             f"new file name:{new_file_name},result: {result}")
        except Exception as error:
            error = str(error)
            err_msg = traceback.format_exc()
            logger.error(
                f"upload file failed! source_file:{source_file}, new file name:{new_file_name} "
                f"reason: {error}, ip:{ip}, tsn:{tsn}, traceback: {err_msg}"
            )

    @classmethod
    def _upload_run_case_log(cls, step, channel_type):
        # 成功的时候会由test脚本负责上传
        if step.state == ExecState.FAIL:
            job_id = step.job_id
            step_id = step.id
            ip = step.server_ip
            if step.private_ip:
                ip = step.private_ip
            log_file = step.log_file
            env_info = step.env_info
            try:
                test_job_case = TestJobCase.get_by_id(step.job_case_id)
                logger.info(f"start upload log, job: {job_id} step: {step.id} ip: {ip}")
                if test_job_case:
                    test_case = TestCase.get_by_id(test_job_case.test_case_id)
                    test_suite = TestSuite.get_by_id(test_job_case.test_suite_id)
                    result_folder = os.path.basename(os.path.dirname(log_file))
                    test_job = TestJob.get(id=job_id)
                    if test_job.server_provider == Provider.ALIGROUP:
                        agent_script = StepCommon.get_agent_script_obj(channel_type)
                        script = StepCommon.get_agent_script(channel_type, agent_script.UPLOAD, job_id=job_id)
                    else:
                        script = StepCommon.get_agent_script(
                            ChannelType.TONE_AGENT,
                            ToneAgentScriptTone.UPLOAD_CLOUD, job_id=job_id
                        )
                    arg = f"{config.TONE_PATH} {test_suite.name} {test_case.name} {result_folder} " \
                          f"{test_case.short_name} {config.TONE_STORAGE_BUCKET}"
                    if channel_type == ChannelType.TONE_AGENT:
                        StepCommon.get_tone_agent_global_env(job_id, env_info)
                    dag_step = DagStepInstance.get_by_id(step.dag_step_id)
                    tsn = dag_step.server_tsn
                    if not tsn:
                        tsn = Cs.get_server_tsn(dag_step.server_id, dag_step.server_provider)
                    success, result = ExecChannel.do_exec(
                        channel_type, ip=ip, command=script,
                        args=arg, env=env_info, sync=False,
                        timeout=config.UPLOAD_LOG_TIMEOUT,
                        tsn=tsn
                    )
                    if success:
                        logger.info(f"upload log success, job: {job_id} step: {step_id},result: {result}")
                    else:
                        logger.error(f"upload log failed, job: {job_id} step: {step_id},result: {result}")
                else:
                    logger.warn(f"no case find with step! job: {job_id} step:{step_id} ip: {ip}")
            except Exception as error:
                error = str(error)
                err_msg = traceback.format_exc()
                logger.error(
                    f"upload_log_file failed! job: {job_id} step: {step_id} reason: {error}, traceback: {err_msg}"
                )

    @classmethod
    def _clear_test_result(cls, result):
        lines = result.splitlines()
        idx, match = 0, False
        for idx, ln in enumerate(lines):
            ln = ln.strip()
            if ln.startswith('http'):
                match = True
                break
        return os.linesep.join(lines[idx:]) if match else ""

    @classmethod
    def _check_server_broken(cls, job_id, server_provider, channel_type, server_id, server_ip,
                             server_sn=None, error_code=None, agent_res=None, in_pool=None,
                             run_mode=None, cluster_id=None, tsn=None):
        if isinstance(server_id, int):
            check_success, error_msg = ExecChannel.check_server_status(
                channel_type, server_ip, sn=server_sn, tsn=tsn)
            if not check_success or (agent_res and error_code in agent_res.ERROR_CODE_LIST):
                server_model = get_server_or_snapshot_server(in_pool, server_provider)
                server = server_model.get_by_id(server_id)
                Oam.set_server_broken_and_send_msg(
                    job_id, server, in_pool=in_pool, error_msg=error_msg, run_mode=run_mode, cluster_id=cluster_id
                )
