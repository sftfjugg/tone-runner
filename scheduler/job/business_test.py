import json

import config
from constant import ServerFlowFields, get_agent_res_obj, ExecState, TidTypeFlag
from core.agent.star_agent import do_exec
from core.decorator import error_detect
from core.exception import ExecStepException
from models.base import db
from models.case import TestSuite, TestCase, BusinessSuiteRelation, TestBusiness, AccessCaseConf
from models.job import TestStep, TestJob, TestJobCase, TestJobSuite
from models.result import BusinessResult
from models.user import User
from tools.business_config.ci_factory import get_ci_inst
from tools.log_util import LoggerFactory
from .base_test import BaseTest, check_server_before_exec_step

logger = LoggerFactory.scheduler()
except_info = {
    "except_class": ExecStepException,
    "except_logger": logger
}


class BusinessTest(BaseTest):

    @classmethod
    @db.atomic()
    @error_detect(**except_info)
    @check_server_before_exec_step
    def run_case(cls, meta_data):
        logger.info(f"Run business case, meta_data:{meta_data}")
        job_suite_id = meta_data[ServerFlowFields.JOB_SUITE_ID]
        test_suite_id = TestJobSuite.get_by_id(job_suite_id).test_suite_id
        business_id = BusinessSuiteRelation.filter(test_suite_id=test_suite_id).first().business_id
        business = TestBusiness.get_by_id(business_id)
        business_name = business.name
        if business_name == 'searching':
            cls.searching_worker(meta_data)
        else:
            cls.test_worker(meta_data, business_name)
        return

    @classmethod
    def searching_worker(cls, meta_data):
        cmd = 'cd /home/admin/dailyrun_env_std/hippo_os_docker_test && sh sam_test_start.sh'
        ip = '11.140.128.82'
        sn = '217334044'
        tid = None
        state_ = None
        result_ = None
        log_file = None
        job_id = meta_data[ServerFlowFields.JOB_ID]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        stage = meta_data[ServerFlowFields.STEP]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_res = get_agent_res_obj(channel_type)
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        job_case_id = meta_data.get(ServerFlowFields.JOB_CASE_ID, 0)
        logger.info('start searching_worker job: %s, task_server: %s on ip: %s sn: %s' % (job_id, job_case_id, ip, sn))
        test_suite_id = TestJobSuite.get_by_id(job_suite_id).test_suite_id
        test_suite = TestSuite.get_or_none(id=test_suite_id)
        arg = test_suite.name + '_' + str(job_id)
        cmd = '%s %s' % (cmd, arg)
        success, result = do_exec(sn_list=[sn], command=cmd, user='admin', args=arg, timeout=60 * 60 * 2)
        logger.info('searching_worker -> task_server: %s success: %s result: %s' % (job_id, success, result))

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
        cls.set_job_suite_or_case_state_when_step_created(state, job_suite_id, job_case_id)
        exec_res_info = f"{stage} execution -> job_id:{job_id} success: {success} result: {result}"
        logger.info(exec_res_info)
        return exec_res_info

    @classmethod
    def test_worker(cls, meta_data, business_name):
        job_id = meta_data[ServerFlowFields.JOB_ID]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        stage = meta_data[ServerFlowFields.STEP]
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        job_case_id = meta_data.get(ServerFlowFields.JOB_CASE_ID, 0)
        ip = meta_data.get(ServerFlowFields.SERVER_IP)
        sn = meta_data.get(ServerFlowFields.SERVER_SN)
        test_suite_id = TestJobSuite.get_by_id(job_suite_id).test_suite_id
        test_suite = TestSuite.get_or_none(id=test_suite_id)
        job_case = TestJobCase.get_by_id(job_case_id)
        test_case = TestCase.get_or_none(id=job_case.test_case_id)
        result_ = None
        log_file = None
        logger.info('biz task: %s test_worker -> biz: %s suite: %s start ip/sn: %s/%s' %
                    (job_id, business_name, test_suite.name, ip, sn))

        conf_list = AccessCaseConf.filter(test_case_id=test_case.id)
        if not conf_list:
            raise Exception('please configure access case conf for test_case: (%s:%s), before run!'
                            % (business_name, test_case.name))
        conf_obj = conf_list.first()
        conf = {
            'ci_type': conf_obj.ci_type,
            'host': conf_obj.host,
            'user': conf_obj.user,
            'token': conf_obj.token,
            'pipeline_id': conf_obj.pipeline_id,
            'job_name': conf_obj.project_name,
            'params': conf_obj.params
        }
        if conf['ci_type'] == 'kernel_install':
            logger.info('biz: %s task worker: %s -> ci_type: kernel_install, only install kernel...'
                        % (business_name, job_id))
            tid = f'{TidTypeFlag.BUSINESS_TEST_TID}only install kernel'
            state = 'success'

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
                result=result_,
                log_file=log_file,
            )
            exec_res_info = f"{stage} execution -> job_id:{job_id} only install kernel success"
            logger.info(exec_res_info)
        else:
            test_job = TestJob.get_or_none(id=job_id)
            user = User.get_or_none(id=test_job.creator)
            user_id = None if not user.emp_id.isdigit() else int(user.emp_id)
            if not user_id:
                user_id = 10887
            conf.update({'emp_id': user_id})
            if test_case.timeout:
                conf.update({'timeout': int(test_case.timeout)})
            ci = get_ci_inst(conf)
            resp = ci.start(**conf)
            logger.info('biz:%s task worker: %s -> ci start: %s'
                        % (business_name, job_id, str(resp)))
            success = resp['success']
            tid = f'{TidTypeFlag.BUSINESS_TEST_TID}{json.dumps(resp)}'
            if success:
                state = ExecState.RUNNING
                result = None
            else:
                state = ExecState.FAIL
                result = resp['message']

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
            exec_res_info = f"{stage} execution -> job_id:{job_id} success: {success} result: {result}"
            logger.info(exec_res_info)
        cls.set_job_suite_or_case_state_when_step_created(state, job_suite_id, job_case_id)
        return exec_res_info

    @classmethod
    def save_result(cls, step, result):
        job_id = step.job_id
        dag_step_id = step.dag_step_id
        logger.info('task: %s btest save_result step: %s result: %s' % (job_id, step.id, result))
        test_job_case = TestJobCase.get_by_id(step.job_case_id)
        test_suite_id = test_job_case.test_suite_id
        test_case_id = test_job_case.test_case_id
        test_case = TestCase.get(id=test_case_id)
        test_case_name = test_case.name
        test_suite = TestSuite.get(id=test_suite_id)
        test_suite_name = test_suite.name
        logger.info('task: %s -> save_result, suite: %s, case: %s' % (job_id, test_case_name, test_suite_name))
        business_id = BusinessSuiteRelation.filter(test_suite_id=test_suite_id).first().business_id
        business = TestBusiness.get_by_id(business_id)
        business_name = business.name
        if business_name == 'searching':
            urls = result.splitlines()
            for url in urls:
                if not url:
                    continue
                BusinessResult.create(
                    test_job_id=job_id,
                    test_business_id=business_id,
                    test_suite_id=test_suite.id,
                    test_case_id=test_case.id,
                    dag_step_id=dag_step_id,
                    key=test_case_name,
                    value=url,
                    result_type='file'
                )
        elif isinstance(result, dict) and result['ci_system'] in ('aone', 'jenkins', 'script'):
            BusinessResult.create(
                test_job_id=job_id,
                test_business_id=business_id,
                test_suite_id=test_suite.id,
                test_case_id=test_case.id,
                dag_step_id=dag_step_id,
                link=result['ci_project'],
                ci_system=result['ci_system'],
                ci_result=result['ci_result'],
                ci_detail=json.dumps(result),
            )
            step.state = result['ci_result']
            step.save()
        return
