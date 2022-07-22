import re
import json
import requests
from core.exception import CheckStepException
from constant import CaseResultTone, TestType, DbField, ExecState
from models.job import TestJobCase
from models.result import FuncResult
from models.baseline import FuncBaselineDetail
from tools.log_util import LoggerFactory
from .base_test import BaseTest


logger = LoggerFactory.job_result()


class FunctionTest(BaseTest):

    TEST_TYPE = TestType.FUNCTIONAL

    @classmethod
    def save_result(cls, step, result):
        parsed, job_id, step_id = False, step.job_id, step.id
        logger.info(f"job: {job_id} function test save_result step: {step_id} result: {result}")
        dag_step_id, sn = step.dag_step_id, step.server_sn
        test_job_case = TestJobCase.get_by_id(step.job_case_id)
        test_suite_id, test_case_id = test_job_case.test_suite_id, test_job_case.test_case_id
        lines = result.splitlines()
        base_url = ''
        for line in lines:
            if not line:
                continue
            if line.startswith('http'):
                base_url = line
            else:
                cls._save_result_file(job_id, test_suite_id, test_case_id, sn, base_url, line)
            if parsed:
                continue
            try:
                statistic_match = re.match(r"^statistic\.json$", line.strip())
                if statistic_match:
                    file_url = base_url + 'statistic.json'
                    response = requests.get(file_url)
                    logger.info(f"send result file request, file_url:{file_url}, response: {response.text}")
                    if response.ok:
                        statistic_data = json.loads(response.text)
                        cls.save_statistic_data(
                            job_id, dag_step_id, test_suite_id,
                            test_case_id, statistic_data
                        )
                        parsed = True
                        if statistic_data["status"] == 'fail':
                            step.state = ExecState.FAIL
                            step.save()
            except Exception as error:
                logger.error('save_result error, detail:{}'.format(str(error)),)
                logger.exception(
                    f"save test data has exception, job_id:{job_id}, step_id:{step_id}:"
                )
                raise CheckStepException(error)

    @classmethod
    def get_sub_result_from_matrix(cls, sub_case_results):
        sub_case_results = set(sub_case_results)
        if CaseResultTone.Fail.name in sub_case_results:
            sub_case_result = CaseResultTone.Fail.value
        elif CaseResultTone.Pass.name in sub_case_results:
            sub_case_result = CaseResultTone.Pass.value
        elif CaseResultTone.Warning.name in sub_case_results:
            sub_case_result = CaseResultTone.Warning.value
        else:
            sub_case_result = CaseResultTone.Skip.value
        return sub_case_result

    @classmethod
    def save_statistic_data(cls, job_id, dag_step_id, test_suite_id, test_case_id, data):
        results = data["results"]
        for result in results:
            sub_case_name = result["testcase"]
            sub_case_results = result["matrix"]
            sub_case_result = cls.get_sub_result_from_matrix(sub_case_results)
            if FuncResult.filter(
                test_job_id=job_id, dag_step_id=dag_step_id,
                test_suite_id=test_suite_id, test_case_id=test_case_id,
                sub_case_name=sub_case_name
            ).exists():
                logger.warning(
                    f"save_un_ltp_data re-parse result, job_id: {job_id}, dag_step_id: {dag_step_id}, "
                    f"test_suite_id: {test_suite_id}, test_case_id: {test_case_id}, sub_case_name: {sub_case_name}"
                )
                continue
            else:
                logger.info(
                    f"save data to func_result table, job_id: {job_id}, dag_step_id: {dag_step_id}, "
                    f"test_suite_id: {test_suite_id}, test_case_id: {test_case_id}, "
                    f"sub_case_name: {sub_case_name}, sub_case_result: {sub_case_result}"
                )
                FuncResult.create(
                    test_job_id=job_id, dag_step_id=dag_step_id,
                    test_suite_id=test_suite_id, test_case_id=test_case_id,
                    sub_case_name=sub_case_name, sub_case_result=sub_case_result
                )

    @classmethod
    def compare_baseline(cls, dag_step):
        baseline_id = dag_step.baseline_id
        baseline_job_id = dag_step.baseline_job_id
        if not (baseline_id or baseline_job_id):
            return
        results = FuncResult.filter(dag_step_id=dag_step.id, sub_case_result=CaseResultTone.Fail.value)
        for result in results:
            if baseline_id:
                func_baseline_detail = FuncBaselineDetail.get_or_none(
                    baseline_id=baseline_id,
                    test_suite_id=result.test_suite_id,
                    test_case_id=result.test_case_id,
                    sub_case_name=result.sub_case_name,
                )
                if func_baseline_detail:
                    if func_baseline_detail.impact_result:
                        result.match_baseline = DbField.TRUE
                    result.description = func_baseline_detail.description
                    result.bug = func_baseline_detail.bug
            else:
                func_baseline_job = FuncResult.get_or_none(
                    test_job_id=baseline_job_id,
                    test_suite_id=result.test_suite_id,
                    test_case_id=result.test_case_id,
                    sub_case_name=result.sub_case_name,
                    sub_case_result=CaseResultTone.Fail.value
                )
                if func_baseline_job:
                    result.match_baseline = DbField.TRUE
            result.save()
