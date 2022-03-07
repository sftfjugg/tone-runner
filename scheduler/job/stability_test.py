from tools.log_util import LoggerFactory
from constant import TestType
from models.job import TestJobCase
from .base_test import BaseTest


logger = LoggerFactory.scheduler()


class StabilityTest(BaseTest):

    TEST_TYPE = TestType.STABILITY

    @classmethod
    def save_result(cls, step, result):
        job_id, step_id = step.job_id, step.id
        logger.info(f"job: {job_id} stability test save_result step: {step_id} result: {result}")
        test_job_case = TestJobCase.get_by_id(step.job_case_id)
        sn = step.server_sn
        test_suite_id, test_case_id = test_job_case.test_suite_id, test_job_case.test_case_id
        lines = result.splitlines()
        url = lines[0]
        for line in lines[1:]:
            if not line:
                continue
            cls._save_result_file(job_id, test_suite_id, test_case_id, sn, url, line)
