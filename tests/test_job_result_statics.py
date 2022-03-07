import unittest
from scheduler.job.base_test import BaseTest
from constant import TestType


class TestJobResultStatistic(unittest.TestCase):

    def test_job_result_statistic(self):
        job_id = 259
        test_type = TestType.FUNCTIONAL
        test_result = BaseTest.get_job_result(job_id, test_type)
        print(test_result)


if __name__ == "__main__":
    unittest.main()
