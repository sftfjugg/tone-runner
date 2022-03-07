import unittest
from scheduler.job.performance_test import PerformanceTest
from models.dag import DagStepInstance


class TestCompareBaseline(unittest.TestCase):

    def test_compare_baseline_with_perf(self):
        dag_step_id = 4224
        dag_step = DagStepInstance.get_by_id(dag_step_id)
        PerformanceTest.compare_baseline(dag_step)


if __name__ == "__main__":
    unittest.main()
