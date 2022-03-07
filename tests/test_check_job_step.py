import unittest
from core.dag.plugin.process_node_plugin import ProcessDagNodePlugin
from constant import ExecState
from scheduler.job.check_job_step import CheckJobStep
from models.job import TestStep


class TestCheckJobStep(unittest.TestCase):

    def test_check_step_with_ali_group(self):
        dag_step_id = 115
        CheckJobStep.check_step_with_ali_group(dag_step_id)
        test_step = TestStep.get_or_none(dag_step_id=dag_step_id)
        self.assertEqual(ExecState.SUCCESS, test_step.state)

    def test_check_step_by_celery(self):
        node_id = 99
        ProcessDagNodePlugin.check_step(node_id)


if __name__ == "__main__":
    unittest.main()
