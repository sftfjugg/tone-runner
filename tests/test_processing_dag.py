import unittest
from core.dag.engine.update_dag import JobUpdateDag
from core.dag.engine.processing_dag import ProcessingDag
from models.dag import Dag


class TestProcessingDag(unittest.TestCase):

    def test_process_dag(self):
        dag_id = 1709
        job_id = Dag.get_by_id(dag_id).job_id
        update_dag_inst = JobUpdateDag(job_id=job_id)
        process_dag_inst = ProcessingDag(dag_id, update_dag_inst)
        dag_is_complete, block = process_dag_inst.run()
        self.assertEqual(False, dag_is_complete)


if __name__ == "__main__":
    unittest.main()
