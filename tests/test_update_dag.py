import unittest
from models.dag import Dag as DagModel
from core.dag.engine.dag_factory import create_dag_from_job
from core.dag.engine.update_dag import UpdateJobDagFactory


class TestUpdateDag(unittest.TestCase):

    def test_update_dag(self):
        job_id = 2
        db_dag_inst = DagModel.get_or_none(job_id=job_id)
        self.assertEqual(1, db_dag_inst.is_update)
        create_dag_from_job(UpdateJobDagFactory(job_id))
        self.assertEqual(False, db_dag_inst.is_update)


if __name__ == "__main__":
    unittest.main()
