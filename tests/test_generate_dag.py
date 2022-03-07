import pickle
import unittest
from models.dag import Dag as DagModel
from core.dag.engine.base_dag import Dag
from core.dag.engine.dag_factory import create_dag_from_job
from core.dag.engine.generate_dag import GeneralJobDagFactory


class TestGenerateDag(unittest.TestCase):

    def test_create_job_dag_with_standalone(self):
        job_id = 1
        create_dag_from_job(GeneralJobDagFactory(job_id))
        dag = DagModel.get(job_id=job_id)
        dag_inst = pickle.loads(dag.graph)
        self.assertEqual(True, isinstance(dag_inst, Dag))

    def test_create_job_dag_with_cluster(self):
        job_id = 2
        create_dag_from_job(GeneralJobDagFactory(job_id))
        dag = DagModel.get(job_id=job_id)
        dag_inst = pickle.loads(dag.graph)
        print(f"\n\ndag_inst.graph: {dag_inst.graph}\n\n")
        self.assertEqual(True, isinstance(dag_inst, Dag))


if __name__ == "__main__":
    unittest.main()
