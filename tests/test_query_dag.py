import pickle
import unittest
from models.dag import Dag as DagModel
from core.dag.engine.base_dag import Dag


class TestQueryDag(unittest.TestCase):

    def test_query_dag(self):
        dag_id = 1717
        dag = DagModel.get_by_id(dag_id)
        dag_inst = pickle.loads(dag.graph)
        print(f"\ndag_inst.graph: {dag_inst.graph}, \ndag_inst.kwargs: {dag_inst.kwargs}\n")
        self.assertEqual(True, isinstance(dag_inst, Dag))


if __name__ == "__main__":
    unittest.main()
