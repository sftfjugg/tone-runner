import unittest
from core.dag.engine.base_dag import Dag


class TestBaseDag(unittest.TestCase):

    # dag example
    G1 = {
        "a": {"b", "f"},
        "b": {"c", "d", "f"},
        "c": {"d"},
        "d": {"e", "f"},
        "e": {"f"},
        "f": {}
    }

    G2 = {
        "a": {"b", "c", "h", "j"},
        "b": {"d", "l"},
        "c": {"d"},
        "d": {"e"},
        "e": {"f"},
        "f": {},
        "h": {"k"},
        "j": {"k"},
        "k": {},
        "l": {"m"},
        "m": {}
    }

    G3 = {
        "a": {"b"},
        "c": {"d"},
        "b": {"e"},
        "d": {"e"},
        "e": {}
    }

    def test_base_dag(self):
        dag_inst = Dag(self.G1)
        self.assertIn("f", dag_inst.list_nodes())

    def test_depend_nodes(self):
        dag_inst = Dag(self.G1)
        self.assertIn("b", dag_inst.depend_nodes("d"))
        self.assertIn("c", dag_inst.depend_nodes("d"))
        self.assertIn("d", dag_inst.depend_nodes("f"))
        self.assertIn("e", dag_inst.depend_nodes("f"))
        dag_inst1 = Dag(self.G3)
        self.assertIn("b", dag_inst1.depend_nodes("e"))

    def test_depend_nodes_with_all(self):
        dag_inst = Dag(self.G2)
        res = set()
        print(dag_inst.depend_nodes_with_all("f", res))
        res = set()
        print(dag_inst.depend_nodes_with_all("d", res))

    def test_get_nearest_enable_scheduler_nodes(self):
        scheduled = {"a", "b"}
        dag_inst = Dag(self.G2)
        nearest_enable_scheduler_nodes = dag_inst.nearest_scheduler_nodes(scheduled)
        self.assertIn("c", nearest_enable_scheduler_nodes)
        self.assertIn("h", nearest_enable_scheduler_nodes)
        self.assertIn("j", nearest_enable_scheduler_nodes)
        self.assertIn("l", nearest_enable_scheduler_nodes)


if __name__ == "__main__":
    unittest.main()
