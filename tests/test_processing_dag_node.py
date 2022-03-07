import unittest
from core.dag.engine.processing_node import ProcessingNode


class TestProcessNode(unittest.TestCase):

    def test_process_node(self):
        dag_node_id = 1
        process_dag_node_inst = ProcessingNode(dag_node_id)
        dag_node_is_complete = process_dag_node_inst.run()
        self.assertEqual(False, dag_node_is_complete)


if __name__ == "__main__":
    unittest.main()
