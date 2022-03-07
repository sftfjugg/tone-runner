import unittest
from core.dag.plugin.process_dag_plugin import ProcessDagPlugin
from models.dag import DagStepInstance


class TestExecNode(unittest.TestCase):

    def test_exec_node(self):
        node_id = 9047
        dag_node_inst = DagStepInstance.get_by_id(node_id)
        ProcessDagPlugin.exec_dag_node(dag_node_inst)


if __name__ == "__main__":
    unittest.main()
