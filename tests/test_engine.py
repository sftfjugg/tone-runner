import unittest
import asyncio
from core.dag.engine import engine_run


class TestScheduler(unittest.TestCase):

    def test_generate_dag(self):
        asyncio.run(engine_run.generate_dag())

    def test_processing_dag_inst(self):
        asyncio.run(engine_run.processing_fast_dag_inst())

    def test_processing_dag_node_inst(self):
        asyncio.run(engine_run.processing_dag_node_inst())


if __name__ == "__main__":
    unittest.main()
