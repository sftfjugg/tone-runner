import unittest
import asyncio
from scheduler.main_thread import produce_exec_job
from core.dag.engine.engine_run import (
    generate_dag,
    processing_fast_dag_inst,
    processing_dag_node_inst
)


class TestScheduler(unittest.TestCase):

    @unittest.skip("ignore")
    def test_produce_exec_job(self):
        asyncio.run(produce_exec_job())

    @unittest.skip("ignore")
    def test_consume_job_to_create_dag(self):
        asyncio.run(generate_dag())

    @unittest.skip("ignore")
    def test_consume_dag(self):
        asyncio.run(processing_fast_dag_inst())

    @unittest.skip("ignore")
    def test_consume_dag_node(self):
        asyncio.run(processing_dag_node_inst())


if __name__ == "__main__":
    unittest.main()
