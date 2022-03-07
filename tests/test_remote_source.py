import unittest
from core.cache.remote_source import RemoteFlowSource, RemoteReleaseServerSource, RemoteAllocServerSource
from core.cache.clear_remote_inst import ClearProcessPlanInstSource as Clp
from constant import QueueType


class TestRemoteSource(unittest.TestCase):

    @unittest.skip("ignore")
    def test_remove_running_dag(self):
        dag_id = 66
        fmt_dag_id = RemoteFlowSource.source_with_slave_fingerprint(dag_id)
        res = RemoteFlowSource.remove_running_dag(None, QueueType.FAST, fmt_dag_id)
        self.assertEqual(True, res)

    @unittest.skip("ignore")
    def test_remote_release_server_source(self):
        job_id = 90
        RemoteReleaseServerSource.delete_job_release_server(job_id)

    @unittest.skip("ignore")
    def test_pull_pending_job(self):
        process_job_id = RemoteFlowSource.pull_pending_job()
        print(process_job_id)

    @unittest.skip("ignore")
    def test_push_pending_dag(self):
        real_dag_id = 1703
        slave_fingerprint = "9310e6ef6e7bb1b9100ead9b5a0332b57c0e530e_8792_386091"
        fmt_dag_id = f"{slave_fingerprint}_{real_dag_id}"
        RemoteFlowSource.push_pending_dag(None, QueueType.FAST, fmt_dag_id)

    @unittest.skip("ignore")
    def test_remove_all(self):
        ws_id = "xxx"
        job_id = 0
        server_provider = "aligroup"
        RemoteAllocServerSource.remove_all_with_job(
            ws_id, job_id, server_provider
        )

    @unittest.skip("ignore")
    def test_clear_plan_inst_context(self):
        plan_inst_id_list = [33509, 33510, 33511, 33513, 33514, 33516, 33519, 33522]
        for plan_inst_id in plan_inst_id_list:
            Clp.remove_process_plan_inst(plan_inst_id)

    @unittest.skip("ignore")
    def test_master_correct_data(self):
        RemoteFlowSource.correct_running_data()

    @unittest.skip("ignore")
    def test_correct_data_when_program_start(self):
        RemoteFlowSource.correct_data_when_first_start()

    @unittest.skip("ignore")
    def test_correct_by_cache_clean(self):
        RemoteFlowSource.correct_data_by_cache_clean()

    @unittest.skip("ignore")
    def test_whether_init(self):
        res = RemoteFlowSource.whether_init()
        print(f"res is: {res}")


if __name__ == "__main__":
    unittest.main()
