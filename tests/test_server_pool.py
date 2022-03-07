import unittest
from core.server.alibaba.alloc_server import AllocServer
from core.server.alibaba.db_operation import AliGroupDbServerOperation
from constant import RunMode, ServerProvider
from models.job import TestJobSuite, TestJobCase


class TestServerPool(unittest.TestCase):

    def test_alloc_server_by_job_case(self):
        job_info = dict()
        job_suite = TestJobSuite.get_by_id(1485)
        job_case = TestJobCase.get_by_id(7189)
        server_info = AllocServer(job_info, job_suite, job_case).get_server_info()
        print(server_info)
        self.assertIn("server_id", server_info)

    def test_ali_group_db_operation(self):
        ws_id = "ucwcr17g"
        job_id = 11
        rand_server = AliGroupDbServerOperation.get_rand_server(ws_id, job_id)
        print(rand_server)
        self.assertIsNotNone(rand_server)

    def test_get_server_by_standalone_tag_pool(self):
        ws_id = "ucwcr17g"
        job_id = 11
        tag_server_set = AliGroupDbServerOperation.get_rand_server_by_tag(ws_id, job_id)
        print(tag_server_set)

    def test_get_spec_tag_server(self):
        tag_id_list = [25]
        tag_server = AliGroupDbServerOperation._get_spec_tag_server(tag_id_list)
        print(tag_server)

    def test_get_rand_server(self):
        ws_id = "ah9m9or5"
        job_id = 447
        rand_server = AliGroupDbServerOperation.get_rand_server(ws_id, job_id)
        print(rand_server)

    def test_get_spec_std_server(self):
        ws_id = "ah9m9or5"
        job_id = 485
        server_id = 162
        spec_server, server_info = AliGroupDbServerOperation.get_spec_std_server_in_pool(ws_id, job_id, server_id)
        print(spec_server, server_info)

    def test_release_server(self):
        job_id = 1219
        server_object_id = 88
        run_mode = RunMode.STANDALONE
        provider = ServerProvider.ALI_CLOUD
        AllocServer.release_server(job_id, server_object_id, run_mode, provider)


if __name__ == "__main__":
    unittest.main()
