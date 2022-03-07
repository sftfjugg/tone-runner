import unittest
from core.op_acc_msg import OperationAccompanyMsg as Oam
from models.server import TestServer


class TestOperationAccompanyMsg(unittest.TestCase):

    @unittest.skip("ignore")
    def test_send_msg_with_job_complete(self):
        job_id = 655
        state = "success"
        Oam.send_msg_with_job_complete(job_id, state)

    @unittest.skip("ignore")
    def test_send_msg_with_plan_complete(self):
        plan_id = 1
        state = "success"
        Oam.send_msg_with_plan_complete(plan_id, state)

    @unittest.skip("ignore")
    def test_send_msg_with_server_broken(self):
        job_id = 665
        server = TestServer.get_by_id(91)
        Oam.set_server_broken_and_send_msg(job_id, server)
