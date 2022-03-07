import unittest
from core.server.alibaba.check_machine import RealStateCorrection
from models.server import TestServer


class TestConfig(unittest.TestCase):

    def test_do_exec(self):
        server = TestServer.get(id=91)
        res = RealStateCorrection._get_server_use_state(server, True)
        print(res)


if __name__ == "__main__":
    unittest.main()
