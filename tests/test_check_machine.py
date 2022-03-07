import unittest
import threading
from constant import ProcessDataSource
from core.server.alibaba.check_machine import RealStateCorrection


class TestCheckMachine(unittest.TestCase):

    check_machine_inst = None

    def setUp(self):
        acquire_timeout = 15
        lock_timeout = 6
        self.check_machine_inst = RealStateCorrection(
            ProcessDataSource.CHECK_REAL_STATE_LOCK,
            acquire_timeout,
            lock_timeout
        )

    @staticmethod
    def one_thread_get_lock():
        acquire_timeout = 15
        lock_timeout = 10
        RealStateCorrection(
            ProcessDataSource.CHECK_REAL_STATE_LOCK,
            acquire_timeout,
            lock_timeout
        ).process()

    @unittest.skip("ignore")
    def test_multi_thread_get_lock(self):
        for _ in range(6):
            threading.Thread(target=self.one_thread_get_lock()).start()

    @unittest.skip("ignore")
    def test_check_machine_process(self):
        self.check_machine_inst.process()


if __name__ == "__main__":
    unittest.main()
