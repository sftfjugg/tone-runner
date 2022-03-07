import time
import unittest
from core.distr_lock import DistributedLock


class TestDistributeLock(unittest.TestCase):

    @DistributedLock("test-tmp-001", 3, 15, purpose="test", is_release=False)
    def _test_dist_lock_with_decorator(self):
        time.sleep(5)
        print("Test done with decorator.")

    @staticmethod
    def _test_dist_lock_with_context():
        with DistributedLock("test-tmp-001", 3, 15, purpose="test") as Lock:
            if Lock.lock:
                time.sleep(6)
        print("Test done with context.")

    @unittest.skip("skip")
    def test_dist_lock_with_decorator(self):
        self._test_dist_lock_with_decorator()

    @unittest.skip("skip")
    def test_dist_lock_with_context(self):
        self._test_dist_lock_with_context()

    @unittest.skip("skip")
    def test_get_lock_until_complete(self):
        if DistributedLock(
                "start program",
                lock_timeout=30,
                is_release=False
        ).get_lock_until_complete():
            print("ok")


if __name__ == "__main__":
    unittest.main()
