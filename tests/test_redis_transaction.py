import time
import unittest
from tools.redis_cache import redis_cache


class TestRedisTransaction(unittest.TestCase):
    que1 = "test_que1"
    que2 = "test_que2"
    test_data = "test_data"
    delay_time = 1

    def init_que(self):
        redis_cache.lpush(self.que2, self.test_data)

    def test_remove_and_push_with_transaction(self):
        """可以适当调长delay_time，在测试过程中停止测试进程，
        预期结果应该是数据还在que2中，正常跑完会到que1中"""
        self.init_que()
        with redis_cache.pipeline() as pipe:
            pipe.lrem(self.que2, 1, self.test_data)
            time.sleep(self.delay_time)
            pipe.lpush(self.que1, self.test_data)
            pipe.execute()

    def test_push_and_remove_with_transaction(self):
        self.init_que()
        with redis_cache.pipeline() as pipe:
            pipe.lpush(self.que1, self.test_data)
            time.sleep(self.delay_time)
            pipe.lrem(self.que2, 1, self.test_data)
            pipe.execute()

    def tearDown(self):
        redis_cache.delete(self.que1)
        redis_cache.delete(self.que2)


if __name__ == "__main__":
    unittest.main()
