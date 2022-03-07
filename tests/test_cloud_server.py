import unittest

import config
from lib.cloud.drivers.aliyun.ecs_driver import EcsDriver


class TestSaveResult(unittest.TestCase):

    # @unittest.skip("ignore")
    def test_save_result(self):
        cloud_driver = EcsDriver(config.ALY_ACCESS_ID, config.ALY_ACCESS_KEY, 'cn-hangzhou', 'cn-hangzhou-h')
        node = cloud_driver.describe_instance('i-bp1bllim99f5st19lg3o')
        print(node)


if __name__ == "__main__":
    unittest.main()
