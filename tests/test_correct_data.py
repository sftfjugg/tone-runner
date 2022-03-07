import unittest
from core.cache.remote_source import RemoteFlowSource


class TestCorrectionData(unittest.TestCase):

    def test_correct_data(self):
        RemoteFlowSource.correct_running_data()


if __name__ == "__main__":
    unittest.main()
