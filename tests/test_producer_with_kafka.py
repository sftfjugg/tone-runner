import json
import unittest
from constant import MsgTopics
from tools.notice.msg_producer import MsgProducer


class TestProducerWithKafka(unittest.TestCase):

    def test_producer(self):
        producer = MsgProducer()
        data = {
            "job_id": 888,
            "state": "success"
        }
        print(producer.send(
            MsgTopics.JOB_TOPIC,
            json.dumps(data).encode(),
            msg_key="job_state_change"
        ))
