import unittest
from constant import MsgTopics
from tools.notice.msg_consumer import MsgConsumer


class TestConsumerWithKafka(unittest.TestCase):

    def test_consumer(self):
        consumer = MsgConsumer().consumer
        consumer.subscribe([MsgTopics.JOB_TOPIC, MsgTopics.PLAN_TOPIC, MsgTopics.MACHINE_TOPIC])
        for message in consumer:
            print(message.value, message.key)
