import config
from kafka import KafkaProducer
from kafka.errors import KafkaError
from env_type import EnvType
from constant import MsgServersForDev, MsgServersForPro


class MsgProducer(object):
    bootstrap_servers = (MsgServersForDev.SERVERS if (EnvType.env in ["dev", "local", "daily"])
                         else MsgServersForPro.SERVERS)
    api_version = config.KAFKA_API_VERSION
    max_block_ms = config.KAFKA_MAX_BLOCK_MS
    retries = config.KAFKA_MSG_RETRIES

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            api_version=self.api_version,
            max_block_ms=self.max_block_ms,
            retries=self.retries
        )

    def send(self, topic_name, msg_value, msg_key=None):
        kafka_server_info = f"\nKafka server:{self.bootstrap_servers}"
        if isinstance(msg_key, str):
            msg_key = msg_key.encode()
        try:
            future = self.producer.send(topic_name, msg_value, key=msg_key)
            result = str(future.get()) + kafka_server_info
            return future.is_done, result
        except KafkaError as error:
            return False, f"Send notice msg has exception, {error}" + kafka_server_info
