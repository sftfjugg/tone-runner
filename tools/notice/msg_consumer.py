import config
from kafka import KafkaConsumer
from constant import MsgServersForDev, MsgGroups, MsgServersForPro
from env_type import EnvType


class MsgConsumer(object):
    api_version = config.KAFKA_API_VERSION
    session_timeout_ms = config.KAFKA_SESSION_TIMEOUT_MS
    max_poll_records = config.KAFKA_MAX_POLL_RECORDS
    fetch_max_bytes = config.KAFKA_FETCH_MAX_BYTES

    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=MsgServersForDev.SERVERS if EnvType.env == "dev" else MsgServersForPro.SERVERS,
            group_id=MsgGroups.JOB_GROUP,
            api_version=self.api_version,
            session_timeout_ms=self.session_timeout_ms,
            max_poll_records=self.max_poll_records,
            fetch_max_bytes=self.fetch_max_bytes
        )
