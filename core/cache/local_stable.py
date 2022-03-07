import config
from constant import ProcessDataSource
from tools.local_cache import local_stable_cache
from tools import utils


class LocalStableSource:

    STABLE_SOURCE_STORE = local_stable_cache

    @classmethod
    def get_device_fingerprint(cls):
        """Get the device's fingerprint from cache or file"""
        fingerprint = cls.STABLE_SOURCE_STORE.get(
            ProcessDataSource.FINGERPRINT
        )
        if not fingerprint:
            fingerprint = utils.get_fingerprint_from_file(config.FINGERPRINT_FILE)
            cls.set_device_fingerprint(fingerprint)
        return fingerprint

    @classmethod
    def set_device_fingerprint(cls, fingerprint):
        """Sets the device's fingerprint to the cache"""
        cls.STABLE_SOURCE_STORE.set(
            ProcessDataSource.FINGERPRINT,
            fingerprint
        )
