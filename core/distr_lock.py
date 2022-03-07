import os
import time
import uuid
from functools import wraps
from tools.redis_cache import redis_cache
from tools.log_util import LoggerFactory


logger = LoggerFactory.dist_lock()


class DistributedLock:
    store = redis_cache

    def __init__(self, lock_name, acquire_timeout=15, lock_timeout=3,
                 who=None, purpose=None, is_release=True):
        self.lock_name = lock_name
        self.acquire_timeout = acquire_timeout
        self.lock_timeout = lock_timeout
        self.identifier = str(uuid.uuid1())
        self.who = who if who else "<pid>" + str(os.getpid())
        self.purpose = purpose
        self.lock = None
        self.is_release = is_release

    def __enter__(self):
        self.lock = self.store.acquire_lock_with_timeout(
            self.lock_name, self.identifier, self.acquire_timeout, self.lock_timeout
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock and self.is_release:
            self.store.release_lock(self.lock_name, self.identifier)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                if self.lock:
                    return func(*args, **kwargs)
        return wrapper

    def get_lock_until_complete(self):
        while 1:
            with self:
                if self.lock:
                    return True
            time.sleep(3)
