import time
import weakref
import collections

__all__ = ["local_cache", "local_stable_cache"]


class __LocalCache:

    class Dict(dict):
        def __del__(self):
            pass

    def __init__(self, max_len=10):
        self.weak = weakref.WeakValueDictionary()
        self.strong = collections.deque(maxlen=max_len)

    @staticmethod
    def now_time():
        return int(time.time())

    def get(self, key):
        value = self.weak.get(key, None)
        if value is not None:
            if "expire" in value:
                expire = value[r'expire']
                if self.now_time() < expire:
                    return None
        return value

    def set(self, key, value):
        self.weak[key] = strong_ref = self.Dict(value)
        self.strong.append(strong_ref)


class __LocalStableCache:

    def __init__(self):
        self.cache = dict()

    def get(self, key):
        return self.cache.get(key, None)

    def set(self, key, value):
        self.cache[key] = value


local_cache = __LocalCache(1000)
local_stable_cache = __LocalStableCache()
