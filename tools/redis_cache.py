import time
import math
import redis
import config
from tools.log_util import LoggerFactory


logger = LoggerFactory.storage()


class RedisCache(object):

    def __init__(self, cache_db=None):
        if not cache_db:
            cache_db = config.REDIS_CACHE_DB
        self.pool = redis.ConnectionPool(host=config.REDIS_HOST, port=int(config.REDIS_PORT),
                                         password=config.REDIS_PASSWORD,
                                         decode_responses=True,
                                         db=int(cache_db))
        self.redis_cursor = redis.Redis(connection_pool=self.pool)

    @property
    def cursor(self):
        while 1:
            try:
                if not self.redis_cursor.ping():
                    logger.info("检测到redis连接不可用，正在尝试重新连接...")
                    self.redis_cursor = redis.Redis(connection_pool=self.pool)
            except Exception as error:
                error_msg = "redis连接异常,正在尝试重连,异常信息: " + str(error)
                logger.error(error_msg)
                time.sleep(1)
                continue
            else:
                return self.redis_cursor

    def get(self, key):
        return self.cursor.get(key)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        return self.cursor.set(name=name, value=value, ex=ex, px=px, nx=nx, xx=xx)

    def lindex(self, name, idx):
        return self.cursor.lindex(name, idx)

    def hset(self, name, key, value):
        return self.cursor.hset(name, key, value)

    def hmset(self, name, mapping):
        return self.cursor.hmset(name, mapping)

    def hsetnx(self, name, key, value):
        return self.cursor.hsetnx(name, key, value)

    def hget(self, name, key):
        return self.cursor.hget(name, key)

    def hmget(self, name, *keys):
        return self.cursor.hmget(name, keys)

    def hgetall(self, name):
        return self.cursor.hgetall(name)

    def hexists(self, name, key):
        return self.cursor.hexists(name, key)

    def hlen(self, name):
        return self.cursor.hlen(name)

    def hdel(self, name, *keys):
        return self.cursor.hdel(name, *keys)

    def expire(self, name, timeout):
        return self.cursor.expire(name, timeout)

    def delete(self, *name):
        return self.cursor.delete(*name)

    def setnx(self, name, timeout):
        return self.cursor.setnx(name, timeout)

    def exists(self, name):
        return self.cursor.exists(name)

    def delete_like(self, name):
        keys = self.cursor.keys(name)
        if keys:
            return self.cursor.delete(*keys)
        return None

    def sadd(self, name, *value):
        return self.cursor.sadd(name, *value)

    def sismember(self, name, value):
        return self.cursor.sismember(name, value)

    def spop(self, name, count=None):
        return self.cursor.spop(name, count)

    def zrank(self, name, value):
        return self.cursor.zrank(name, value)

    def zadd(self, name, args, *kwargs):
        return self.cursor.zadd(name, args, *kwargs)

    def zrem(self, name, *values):
        return self.cursor.zrem(name, *values)

    def lpush(self, name, *values):
        return self.cursor.lpush(name, *values)

    def rpop(self, name):
        return self.cursor.rpop(name)

    def rpoplpush(self, src, dst):
        return self.cursor.rpoplpush(src, dst)

    def scard(self, name):
        return self.cursor.scard(name)

    def srem(self, name, *values):
        return self.cursor.srem(name, *values)

    def llen(self, name):
        return self.cursor.llen(name)

    def lrem(self, name, count, value):
        return self.cursor.lrem(name, count, value)

    def keys(self, pattern):
        return self.cursor.keys(pattern)

    def smembers(self, name):
        return self.cursor.smembers(name)

    def lrange(self, name, start, end):
        return self.cursor.lrange(name, start, end)

    def hscan_iter(self, name, match=None, count=None):
        return self.cursor.hscan_iter(name, match, count)

    def hkeys(self, name):
        return self.cursor.hkeys(name)

    def hincrby(self, name, key, amount=1):
        return self.cursor.hincrby(name, key, amount)

    def zincrby(self, name, amount, value):
        return self.cursor.zincrby(name, amount, value)

    def zrange(self, name, start, end):
        return self.cursor.zrange(name, start, end)

    def flushdb(self):
        return self.cursor.flushdb()

    def pipeline(self, transaction=True, shard_hint=None):
        return self.cursor.pipeline(transaction, shard_hint)

    def acquire_lock_with_timeout(self, lock_name, identifier,
                                  acquire_timeout=3, lock_timeout=2):
        """
        基于 Redis 实现的分布式锁
        :param lock_name: 锁的名称
        :param identifier: 锁的标识
        :param acquire_timeout: 获取锁的超时时间，默认 3 秒
        :param lock_timeout: 锁的超时时间，默认 2 秒
        :return:
        """
        lock_name = f'lock:{lock_name}'
        lock_timeout = int(math.ceil(lock_timeout))
        end = time.time() + acquire_timeout

        while time.time() < end:
            # 如果不存在这个锁则加锁并设置过期时间，避免死锁
            if self.cursor.set(lock_name, identifier, ex=lock_timeout, nx=True):
                return identifier

            time.sleep(0.1)

        return None

    def release_lock(self, lock_name, identifier):
        """
        释放锁
        :param lock_name: 锁的名称
        :param identifier: 锁的标识
        :return:
        """
        unlock_script = """
        if redis.call("get",KEYS[1]) == ARGV[1] then
            return redis.call("del",KEYS[1])
        else
            return 0
        end
        """
        lock_name = f'lock:{lock_name}'
        unlock = self.cursor.register_script(unlock_script)
        result = unlock(keys=[lock_name], args=[identifier])
        if result:
            return True
        else:
            return False


redis_cache = RedisCache()
