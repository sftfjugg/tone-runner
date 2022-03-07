import json
from config import H_CACHE_IP2SN, H_CACHE_SN2IP
from .redis_cache import redis_cache
from .log_util import LoggerFactory

logger = LoggerFactory.scheduler()


def cache_ip2sn(ip, sn):
    if ip and sn:
        return redis_cache.hset(H_CACHE_IP2SN, ip, sn)
    return False


def get_sn_by_ip(ip):
    sn = redis_cache.hget(H_CACHE_IP2SN, ip)
    if sn:
        return sn
    for i in range(3):
        try:
            resp = None
            if not resp:
                raise Exception('get_sn_by_ip -> query skyline failed !')
            jd = json.loads(resp)
            sn = jd['value']['itemList'][0]['sn']
            cache_sn2ip(sn, ip)
            cache_ip2sn(ip, sn)
            return sn
        except Exception as e:
            logger.error('get_sn_by_ip error: %s ip: %s' % (str(e), ip))


def cache_sn2ip(sn, ip):
    if sn and ip:
        return redis_cache.hset(H_CACHE_SN2IP, sn, ip)
    return False


def get_ip_by_sn(sn):
    ip = redis_cache.hget(H_CACHE_SN2IP, sn)
    if ip:
        return ip
    for i in range(3):
        try:
            resp = None
            if not resp:
                raise Exception('get_ip_by_sn -> query skyline failed !')
            jd = json.loads(resp)
            ip = jd['value']['itemList'][0]['ip']
            cache_ip2sn(ip, sn)
            cache_sn2ip(sn, ip)
            return ip
        except Exception as e:
            logger.error('get_ip_by_sn error: %s' % str(e))


def get_all_cached_ip():
    return redis_cache.hgetall(H_CACHE_IP2SN)


def get_all_cached_sn():
    return redis_cache.hgetall(H_CACHE_SN2IP)
