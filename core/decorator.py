import time
from functools import wraps
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


def retry(times=5, interval=0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            exp_meet = False
            for i in range(times):
                try:
                    ret = func(*args, **kwargs)
                except Exception as err:
                    exp_meet = True
                    exp_info = str(err)
                    if i == (times - 1):
                        raise Exception(
                            'retry %s failed(max times=%s interval=%s) but error happens: %s' % (
                                func.__name__, times, interval, exp_info
                            )
                        )
                    else:
                        logger.warn(
                            'retry %s failed for %s times(max times=%s interval=%s) '
                            'with error happens: %s' % (func.__name__, i, times, interval, exp_info)
                        )
                        logger.warn('sleep %s and continue ..' % interval)
                        time.sleep(interval)
                        continue
                if not exp_meet:
                    return ret
        return wrapper
    return decorator


def error_detect(except_class=Exception, except_logger=logger):
    def wrapper(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                except_logger.exception(
                    f"exec {func.__name__} has exception: "
                )
                raise except_class(error)
        return _wrapper
    return wrapper
