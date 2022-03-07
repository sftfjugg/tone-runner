import os
import sys
import config
from celery import Celery
from core.exception import AsyncCallException
from constant import TaskExchange, ObjectType
from .log_util import LoggerFactory
sys.path.append(os.getcwd())


logger = LoggerFactory.worker()


app = Celery(
    "tone-runner",
    broker=config.CELERY_BROKER_ADDRESS,
    backend=config.CELERY_BACKEND_ADDRESS
)
app.conf.timezone = 'Asia/Shanghai'
app.conf.enable_utc = False
app.conf.task_publish_retry_policy = {
    'max_retries': 3,
    'interval_start': 0,
    'interval_step': 0.2,
    'interval_max': 0.2
}
app.conf.task_routes = {
    'tools.celery.general_async_method': {'queue': TaskExchange.GENERAL},
}
app.conf.celery_task_result_expires = 86400 * 3
app.conf.celery_max_cached_results = 10000
inspect_celery_worker = app.control.inspect()


@app.task()
def general_async_method(*args, **kwargs):
    try:
        return _async_method(*args, **kwargs)
    except Exception as error:
        logger.exception(error)


def _async_method(
        callback_module,
        callback_object=None,
        object_type=ObjectType.CLASS,
        inst_args=None,
        inst_kwargs=None,
        callback_method=None,
        *args, **kwargs
):
    """
    General async method
    :param callback_module: module name
    :param callback_object: object name
    :param object_type: class | instance
    :param inst_args: class instance args
    :param inst_kwargs: class instance kwargs
    :param callback_method: method name
    :param args: method or function args
    :param kwargs: method or function kwargs
    :return:
    """
    try:
        logger.info(
            f"callback_module:{callback_module}, callback_object={callback_object}, "
            f"object_type={object_type if callback_object else 'module'}, "
            f"inst_args={inst_args}, inst_kwargs={inst_kwargs}, "
            f"callback_method={callback_method}, method_args={args}, "
            f"method_kwargs={kwargs}"
        )
        if not callback_method:
            raise AsyncCallException("The callback method cannot be null!")
        _callback_module = __import__(callback_module, fromlist=["Any"])
        if callback_object:
            _callback_object = getattr(_callback_module, callback_object)
            if object_type == ObjectType.INSTANCE:
                inst_args = inst_args or list()
                inst_kwargs = inst_kwargs or dict()
                _callback_object = _callback_object(*inst_args, **inst_kwargs)
            _callback_method = getattr(_callback_object, callback_method)
            return _callback_method(*args, **kwargs)
        else:
            _callback_method = getattr(_callback_module, callback_method)
            return _callback_method(*args, **kwargs)
    except (AttributeError, ModuleNotFoundError):
        logger.exception(
            "An exception was encountered before method real executed, please check up:"
        )
