import os
import logging
import functools
import time

import config
from logging import handlers
from logging import FileHandler
__all__ = ["LoggerFactory"]


class SafeFileHandler(FileHandler):
    def __init__(self, filename, mode="a", encoding=None, delay=0, suffix="%Y-%m-%d_%H"):
        current_time = time.strftime(suffix, time.localtime())
        FileHandler.__init__(self, filename + "." + current_time, mode, encoding, delay)

        self.filename = os.fspath(filename)

        self.mode = mode
        self.encoding = encoding
        self.suffix = suffix
        self.suffix_time = current_time

    def emit(self, record):
        try:
            if self.check_base_filename():
                self.build_base_filename()
            FileHandler.emit(self, record)
        except(KeyboardInterrupt, SystemExit):
            raise
        except (IOError, Exception):
            self.handleError(record)

    def check_base_filename(self):
        time_tuple = time.localtime()

        if self.suffix_time != time.strftime(self.suffix, time_tuple) or not os.path.exists(
                os.path.abspath(self.filename) + '.' + self.suffix_time):
            return 1
        else:
            return 0

    def build_base_filename(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        current_time_tuple = time.localtime()
        self.suffix_time = time.strftime(self.suffix, current_time_tuple)
        self.baseFilename = os.path.abspath(self.filename) + "." + self.suffix_time

        if not self.delay:
            self.stream = open(self.baseFilename, self.mode, encoding=self.encoding)


class Logger(object):
    level_relations = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    def __init__(self, filename, level="info",
                 when="D", back_count=5,
                 fmt="%(asctime)s - %(filename)s - %(funcName)s[line:%(lineno)d]"
                     " - %(levelname)s: %(message)s"
                 ):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)  # 设置日志格式
        self.logger.setLevel(self.level_relations.get(level))  # 设置日志级别
        sh = logging.StreamHandler()  # 往屏幕上输出
        sh.setFormatter(format_str)  # 设置屏幕上显示的格式
        handlers.TimedRotatingFileHandler(
            filename=filename, when=when,
            backupCount=back_count,
            encoding="utf-8"
        )
        # 按照时间戳对日志进行拆分
        th = SafeFileHandler(
            filename=filename,
            encoding="utf-8",
            suffix='%Y-%m-%d'
        )
        th.setFormatter(format_str)  # 设置文件里写入的格式
        self.logger.addHandler(sh)   # 把对象加到logger里
        self.logger.addHandler(th)


def singleton(cls):
    instance = {}

    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        log_name = args[0]
        if log_name not in instance:
            instance[log_name] = cls(*args, **kwargs)
        return instance[log_name]

    return get_instance


@singleton
class SingleLogger:

    def __init__(self, log_name, log_level="info", **kwargs):
        log_dir = os.path.dirname(log_name)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.logger = Logger(log_name, log_level, **kwargs).logger


class LoggerFactory:
    LOG_LEVEL = config.LOG_LEVEL
    LOG_PATH = config.LOG_PATH

    @classmethod
    def summary_error_log(cls):
        file_name = os.path.join(cls.LOG_PATH, "summary_error.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def scheduler(cls):
        file_name = os.path.join(cls.LOG_PATH, "scheduler.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def worker(cls):
        """暂时未用到，celery暂未使用"""
        file_name = os.path.join(cls.LOG_PATH, "worker.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def exec_channel(cls):
        file_name = os.path.join(cls.LOG_PATH, "exec_channel.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def job_complete(cls):
        file_name = os.path.join(cls.LOG_PATH, "job_complete.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def job_result(cls):
        file_name = os.path.join(cls.LOG_PATH, "job_result.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def plan_complete(cls):
        file_name = os.path.join(cls.LOG_PATH, "plan_complete.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def alloc_server(cls):
        file_name = os.path.join(cls.LOG_PATH, "alloc_server.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def release_server(cls):
        file_name = os.path.join(cls.LOG_PATH, "release_server.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def storage(cls):
        file_name = os.path.join(cls.LOG_PATH, "storage.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def build_package(cls):
        file_name = os.path.join(cls.LOG_PATH, "build_package.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def query_package(cls):
        file_name = os.path.join(cls.LOG_PATH, "query_package.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def check_machine(cls):
        file_name = os.path.join(cls.LOG_PATH, "check_machine.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def notice_msg(cls):
        file_name = os.path.join(cls.LOG_PATH, "notice_msg.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def dist_lock(cls):
        file_name = os.path.join(cls.LOG_PATH, "dist_lock.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def program_init(cls):
        file_name = os.path.join(cls.LOG_PATH, "program_init.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger

    @classmethod
    def queue(cls):
        file_name = os.path.join(cls.LOG_PATH, "queue.log")
        return SingleLogger(file_name, cls.LOG_LEVEL).logger
