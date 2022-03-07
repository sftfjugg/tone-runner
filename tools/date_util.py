import calendar
import logging
import time
from datetime import datetime, timedelta

DATE_TIME_FMT = '%Y-%m-%d %H:%M:%S'
DATE_TIME_FMT_HWC = '%Y-%m-%dT%H:%M:%S'
DATE_TO_DAY = '%Y/%m/%d'
TODAY = '%Y-%m-%d 00:00:00'

logger = logging.getLogger()


class DateUtil:
    @classmethod
    def now(cls):
        return cls.datetime_to_str(datetime.now())

    @staticmethod
    def day_start(start=0):
        # type: (int) -> str
        """
        n天前的0点时刻
        :param start: 几天前
        :return: 日期时间字符串格式
        """
        return (datetime.today() - timedelta(days=start)).strftime(TODAY)

    @staticmethod
    def date_to_day():
        """
        精简显示日期
        :return: '%Y/%m/%d'
        """
        return time.strftime(DATE_TO_DAY)

    @staticmethod
    def str_to_datetime(dt):
        # type: (str) -> datetime|str
        """
        将字符串datetime转换为datetime对象
        :param dt: 字符串形式datetime
        :return: datetime
        """
        if isinstance(dt, str):
            try:
                return datetime.strptime(dt, DATE_TIME_FMT)
            except Exception as error:
                logger.error(error)
                return dt
        else:
            return dt

    @staticmethod
    def datetime_to_str(dt, fmt=DATE_TIME_FMT):
        # type: (datetime, str) -> str|datetime
        """
        将datetime对象格式化成字符串
        :param dt: datetime对象
        :param fmt: 格式
        :return: 字符串
        """
        if isinstance(dt, str):
            return dt
        return dt.strftime(fmt) if dt else None

    @staticmethod
    def datetime_add(dt, hours=8):
        if dt:
            return dt + timedelta(hours=hours)
        else:
            return None

    @staticmethod
    def datetime_subtract(dt, hours=8):
        if dt:
            return dt - timedelta(hours=hours)
        else:
            return None

    @staticmethod
    def get_month_first_day(year=None, month=None):
        """
        :param year: 年份，默认是本年，可传int或str类型
        :param month: 月份，默认是本月，可传int或str类型
        :return: 当月的第一天，datetime.date类型
        """
        if year:
            year = int(year)
        else:
            year = datetime.today().year

        if month:
            month = int(month)
        else:
            month = datetime.today().month
        first = datetime(year=year, month=month, day=1)
        return first

    @staticmethod
    def get_month_last_day(year=None, month=None):
        """
        :param year: 年份，默认是本年，可传int或str类型
        :param month: 月份，默认是本月，可传int或str类型
        :return: 当月的最后一天，datetime.date类型
        """
        if year:
            year = int(year)
        else:
            year = datetime.today().year

        if month:
            month = int(month)
        else:
            month = datetime.today().month
        _, month_total = calendar.monthrange(year, month)
        last = datetime(year=year, month=month, day=month_total)
        return last

    @classmethod
    def format_create_time(cls, dt, hours=8):
        create_time_obj = datetime.strptime(dt, DATE_TIME_FMT_HWC)
        add_time_obj = cls.datetime_add(create_time_obj, hours=hours)
        return cls.datetime_to_str(add_time_obj)
