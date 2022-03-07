import os
import time
import peewee
from config import DATABASE
from tools.log_util import LoggerFactory


logger = LoggerFactory.storage()


def singleton(cls, *args, **kw):
    # 解决peewee不支持多进程的问题
    instances = {}

    def _singleton():
        key = str(cls) + str(os.getpid())
        logger.info(key)
        if key not in instances:
            instances[key] = cls(*args, **kw)
        return instances[key]

    return _singleton


class RetryOperationalError:

    def execute_sql(self, sql, params=None, commit=True):
        err_msg = "Execute sql has error, now try reconnecting..."
        try:
            cursor = super(RetryOperationalError, self).execute_sql(sql, params, commit)
            return cursor
        except (peewee.OperationalError, peewee.InterfaceError):
            logger.error(err_msg)
            while 1:
                if not self.is_closed():
                    self.close()
                try:
                    cursor = self.cursor()
                    cursor.execute(sql, params or ())
                    if commit and not self.in_transaction():
                        self.commit()
                except (peewee.OperationalError, peewee.InterfaceError):
                    logger.error(err_msg)
                    time.sleep(1)
                else:
                    return cursor


class RetryMySQLDatabase(RetryOperationalError, peewee.MySQLDatabase):
    pass


@singleton
class DB(object):
    def __init__(self):
        self.mysql_db = RetryMySQLDatabase(**DATABASE)


def __get_db():
    database = DB().mysql_db
    database.connect()
    return database


db = __get_db()


class BaseModel(peewee.Model):
    id = peewee.AutoField(primary_key=True)

    class Meta:
        database = db


class User(BaseModel):
    username = peewee.CharField()
    password = peewee.CharField()
    first_name = peewee.CharField()
    last_name = peewee.CharField()
    email = peewee.CharField()
    is_staff = peewee.SmallIntegerField()
    is_active = peewee.SmallIntegerField()
    is_superuser = peewee.SmallIntegerField()
    last_login = peewee.DateTimeField()
    date_joined = peewee.DateTimeField()
    emp_id = peewee.CharField()
    job_desc = peewee.CharField()
    dep_desc = peewee.CharField()
    token = peewee.CharField()

    class Meta:
        table_name = "user"
