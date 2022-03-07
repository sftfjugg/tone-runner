import os
import time
from lib import peewee
from config import DATABASE
from tools import utils
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
    gmt_created = peewee.DateTimeField(default=utils.get_now())
    gmt_modified = peewee.DateTimeField(default=utils.get_now())
    is_deleted = peewee.BooleanField(default=False)

    class Meta:
        database = db

    def to_dict(self):
        result_dict = dict()
        result_dict['gmt_created'] = self.gmt_created
        result_dict['gmt_modified'] = self.gmt_modified
        return result_dict

    def save(self, *args, **kwargs):
        self.gmt_modified = utils.get_now()
        return super(BaseModel, self).save(*args, **kwargs)

    @classmethod
    def create(cls, **query):
        query.update({"gmt_created": utils.get_now()})
        return super(BaseModel, cls).create(**query)

    @classmethod
    def update(cls, **kwargs):
        kwargs.update({"gmt_modified": utils.get_now()})
        return super(BaseModel, cls).update(**kwargs)

    @classmethod
    def _check_is_deleted(cls, dq_node):
        if isinstance(dq_node.lhs, peewee.Expression):
            if cls._check_is_deleted(dq_node.lhs):
                return cls._check_is_deleted(dq_node.lhs)
            if cls._check_is_deleted(dq_node.rhs):
                return cls._check_is_deleted(dq_node.rhs)
        else:
            return dq_node.lhs.name == "is_deleted"

    @classmethod
    def filter(cls, *dq_nodes, **filters):
        for dq_node in dq_nodes:
            if cls._check_is_deleted(dq_node):
                return super(BaseModel, cls).filter(*dq_nodes, **filters)
        else:
            dq_node = peewee.Expression(
                lhs=cls.is_deleted,
                op="=",
                rhs=False
            )
            dq_nodes = list(dq_nodes)
            dq_nodes.append(dq_node)
            dq_nodes = tuple(dq_nodes)
            return super(BaseModel, cls).filter(*dq_nodes, **filters)
