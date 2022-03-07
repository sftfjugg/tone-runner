import unittest
from celery.result import AsyncResult
from constant import CeleryTaskStatus, ObjectType
from tools.celery import general_async_method
from tools import utils


class CeleryCallClassMethod:

    @classmethod
    def add(cls, x, y):
        return x + y


class CeleryCallInstMethod:

    def __init__(self, default=None):
        self.default = default

    def add(self, x, y):
        return self.default + x + y


class TestCeleryTask(unittest.TestCase):

    def test_general_async_method_for_class(self):
        x, y = 5, 7
        celery_task = general_async_method.delay(
            *utils.inspect_callback_method(CeleryCallClassMethod.add),
            x, y
        )
        print(celery_task.id)

    def test_general_async_method_for_inst(self):
        x, y = 5, 7
        inst_args = [10]
        celery_task = general_async_method.delay(
            *utils.inspect_callback_method(
                CeleryCallInstMethod.add,
                ObjectType.INSTANCE,
                inst_args
            ),
            x, y
        )
        print(celery_task.id)

    def test_celery_task_result(self):
        celery_task_id = "f2284259-6ef6-4986-b805-01db4395b8c4"
        res = AsyncResult(celery_task_id)
        print(res.status, res.result)
        self.assertEqual(CeleryTaskStatus.SUCCESS, res.status)


if __name__ == "__main__":
    unittest.main()
