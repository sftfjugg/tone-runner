import time
import unittest
from constant import ExecState
from models.job import TestJob, TestJobCase
from models.base import db


class TestJobModule(unittest.TestCase):

    def test_job_name(self):
        job = TestJob.filter(id=1).first()
        self.assertEqual("test001", job.name)


class TestDagModule(unittest.TestCase):

    def test_overwrite_save(self):
        test_job_case = TestJobCase.get(id=1)
        test_job_case.state = ExecState.RUNNING
        test_job_case.save()
        test_job_case = TestJobCase.get(id=1)
        self.assertEqual(ExecState.RUNNING, test_job_case.state)

    def test_overwrite_update(self):
        TestJobCase.update(state=ExecState.RUNNING).where(
            TestJobCase.id == 1,
        ).execute()
        test_job_case = TestJobCase.get(id=1)
        self.assertEqual(ExecState.RUNNING, test_job_case.state)


class TestDbSession(unittest.TestCase):

    def db_operation1(self, job_case_id):
        self.db_operation2(job_case_id)
        # raise Exception("error")

    def db_operation2(self, job_case_id):
        self.db_operation3(job_case_id)
        # raise Exception("error")

    @staticmethod
    def db_operation3(job_case_id):
        TestJobCase.update(state=ExecState.FAIL).where(
            TestJobCase.id == job_case_id,
        ).execute()
        print("sleep 30...")
        time.sleep(30)
        raise Exception("error")

    # @db.atomic()
    def db_session(self, job_case_id):
        self.db_operation1(job_case_id)
        # raise Exception("db operation has error, rollback...")

    @db.atomic()
    def test_db_session(self):
        job_case_id = 4
        try:
            TestJobCase.update(state="running", note="xxx").where(
                TestJobCase.id == job_case_id,
            ).execute()
            self.db_session(job_case_id)
        except Exception as error:
            print(error)
        finally:
            self.assertNotEqual(
                ExecState.FAIL,
                TestJobCase.get_by_id(job_case_id).state
            )


if __name__ == "__main__":
    unittest.main()
