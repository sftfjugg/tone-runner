import unittest
from scheduler.plan.create_job import CreateJob
from scheduler.plan.plan_executor import PlanExecutor


class TestCreateJob(unittest.TestCase):

    def test_create_job(self):
        create_job_info = {
            "username": "test_user",
            "user_token": "",
            "template_id": 98,
            "project_id": 1
        }
        res = CreateJob(**create_job_info).create()
        print(res.text)

    def test_create_job_by_plan_inst(self):
        plan_inst_id = 30816
        instance_stage_id = 23549
        success, job_id, err_msg = PlanExecutor.create_plan_job(plan_inst_id, instance_stage_id)
        print(success, job_id, err_msg)


if __name__ == "__main__":
    unittest.main()
