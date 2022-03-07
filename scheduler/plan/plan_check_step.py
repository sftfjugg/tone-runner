import json
import config
from core.exec_channel import ExecChannel
from constant import get_agent_res_obj, get_agent_res_status_obj, ExecState
from models.job import BuildJob, TestJob


class PlanCheckStep:

    @classmethod
    def check_build_info(cls, build_job_id):
        build_kernel = BuildJob.get_by_id(build_job_id)
        if build_kernel.rpm_list:
            rpm_list = json.loads(build_kernel.rpm_list)
        else:
            rpm_list = []
        return build_kernel.state, rpm_list, build_kernel.build_msg

    @classmethod
    def check_prepare(cls, channel_type, tid):
        state, output = ExecState.RUNNING, None
        agent_res = get_agent_res_obj(channel_type)
        agent_res_status = get_agent_res_status_obj(channel_type)
        result = ExecChannel.do_query(channel_type, tid=tid)
        status = result[agent_res.STATUS]
        success = result[agent_res.SUCCESS]
        error_msg = result.get(agent_res.ERROR_MSG)
        exec_result = result[agent_res.JOB_RESULT] or ""
        output = exec_result[:config.EXEC_RESULT_LIMIT] or error_msg
        if agent_res_status.check_end(status):
            if agent_res.check_success(success):
                state = ExecState.SUCCESS
            else:
                state = ExecState.FAIL
        return state, output

    @classmethod
    def check_test(cls, job_id):
        test_job = TestJob.get_or_none(id=job_id)
        if test_job:
            return test_job.state, test_job.state_desc
        else:
            return ExecState.FAIL, f"Test job is deleted, job_id:{job_id}"
