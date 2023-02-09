import json
import traceback
import config
from core.exception import ExecChanelException
from core.exec_channel import ExecChannel
from constant import (
    ExecState,
    get_agent_res_obj,
    PlanStage,
    BuildKernelFrom,
    JobCfgFields,
    TestType,
    BaselineType
)
from models.plan import (
    PlanInstanceStageRelation as Pis,
    PlanInstancePrepareRelation as Pip,
    PlanInstanceTestRelation as Pit
)
from models.plan import PlanInstance
from models.job import BuildJob, TestJob, TestTemplate, JobType
from models.baseline import Baseline
from models.sysconfig import KernelInfo
from tools.log_util import LoggerFactory
from tools import utils
from tools.utils import kernel_info_format
from .plan_common import PlanCommon
from .plan_check_step import PlanCheckStep
from .plan_server import PlanServer
from .create_job import CreateJob

logger = LoggerFactory.scheduler()
plan_complete_logger = LoggerFactory.plan_complete()


class PlanExecutor:

    @staticmethod
    def _convert_build_info(build_info):
        _build_info = dict()
        _build_info["product_id"] = build_info["name"]
        _build_info["git_repo"] = build_info["code_repo"]
        _build_info["git_branch"] = build_info["code_branch"]
        _build_info["git_commit"] = build_info["commit_id"]
        _build_info["build_branch"] = build_info.get("compile_branch")
        _build_info["arch"] = build_info.get("cpu_arch")
        _build_info["build_config"] = build_info.get("build_config")
        _build_info["build_machine"] = build_info.get("build_machine")
        return _build_info

    @classmethod
    def build_kernel_package(cls, plan_inst):
        plan_id, plan_inst_id = plan_inst.plan_id, plan_inst.id
        build_name = f"build-pkg-for-plan-{plan_id}-by-tone-runner"
        build_info = json.loads(plan_inst.build_pkg_info)
        build_kernel = BuildJob.create(
            name=build_name,
            creator=plan_inst.creator,
            build_from=BuildKernelFrom.PLAN,
            **cls._convert_build_info(build_info)
        )
        build_job_id = build_kernel.id
        cls.update_plan_inst_running(plan_inst_id)
        cls.update_plan_inst_build_kernel_id(plan_inst.id, build_job_id)
        return build_job_id

    @classmethod
    def update_plan_inst_build_kernel_id(cls, plan_inst_id, build_job_id):
        PlanInstance.update(build_job_id=build_job_id).where(
            PlanInstance.id == plan_inst_id
        ).execute()

    @classmethod
    def update_plan_kernel_info(cls, plan_inst, rpm_list):
        headers, dev, kernel = rpm_list
        kernel_info = json.loads(plan_inst.kernel_info)
        kernel_info[JobCfgFields.HEADERS] = headers
        kernel_info[JobCfgFields.DEV] = dev
        kernel_info[JobCfgFields.KERNEL] = kernel
        kernel_info[JobCfgFields.KERNEL_PACKAGES] = rpm_list
        PlanInstance.update(
            kernel_info=json.dumps(kernel_info)
        ).where(
            PlanInstance.id == plan_inst.id
        ).execute()

    @classmethod
    def check_build_step(cls, build_job_id):
        return PlanCheckStep.check_build_info(build_job_id)

    @classmethod
    def _exec_script(cls, channel_type, ip, script):
        tid = err_msg = None
        state = ExecState.FAIL
        try:
            agent_res = get_agent_res_obj(channel_type)
            success, result = ExecChannel.do_exec(channel_type, ip=ip, command=script)
            if success:
                tid = result[agent_res.TID]
                state = ExecState.RUNNING
            else:
                err_msg = str(result)[:config.EXEC_RESULT_LIMIT]
        except ExecChanelException:
            err_msg = traceback.format_exc(config.TRACE_LIMIT)

        return tid, state, err_msg

    @classmethod
    def exec_plan_inst_prepare_script(cls, plan_inst_id, instance_stage_id):
        pending = running = fail = False
        prepare_steps = Pip.filter(instance_stage_id=instance_stage_id, state=ExecState.PENDING)
        for ps in prepare_steps:
            ip, server_sn = ps.ip, ps.server_sn
            channel_type = ps.channel_type
            script = ps.script_info
            ok, server_check_res = PlanServer.get_server_by_plan(plan_inst_id, channel_type, ip, server_sn)
            if ok:
                tid, state, err_msg = cls._exec_script(channel_type, ip, script)
                ps.tid, ps.state, ps.state_desc = tid, state, err_msg
                if state == ExecState.RUNNING:
                    running = True
                if state == ExecState.FAIL:
                    fail = True
                cls.update_plan_inst_running(plan_inst_id)
            else:
                state_desc = PlanServer.get_state_desc_by_server_state(server_check_res)
                ps.state_desc = state_desc
                pending = True
            ps.save()
        return pending, running, fail

    @classmethod
    def check_prepare_running_step(cls, plan_inst_id, instance_stage_id):
        running = False
        prepare_steps = Pip.filter(instance_stage_id=instance_stage_id, state=ExecState.RUNNING)
        for ps in prepare_steps:
            state, output = PlanCheckStep.check_prepare(ps.channel_type, ps.tid)
            if state == ExecState.RUNNING:
                running = True
            ps.state = state
            ps.result = output
            ps.save()
            PlanServer.unlock_plan_server(plan_inst_id, ps.sn)
        return running

    @classmethod
    def check_prepare_end(cls, plan_inst_id, instance_stage_id):
        success, fail, next_inst_stage_id = False, False, None
        state, _ = PlanCommon.check_stage_state(PlanStage.PREPARE_ENV, instance_stage_id)
        if state == ExecState.FAIL:
            fail = True
        elif state == ExecState.SUCCESS:
            success = True
            next_inst_stage_id = PlanCommon.get_next_inst_test_stage_id(plan_inst_id)
        return success, fail, next_inst_stage_id

    @classmethod
    def check_test_end(cls, plan_inst_id, instance_stage_id):
        success, fail, next_inst_stage_id = False, False, None
        pis = Pis.get_by_id(instance_stage_id)
        cur_idx = pis.stage_index
        state, _ = PlanCommon.check_stage_state(PlanStage.TEST_STAGE, instance_stage_id)
        if state == ExecState.FAIL:
            fail = True
            if not pis.impact_next:
                next_inst_stage_id = PlanCommon.get_next_inst_test_stage_id(plan_inst_id, cur_idx)
        elif state == ExecState.SUCCESS:
            success = True
            next_inst_stage_id = PlanCommon.get_next_inst_test_stage_id(plan_inst_id, cur_idx)
        return success, fail, next_inst_stage_id

    @classmethod
    def get_create_job_info_by_plan_inst(cls, plan_inst_id):
        create_job_info = {}
        plan_inst = PlanInstance.get_by_id(plan_inst_id)
        kernel_info = json.loads(plan_inst.kernel_info)
        rpm_info = json.loads(plan_inst.rpm_info)
        env_info = json.loads(plan_inst.env_info)
        baseline_info = json.loads(plan_inst.baseline_info)
        create_job_info["ws_id"] = plan_inst.ws_id
        create_job_info["plan_id"] = plan_inst_id
        create_job_info["project_id"] = plan_inst.project_id
        create_job_info["username"] = plan_inst.username
        create_job_info["token"] = plan_inst.token
        if plan_inst.build_job_id:
            create_job_info["build_job_id"] = plan_inst.build_job_id
        if plan_inst.kernel_version:
            kernel = KernelInfo.filter(version=plan_inst.kernel_version).first()
            if kernel:
                create_job_info["kernel_id"] = kernel.id
        if kernel_info:
            create_job_info["kernel_info"] = kernel_info_format(kernel_info)
        if rpm_info:
            rpm_info_list = list()
            for rpm in rpm_info:
                rpm_res = dict()
                rpm_res["pos"] = "before"
                rpm_res["rpm"] = rpm
                rpm_info_list.append(rpm_res)
            create_job_info["rpm_info"] = rpm_info_list
        if env_info:
            create_job_info["env_info"] = ",".join(
                map(lambda env: f"{env[0]}={env[1]}", env_info.items())
            )
        if baseline_info:
            create_job_info["baseline_info"] = baseline_info
        return create_job_info

    @classmethod
    def update_create_job_info_with_test_stage_step(cls, create_job_info, test_stage_step):
        create_job_info.update({"template_id": test_stage_step.tmpl_id})
        baseline_info = create_job_info.get("baseline_info")
        if baseline_info:
            baseline_id = None
            if test_stage_step.test_type == TestType.FUNCTIONAL:
                baseline_id = baseline_info.get(BaselineType.FUNCTIONAL)
            elif test_stage_step.test_type == TestType.PERFORMANCE:
                baseline_id = baseline_info.get(BaselineType.PERFORMANCE)
            if baseline_id:
                baseline = Baseline.get_by_id(baseline_id)
                if baseline:
                    create_job_info["baseline"] = baseline.name

    @classmethod
    def create_plan_job(cls, plan_inst_id, instance_stage_id):
        pending = running = fail = False
        create_job_info = cls.get_create_job_info_by_plan_inst(plan_inst_id)
        test_steps = Pit.filter(instance_stage_id=instance_stage_id, state=ExecState.PENDING)
        for ts in test_steps:
            cls.update_create_job_info_with_test_stage_step(create_job_info, ts)
            success, job_id, err_msg = CreateJob(**create_job_info).create()
            if success:
                running = True
                ts.job_id = job_id
                if create_job_info.get('build_job_id'):
                    TestJob.update(build_job_id=create_job_info.get('build_job_id')).where(
                        TestJob.id == job_id,
                    ).execute()
            else:
                ts.state = ExecState.FAIL
                ts.state_desc = err_msg
            ts.save()
        cls.update_plan_inst_running(plan_inst_id)
        return pending, running, fail

    @classmethod
    def check_test_running_step(cls, instance_stage_id):
        running = False
        test_steps = Pit.filter(
            Pit.instance_stage_id == instance_stage_id,
            Pit.state.in_([ExecState.PENDING, ExecState.RUNNING])
        )
        for ts in test_steps:
            job_state, output = PlanCheckStep.check_test(ts.job_id)
            if job_state in (ExecState.PENDING_Q, ExecState.PENDING, ExecState.RUNNING):
                running = True
            if job_state == ExecState.RUNNING:
                running = True
                ts.state = job_state
                ts.save()
            if job_state in ExecState.end:
                ts.state = job_state
                ts.result = output
                ts.save()
        return running

    @classmethod
    def clear_plan_server(cls, plan_inst_id):
        PlanServer.release_server_by_plan(plan_inst_id)
        prepare_steps = Pip.filter(plan_instance_id=plan_inst_id)
        for ps in prepare_steps:
            server_sn = ps.server_sn
            PlanServer.remove_plan_server_lock(plan_inst_id, server_sn)

    @classmethod
    def _stop_prepare_stage(cls, plan_inst_id):
        prepare_steps = Pip.filter(
            Pip.plan_instance_id == plan_inst_id,
            Pip.state.not_in(ExecState.end)
        )
        for ps in prepare_steps:
            channel_type = ps.channel_type
            ip, sn, tid = ps.ip, ps.sn, ps.tid
            try:
                if tid:
                    ExecChannel.do_stop(channel_type, ip=ip, sn=sn, tid=tid)
                    plan_complete_logger.info(
                        f"Stop plan prepare step success, plan_inst_id:{plan_inst_id}, "
                        f"prepare_step_id:{ps.id}"
                    )
            except ExecChanelException as error:
                plan_complete_logger.error(
                    f"Stop plan prepare step has error: {error}\n"
                    f"plan_inst_id:{plan_inst_id}, prepare_step_id:{ps.id}"
                )

    @classmethod
    def _stop_test_stage(cls, plan_inst_id):
        plan_test_steps = Pit.filter(
            Pit.plan_instance_id == plan_inst_id,
            Pit.state.not_in(ExecState.end)
        )
        for pt in plan_test_steps:
            job_id = pt.job_id
            if job_id:
                success = TestJob.update(state=ExecState.STOP).where(
                    TestJob.id == job_id,
                    TestJob.state.not_in(ExecState.end)
                ).execute()
                if success:
                    plan_complete_logger.info(
                        f"Stop plan instance job success, "
                        f"plan_inst_id:{plan_inst_id}, job_id:{job_id}"
                    )

    @classmethod
    def stop_plan_stage(cls, plan_inst_id):
        cls._stop_prepare_stage(plan_inst_id)
        cls._stop_test_stage(plan_inst_id)

    @classmethod
    def update_plan_stage_state(cls, plan_inst_id, plan_inst_state):
        Pip.update(state=plan_inst_state).where(
            Pip.plan_instance_id == plan_inst_id,
            Pip.state.not_in(ExecState.end)
        ).execute()
        Pit.update(state=plan_inst_state).where(
            Pit.plan_instance_id == plan_inst_id,
            Pit.state.not_in(ExecState.end)
        ).execute()

    @classmethod
    def update_plan_inst_running(cls, plan_inst_id):
        logger.info(f"Update plan instance running, plan_inst_id:{plan_inst_id}")
        PlanInstance.update(
            state=ExecState.RUNNING,
            start_time=utils.get_now(),
        ).where(
            PlanInstance.id == plan_inst_id,
            PlanInstance.state.in_([ExecState.PENDING_Q, ExecState.PENDING])
        ).execute()
