from core.cache.local_inst import LocalInstancePool
from constant import PlanStage, ExecState
from tools.log_util import LoggerFactory
from .plan_executor import PlanExecutor
from .plan_common import PlanCommon


logger = LoggerFactory.scheduler()


class RealRunPlan:

    def __init__(self, context):
        self.context = context
        self.plan_inst_id = context.plan_inst_id
        self.plan_inst = LocalInstancePool.get_plan_inst(self.plan_inst_id)

    def _check_build_info(self, build_job_id):
        state, rpm_list, build_msg = PlanExecutor.check_build_step(build_job_id)
        if state == ExecState.FAIL:
            self.context.update_ctx(
                PlanStage.BUILD_KERNEL, ExecState.FAIL,
                is_end=True, **{self.context.ERROR_MSG: build_msg}
            )
        if state == ExecState.SUCCESS:
            PlanExecutor.update_plan_kernel_info(self.plan_inst, rpm_list)
            self.context.update_ctx(PlanStage.PREPARE_ENV, ExecState.PENDING)

    def build_kernel(self):
        if self.context.build_job_id:
            self._check_build_info(self.context.build_job_id)
        else:
            build_job_id = PlanExecutor.build_kernel_package(self.plan_inst)
            self.context.update_ctx(**{self.context.BUILD_KERNEL_ID: build_job_id})
            self._check_build_info(build_job_id)

    def prepare(self):
        self.prepare_dispatch_task()
        self.prepare_check_state()
        self.check_prepare_end()

    def prepare_dispatch_task(self):
        if self.context.state == ExecState.PENDING or self.context.pending:
            pending, running, fail = PlanExecutor.exec_plan_inst_prepare_script(
                self.plan_inst_id, self.context.instance_stage_id
            )
            run_details = {
                self.context.PENDING: pending,
                self.context.RUNNING: running
            }
            if running or fail:
                inst_stage_id = PlanCommon.get_inst_prepare_stage_id(self.plan_inst_id)
                run_details.update(
                    {
                        self.context.STATE: ExecState.RUNNING,
                        self.context.INSTANCE_STAGE_ID: inst_stage_id
                    }
                )
            self.context.update_ctx(**run_details)

    def prepare_check_state(self):
        if self.context.running:
            running = PlanExecutor.check_prepare_running_step(self.plan_inst_id, self.context.instance_stage_id)
            run_details = {self.context.RUNNING: running}
            self.context.update_ctx(**run_details)

    def check_prepare_end(self):
        success, fail, next_inst_stage_id = PlanExecutor.check_prepare_end(
            self.plan_inst_id, self.context.instance_stage_id)
        if success:
            self.context.update_ctx(
                stage=PlanStage.TEST_STAGE,
                state=ExecState.PENDING,
                instance_stage_id=next_inst_stage_id
            )
        if fail:
            self.context.update_ctx(
                state=ExecState.FAIL,
                is_end=True
            )
        if success or fail:
            PlanExecutor.clear_plan_server(self.plan_inst_id)

    def test(self):
        self.test_dispatch_task()
        self.test_check_state()
        self.check_test_end()

    def test_dispatch_task(self):
        if self.context.state == ExecState.PENDING or self.context.pending:
            inst_stage_id = self.context.instance_stage_id
            pending, running, fail = PlanExecutor.create_plan_job(self.plan_inst_id, inst_stage_id)
            run_details = {
                self.context.PENDING: pending,
                self.context.RUNNING: running
            }
            if running or fail:
                run_details.update(
                    {
                        self.context.STATE: ExecState.RUNNING,
                    }
                )
            self.context.update_ctx(**run_details)

    def test_check_state(self):
        if self.context.running:
            running = PlanExecutor.check_test_running_step(self.context.instance_stage_id)
            run_details = {self.context.RUNNING: running}
            self.context.update_ctx(**run_details)

    def check_test_end(self):
        state = ExecState.RUNNING
        success, fail, next_inst_stage_id = PlanExecutor.check_test_end(
            self.plan_inst_id, self.context.instance_stage_id)
        is_end = not bool(next_inst_stage_id) and (success or fail)
        if is_end and fail:
            state = ExecState.FAIL
        if next_inst_stage_id:
            state = ExecState.PENDING
        if is_end and success:
            state = ExecState.SUCCESS
        self.context.update_ctx(
            stage=PlanStage.TEST_STAGE,
            state=state,
            instance_stage_id=next_inst_stage_id,
            is_end=is_end
        )

    def dispatch_stage(self, stage):
        {
            PlanStage.BUILD_KERNEL: self.build_kernel,
            PlanStage.PREPARE_ENV: self.prepare,
            PlanStage.TEST_STAGE: self.test
        }.get(stage)()

    def run(self):
        logger.info(f"Plan instance real run, context:{self.context.get_ctx()}")
        stage = self.context.stage
        PlanStage.check_stage(stage)
        self.dispatch_stage(stage)
