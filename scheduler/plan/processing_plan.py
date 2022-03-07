import json
from core.exception import PlanEndException, PlanRunException, PlanNotExistsException
from constant import ExecState, PlanStage, JobCfgFields
from models.plan import PlanInstance
from tools.log_util import LoggerFactory
from .real_run_plan import RealRunPlan
from .plan_complete import PlanInstComplete
from .plan_common import PlanCommon


logger = LoggerFactory.scheduler()


class ProcessingPlanInst:

    def __init__(self, context):
        self.context = context
        self.plan_inst_id = self.context.plan_inst_id
        self.check_stage = PlanStage.BUILD_KERNEL

    def check_build_kernel(self):
        if self.check_stage == PlanStage.BUILD_KERNEL:
            is_end = False
            plan_inst = PlanInstance.get_by_id(self.plan_inst_id)
            build_state = plan_inst.build_state
            build_info = json.loads(plan_inst.build_pkg_info)
            if JobCfgFields.KERNEL in plan_inst.kernel_info:
                self.check_stage = PlanStage.PREPARE_ENV
            else:
                if not build_info:
                    self.check_stage = PlanStage.PREPARE_ENV
                    return
                if build_state == ExecState.FAIL:
                    is_end = True
                self.context.update_ctx(
                    PlanStage.BUILD_KERNEL,
                    build_state or ExecState.PENDING,
                    is_end=is_end,
                    **{self.context.BUILD_KERNEL_ID: plan_inst.build_job_id}
                )

    def check_prepare(self):
        if self.check_stage == PlanStage.PREPARE_ENV:
            is_end = False
            prepare_stage_id = PlanCommon.get_inst_prepare_stage_id(plan_inst_id=self.plan_inst_id)
            if prepare_stage_id:
                state, run_details = PlanCommon.check_stage_state(
                    PlanStage.PREPARE_ENV,
                    prepare_stage_id
                )
                if state == ExecState.SUCCESS:
                    self.check_stage = PlanStage.TEST_STAGE
                    return
                if state == ExecState.FAIL:
                    is_end = True
                self.context.update_ctx(
                    stage=PlanStage.PREPARE_ENV,
                    state=state,
                    instance_stage_id=prepare_stage_id,
                    is_end=is_end,
                    **run_details
                )
            else:
                self.check_stage = PlanStage.TEST_STAGE

    def check_test(self):
        if self.check_stage == PlanStage.TEST_STAGE:
            test_stage_list = PlanCommon.get_plan_stage_list(self.plan_inst_id, PlanStage.TEST_STAGE)
            test_stage_id, all_success = None, []
            for ts in test_stage_list:
                test_stage_id = ts.id
                state, run_details = PlanCommon.check_stage_state(PlanStage.TEST_STAGE, test_stage_id)
                if state not in ExecState.end:
                    self.context.update_ctx(PlanStage.TEST_STAGE, state, test_stage_id, **run_details)
                    return
                if state == ExecState.FAIL and ts.impact_next:
                    self.context.update_ctx(
                        PlanStage.TEST_STAGE,
                        ExecState.FAIL,
                        test_stage_id,
                        is_end=True,
                        **run_details
                    )
                    return
                all_success.append(state)
            if all_success:
                self.context.update_ctx(
                    PlanStage.TEST_STAGE,
                    ExecState.SUCCESS,
                    test_stage_id,
                    is_end=True
                )

    def init_plan_run_stage(self):
        init_stage_order = (self.check_build_kernel, self.check_prepare, self.check_test)
        for init_stage in init_stage_order:
            init_stage()

    def where_to_run(self):
        if self.context.is_empty():
            self.init_plan_run_stage()

    def check_complete(self):
        is_end = self.context.is_end
        state = self.context.state
        err_msg = self.context.error_msg
        if is_end:
            if state == ExecState.FAIL:
                finally_state = state
            else:
                finally_state = PlanCommon.get_plan_inst_finally_state(self.plan_inst_id)
            return PlanInstComplete(self.plan_inst_id, finally_state, err_msg).process()
        return False

    def run(self):
        PlanCommon.check_plan_is_end(self.plan_inst_id)
        self.where_to_run()
        check_complete = self.check_complete()
        if not check_complete:
            RealRunPlan(self.context).run()
            check_complete = self.check_complete()
        return check_complete


class ProcessingPlanInstContext:
    STAGE = "stage"
    STATE = "state"
    IS_END = "is_end"
    ERROR_MSG = "error_msg"
    BUILD_KERNEL_ID = "build_kernel_id"
    INSTANCE_STAGE_ID = "instance_stage_id"
    PENDING = ExecState.PENDING
    RUNNING = ExecState.RUNNING

    _FIELDS = (
        STAGE,
        STATE,
        INSTANCE_STAGE_ID,
        IS_END,
        ERROR_MSG,
        BUILD_KERNEL_ID,
        PENDING,
        RUNNING,
    )

    def __init__(self, plan_inst_id):
        self.plan_inst_id = plan_inst_id
        self.__last_runner = {}

    @property
    def stage(self):
        return self.__last_runner.get(self.STAGE)

    @stage.setter
    def stage(self, stage):
        self.__last_runner[self.STAGE] = stage

    @property
    def instance_stage_id(self):
        return self.__last_runner.get(self.INSTANCE_STAGE_ID)

    @instance_stage_id.setter
    def instance_stage_id(self, instance_stage_id):
        self.__last_runner[self.INSTANCE_STAGE_ID] = instance_stage_id

    @property
    def state(self):
        return self.__last_runner.get(self.STATE)

    @state.setter
    def state(self, state):
        self.__last_runner[self.STATE] = state

    @property
    def is_end(self):
        return self.__last_runner.get(self.IS_END)

    @is_end.setter
    def is_end(self, is_end):
        self.__last_runner[self.IS_END] = is_end

    @property
    def error_msg(self):
        return self.__last_runner.get(self.ERROR_MSG)

    @property
    def build_job_id(self):
        return self.__last_runner.get(self.BUILD_KERNEL_ID)

    @property
    def pending(self):
        return self.__last_runner.get(self.PENDING)

    @property
    def running(self):
        return self.__last_runner.get(self.RUNNING)

    def is_empty(self):
        return not bool(self.__last_runner)

    def get_ctx(self):
        return self.__last_runner

    def update_ctx(self, stage=None, state=None, instance_stage_id=None, is_end=None, **extends):
        if stage:
            self.stage = stage
        if state:
            self.state = state
        if instance_stage_id:
            self.instance_stage_id = instance_stage_id
        if is_end:
            self.is_end = is_end
        for k, v in extends.items():
            assert k in self._FIELDS, f"{k} must be in {self._FIELDS}!"
            self.__last_runner[k] = v

    def run(self):
        try:
            return ProcessingPlanInst(self).run()
        except PlanEndException as error:
            logger.error(error)
            raise PlanEndException(error)
        except PlanNotExistsException as error:
            logger.error(error)
            raise PlanNotExistsException(error)
        except Exception as error:
            logger.exception(error)
            raise PlanRunException(error)


class SerializePlanInstContext(ProcessingPlanInstContext):
    pass
