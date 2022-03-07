from core.exception import PlanEndException, PlanNotExistsException
from constant import (
    ExecState,
    PlanStage,
    DbField,
)
from models.plan import (
    PlanInstance,
    PlanInstanceStageRelation as Pis,
    PlanInstancePrepareRelation as Pip,
    PlanInstanceTestRelation as Pit
)
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


class PlanCommon:

    @staticmethod
    def check_plan_is_end(plan_inst_id):
        if not PlanInstance.filter(
            PlanInstance.id == plan_inst_id,
            PlanInstance.is_deleted.in_([DbField.TRUE, DbField.FALSE])
        ).exists():
            raise PlanNotExistsException(f"Plan instance is not exists! plan_inst_id:{plan_inst_id}.")
        if PlanInstance.filter(
                PlanInstance.id == plan_inst_id,
                (PlanInstance.is_deleted == DbField.TRUE) |
                (PlanInstance.state.in_(ExecState.end))
        ).first():
            raise PlanEndException(f"Plan instance is end! plan_inst_id:{plan_inst_id}.")

    @staticmethod
    def get_plan_stage_list(plan_inst_id, stage_type):
        return Pis.filter(
            plan_instance_id=plan_inst_id,
            stage_type=stage_type
        ).order_by(
            Pis.stage_index.asc()
        ).order_by(
            Pis.gmt_created.asc()
        )

    @staticmethod
    def get_inst_prepare_stage_id(plan_inst_id):
        prepare_stage = Pis.filter(
            Pis.plan_instance_id == plan_inst_id,
            Pis.stage_type == PlanStage.PREPARE_ENV,
        ).first()
        if prepare_stage:
            return prepare_stage.id
        return 0

    @staticmethod
    def get_next_inst_test_stage_id(plan_inst_id, cur_idx=0):
        next_stage = Pis.filter(
            Pis.plan_instance_id == plan_inst_id,
            Pis.stage_type == PlanStage.TEST_STAGE,
            Pis.stage_index > cur_idx
        ).order_by(
            Pis.stage_index.asc()
        ).order_by(
            Pis.gmt_created.asc()
        ).first()
        if next_stage:
            return next_stage.id
        return 0

    @classmethod
    def check_stage_state(cls, check_stage, stage_id):
        if check_stage == PlanStage.PREPARE_ENV:
            stage_model = Pip
        else:
            stage_model = Pit
        stage_elements = cls.stage_elements(stage_model, stage_id)
        stage_elements_state_set = {se.state for se in stage_elements}
        if ExecState.RUNNING in stage_elements_state_set:
            run_details = cls.stage_run_details(stage_elements)
            return ExecState.RUNNING, run_details
        elif ExecState.PENDING in stage_elements_state_set:
            run_details = cls.stage_run_details(stage_elements)
            return ExecState.PENDING, run_details
        elif ExecState.FAIL in stage_elements_state_set:
            return ExecState.FAIL, {}
        else:
            return ExecState.SUCCESS, {}

    @classmethod
    def stage_elements(cls, stage_model, stage_id):
        return stage_model.filter(
            stage_model.instance_stage_id == stage_id,
        )

    @classmethod
    def stage_run_details(cls, stage_elements):
        return {
            ExecState.PENDING: stage_elements.filter(state=ExecState.PENDING).exists(),
            ExecState.RUNNING: stage_elements.filter(state=ExecState.RUNNING).exists()
        }

    @classmethod
    def get_plan_inst_finally_state(cls, plan_inst_id):
        all_state = set()
        prepare_env_stage_list = Pis.filter(plan_instance_id=plan_inst_id, stage_type=PlanStage.PREPARE_ENV)
        if prepare_env_stage_list:
            prepare_env_stage_id = prepare_env_stage_list[0].id
            state, _ = cls.check_stage_state(PlanStage.PREPARE_ENV, prepare_env_stage_id)
            all_state.add(state)
        test_stage_list = Pis.filter(plan_instance_id=plan_inst_id, stage_type=PlanStage.TEST_STAGE)
        test_stage_id_list = [ts.id for ts in test_stage_list]
        for test_stage_id in test_stage_id_list:
            state, _ = cls.check_stage_state(PlanStage.TEST_STAGE, test_stage_id)
            all_state.add(state)
        if ExecState.FAIL in all_state:
            return ExecState.FAIL
        elif all_state == {ExecState.STOP}:
            return ExecState.STOP
        elif all_state == {ExecState.SKIP}:
            return ExecState.SKIP
        return ExecState.SUCCESS
