from constant import ExecState, DbField
from models.plan import PlanInstance
from models.job import TestJob, TestJobSuite, TestJobCase, TestStep
from models.dag import Dag, DagStepInstance
from tools import utils


def db_running_plan_inst_set():
    return {
        plan_inst.id for plan_inst in
        PlanInstance.select(PlanInstance.id).filter(
            PlanInstance.state.in_([ExecState.PENDING_Q, ExecState.RUNNING])
        )
    }


def check_plan_inst_end(plan_inst_id):
    return PlanInstance.filter(
        PlanInstance.id == plan_inst_id,
        (PlanInstance.is_deleted == DbField.TRUE) |
        (PlanInstance.state.in_(ExecState.end))
    ).exists() or not PlanInstance.filter(
        PlanInstance.id == plan_inst_id,
        PlanInstance.is_deleted.in_([DbField.TRUE, DbField.FALSE])
    ).exists()


def db_running_job_set():
    all_running_job_set = {
        job.id for job in
        TestJob.select(TestJob.id).filter(
            TestJob.state.in_((ExecState.PENDING_Q, ExecState.RUNNING))
        )
    }
    has_dag_running_job_set = {
        job.id for job in
        TestJob.select(TestJob.id).filter(
            TestJob.state.in_((ExecState.PENDING_Q, ExecState.RUNNING)),
            TestJob.is_deleted.in_((DbField.TRUE, DbField.FALSE))
        ).join(
            Dag, on=(TestJob.id == Dag.job_id)
        ).where(
            Dag.is_deleted == DbField.FALSE
        )
    }
    return all_running_job_set - has_dag_running_job_set


def db_running_dag_set():
    return {
        dag.id for dag in
        Dag.select(Dag.id).join(
            TestJob,
            on=(Dag.job_id == TestJob.id)
        ).where(
            TestJob.state.in_((ExecState.PENDING_Q, ExecState.RUNNING)),
            TestJob.is_deleted.in_((DbField.TRUE, DbField.FALSE))
        )
    }


def db_running_dag_node_set():
    return {
        dag_node.id for dag_node in
        DagStepInstance.filter(state=ExecState.RUNNING)
    }


def exists_job(job_id):
    return TestJob.filter(
        TestJob.id == job_id,
        TestJob.is_deleted.in_([DbField.TRUE, DbField.FALSE])
    ).exists()


def _check_job_end(job_id):
    return TestJob.filter(
        TestJob.id == job_id,
        (TestJob.is_deleted == DbField.TRUE) |
        (TestJob.state.in_(ExecState.end))
    ).exists()


def check_job_end(job_id=None, dag_id=None):
    if dag_id:
        dag = Dag.get_or_none(id=dag_id)
        if not dag:
            return True
        else:
            job_id = Dag.get_by_id(dag_id).job_id
    if not exists_job(job_id):
        check_res = True
    elif _check_job_end(job_id):
        check_res = True
    else:
        check_res = False
    return check_res


def check_dag_node_end(dag_node_id):
    dag_step = DagStepInstance.get_or_none(id=dag_node_id)
    if dag_step and dag_step.state in ExecState.end:
        return True
    return False


def set_dag_node_state_by_job(dag_id, state, err_msg):
    DagStepInstance.update(state=state, remark=err_msg).where(
        DagStepInstance.dag_id == dag_id,
        DagStepInstance.state.not_in(ExecState.end)
    ).execute()


def set_test_step_state_by_job(job_id, state):
    TestStep.update(state=state).where(
        TestStep.job_id == job_id,
        TestStep.state.not_in(ExecState.end)
    ).execute()


def set_one_dag_node_state(dag_step_id, state, err_msg):
    DagStepInstance.update(state=state, remark=err_msg).where(
        DagStepInstance.id == dag_step_id,
        DagStepInstance.state.not_in(ExecState.end)
    ).execute()


def get_dag_node_ids(dag_id):
    return {
        node.id for node in
        DagStepInstance.select(
            DagStepInstance.id
        ).filter(
            DagStepInstance.dag_id == dag_id,
        )
    }


def get_dag_node_ids_with_no_end(dag_id):
    return {
        node.id for node in
        DagStepInstance.select(
            DagStepInstance.id
        ).filter(
            DagStepInstance.dag_id == dag_id,
            DagStepInstance.state.not_in(ExecState.end)
        )
    }


def get_job_state_by_all_suite(job_id):
    job_suite_states = {
        job_suite.state for job_suite in
        TestJobSuite.select(TestJobSuite.state).filter(job_id=job_id)
    }
    if ExecState.SUCCESS in job_suite_states and ExecState.FAIL not in job_suite_states:
        state = ExecState.SUCCESS
    elif ExecState.FAIL in job_suite_states:
        state = ExecState.FAIL
    elif not (job_suite_states - {ExecState.STOP}):
        state = ExecState.STOP
    else:
        state = ExecState.SKIP
    return state


def set_job_suite_state_by_job(job_id, job_state):
    TestJobSuite.update(state=job_state, end_time=utils.get_now()).where(
        TestJobSuite.job_id == job_id,
        TestJobSuite.state.not_in(ExecState.end)
    ).execute()


def set_job_case_state_by_job(job_id, job_state):
    TestJobCase.update(state=job_state, end_time=utils.get_now()).where(
        TestJobCase.job_id == job_id,
        TestJobCase.state.not_in(ExecState.end)
    ).execute()


def set_job_case_state_by_suite(job_id, test_suite_id, state):
    """根据job_suite的终态来设置job_case的终态"""
    TestJobCase.update(state=state, end_time=utils.get_now()).where(
        TestJobCase.job_id == job_id,
        TestJobCase.test_suite_id == test_suite_id,
        TestJobCase.state.not_in(ExecState.end)
    ).execute()


def set_dag_step_state(step_id, state):
    DagStepInstance.update(state=state).where(
        DagStepInstance.id == step_id
    ).execute()


def set_dag_step_state_by_job_suite(dag_id, job_suite_id, state, job_suite_flag):
    DagStepInstance.update(state=state).where(
        DagStepInstance.dag_id == dag_id,
        DagStepInstance.state.not_in(ExecState.end),
        DagStepInstance.step_data.contains(
            f"\"{job_suite_flag}\": {job_suite_id}"
        )
    ).execute()


def set_job_suite_state_by_dag_step(step_id, state):
    dag_step = DagStepInstance.get_by_id(step_id)
    job_suite_id = dag_step.job_suite_id
    TestJobSuite.update(state=state, end_time=utils.get_now()).where(
        TestJobSuite.id == job_suite_id,
        TestJobSuite.state.not_in(ExecState.end)
    ).execute()


def set_job_case_state_by_id(job_case_id, state):
    TestJobCase.update(state=state, end_time=utils.get_now()).where(
        TestJobCase.id == job_case_id,
        TestJobCase.state.not_in(ExecState.end)
    ).execute()


def get_state_of_dag_steps(step_id_set):
    return {
        ds.state for ds in
        DagStepInstance.filter(
            DagStepInstance.id.in_(step_id_set)
        )
    }
