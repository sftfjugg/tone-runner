from core.exception import CheckStepException
from core.exec_channel import ExecChannel
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from core.dag.plugin import db_operation
from core.dag.plugin.job_complete import JobComplete
from models.workspace import Product, Project
from scheduler.job.check_job_step import CheckJobStep
from constant import TidTypeFlag, ExecState, StepStage, ServerFlowFields, get_agent_res_obj, ServerProvider
from models.job import TestStep, TestJob
from models.dag import DagStepInstance
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


class ProcessDagNodePlugin:
    EXEC_CMD_WITH_CASE_DONE = {
        "gcc": "gcc --version",
        "arch": "uname -p",
        "rpm_list": "rpm -q glibc",
        "distro": "cat /etc/os-release | grep -i id=",
        "kernel_version": "uname -r",
        "glibc": "ldd --version",
        "memory_info": "cat /proc/meminfo",
        "disk": "df -h",
        "cpu_info": "cat /proc/cpuinfo",
        "ether": "ifconfig"
    }

    @classmethod
    def check_step(cls, dag_step_id):
        test_step = TestStep.get_or_none(dag_step_id=dag_step_id)

        if not test_step:
            raise CheckStepException(f"Test step not exists, dag_step_id:{dag_step_id}")

        if test_step and test_step.state not in ExecState.end:
            tid = test_step.tid
            if tid.startswith(TidTypeFlag.AGENT_TID):
                CheckJobStep.check_step_with_ali_group(test_step, dag_step_id, tid)
            elif tid.startswith(TidTypeFlag.I_CLONE_TID):
                CheckJobStep.check_step_with_i_clone(test_step, dag_step_id, tid)
            elif tid.startswith(TidTypeFlag.BUILD_PKG_TID):
                CheckJobStep.check_step_with_build_package(test_step, dag_step_id, tid)
            elif tid.startswith(TidTypeFlag.BUSINESS_TEST_TID):
                CheckJobStep.check_step_with_business_test(test_step, dag_step_id, tid)
            else:
                raise CheckStepException(f"Tid is not support!tid:{tid}")

        cls.update_dag_step(dag_step_id)

    @classmethod
    def update_dag_step(cls, dag_step_id):
        test_step = TestStep.get_or_none(dag_step_id=dag_step_id)
        stage, state = test_step.stage, test_step.state
        JobComplete.set_job_suite_or_case_state_by_test_step(test_step, stage, state)
        dag_step = DagStepInstance.get_by_id(dag_step_id)

        if stage == StepStage.RUN_CASE and state in ExecState.end:
            # 为了解决一个job中多个suite使用同一个server或cluster且存入一条线上时
            # 执行链跨suite时，不能按每个suite结束来更新job_suite状态的问题。
            JobComplete.set_job_suite_state_in_real_time(dag_step.job_suite_id)
            cls.update_server_info_to_snapshot(dag_step)
        elif stage in StepStage.JOB_END_POINT_WITH_SUITE and state == ExecState.FAIL:
            # suite执行前的script或reboot失败时将该suite下的所有case步骤状态、
            # 相应test_job_case状态以及相应的dag_step状态置为fail
            cls._set_node_state_by_job_suite(dag_step_id, ExecState.FAIL)
        elif stage in StepStage.ONE_CASE_END_POINT and state == ExecState.FAIL:
            # case执行前的script或reboot失败时将该case步骤的状态以及相应dag_step的状态设置为fail
            db_operation.set_job_case_state_by_id(dag_step.job_case_id, state)

        dag_step.state = state
        dag_step.save()
        logger.info(
            f"Update dag step state, node:{dag_step.id}  stage:"
            f"{stage}, state:{state}, run_strategy:{dag_step.run_strategy}"
        )
        JobComplete.check_node_stop_or_skip_by_user(dag_step_id)

    @classmethod
    def _set_node_state_by_job_suite(cls, node, state):
        dag_step = DagStepInstance.get_by_id(node)
        job_id, dag_id, job_suite_id = dag_step.job_id, dag_step.dag_id, dag_step.job_suite_id
        db_operation.set_dag_step_state_by_job_suite(dag_id, job_suite_id, state, ServerFlowFields.JOB_SUITE_ID)
        db_operation.set_job_case_state_by_suite(job_id, job_suite_id, state)

    @classmethod
    def _get_dist_field(cls, result, index):
        res = ""
        if "\n" in result:
            line_info = result.split("\n")[index]
            if "=" in line_info:
                res = line_info.split("=")[1].replace("\"", "")
        return res

    @classmethod
    def _replenish_cmd_info(cls, dag_step):
        job = TestJob.get(dag_step.job_id)
        product = Product.get(id=job.product_id)
        product_version_command = product.command if product.command else 'uname -r'
        cls.EXEC_CMD_WITH_CASE_DONE.update({"product_version": product_version_command})

    @classmethod
    def _get_project_default_version(cls, dag_step):
        job = TestJob.get(dag_step.job_id)
        project = Project.get(id=job.project_id)
        return project.product_version

    @classmethod
    def update_server_info_to_snapshot(cls, dag_step):
        logger.info(f"Update server info to snapshot, job_case_id:{dag_step.job_case_id}")
        snapshot_update_fields = {}
        server_ip = dag_step.server_ip
        server_sn = dag_step.server_sn
        channel_type = dag_step.channel_type
        server_provider = dag_step.server_provider
        snapshot_server_id = dag_step.snapshot_server_id
        server_id = dag_step.server_id
        server_tsn = Cs.get_server_tsn(server_id, server_provider)
        agent_res = get_agent_res_obj(channel_type)
        cls._replenish_cmd_info(dag_step)
        default_product_version = cls._get_project_default_version(dag_step)
        type_command_list = list(cls.EXEC_CMD_WITH_CASE_DONE.items())
        retry_times = 0
        for key, command in type_command_list:
            try:
                cls._exec_server_info_cmd(
                    channel_type, server_ip, server_sn, command, agent_res,
                    type_command_list, retry_times, key, default_product_version,
                    snapshot_update_fields, server_tsn
                )
            except Exception as error:
                logger.error(
                    f"Update server info to snapshot error! "
                    f"job_case_id:{dag_step.job_case_id}, error: {error}")
        Cs.update_snapshot_server(
            Cs.get_snapshot_server_by_provider(snapshot_server_id, server_provider),
            **snapshot_update_fields
        )

    @classmethod
    def _exec_server_info_cmd(cls, channel_type, server_ip, server_sn, command, agent_res,
                              type_command_list, retry_times, key, default_product_version,
                              snapshot_update_fields, server_tsn=None):
        success, result = ExecChannel.do_exec(
            channel_type,
            ip=server_ip,
            sn=server_sn,
            tsn=server_tsn,
            command=command,
            sync=True
        )
        if success:
            result = result[agent_res.JOB_RESULT].strip()
            if key == "distro":
                os_name = cls._get_dist_field(result, 0)
                version = cls._get_dist_field(result, 1)
                result = os_name + version
                if not result and retry_times < 2:
                    type_command_list.append(("distro", "cat /etc/os-release | grep -i id="))
                    retry_times += 1
            if key == 'product_version' and not result:
                result = default_product_version
            snapshot_update_fields[key] = result
        else:
            logger.error(f"Exec command fail, command:{command}")
            if key == 'product_version':
                snapshot_update_fields['product_version'] = default_product_version
        return snapshot_update_fields
