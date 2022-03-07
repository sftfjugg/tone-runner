from typing import List, Dict
from core.exception import GenerateDagException
from constant import SuiteCaseFlag, ServerFlowFields, StepStage, DbField, JobCfgFields
from .base_step_plugin import BaseStepPlugin


CaseStepList = List[Dict]


class JobStepInCase(BaseStepPlugin):

    def __init__(self, job, job_suite, job_case, suite_case_flag):
        super(JobStepInCase, self).__init__(job)
        self.job_suite = job_suite
        self.job_case = job_case
        self.job_suite_id = job_suite.id
        self.job_case_id = job_case.id
        self.case_flag_is_first = suite_case_flag[0]
        self.case_flag_is_last = suite_case_flag[1]

    def reboot_before_suite(self):
        if (self.case_flag_is_first == SuiteCaseFlag.FIRST and
                self.job_suite.need_reboot == DbField.TRUE):
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.REBOOT_BEFORE_SUITE,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def suite_monitor(self):
        pass

    def script_before_suite(self):
        script_info = self.job_suite.setup_info
        if (self.case_flag_is_first == SuiteCaseFlag.FIRST and
                script_info):
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.SCRIPT_BEFORE_SUITE,
                    JobCfgFields.SCRIPT: script_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def reboot_before_case(self):
        if (
                (self.case_flag_is_first == SuiteCaseFlag.FIRST and
                 self.job_suite.need_reboot == DbField.TRUE) or
                (self.case_flag_is_first != SuiteCaseFlag.FIRST
                 and self.job_case.need_reboot == DbField.TRUE)
        ):
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.REBOOT_BEFORE_CASE,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def case_monitor(self):
        pass

    def script_before_case(self):
        script_info = self.job_case.setup_info
        if script_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.SCRIPT_BEFORE_CASE,
                    JobCfgFields.SCRIPT: script_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def run_case(self):
        self.expect_steps.append(
            {
                ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                ServerFlowFields.STEP: StepStage.RUN_CASE,
                ServerFlowFields.BASELINE_ID: self.job.baseline_id,
                JobCfgFields.ENV_INFO: self.env_info
            }
        )

    def script_after_case(self):
        script_info = self.job_case.cleanup_info
        if script_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.SCRIPT_AFTER_CASE,
                    JobCfgFields.SCRIPT: script_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def script_after_suite(self):
        script_info = self.job_suite.cleanup_info
        if (self.case_flag_is_last == SuiteCaseFlag.LAST and
                script_info):
            self.expect_steps.append(
                {
                    ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                    ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                    ServerFlowFields.STEP: StepStage.SCRIPT_AFTER_SUITE,
                    JobCfgFields.SCRIPT: script_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def build_case_detail_step_in_order(self):
        sorted_steps = (
            self.reboot_before_suite,
            self.suite_monitor,
            self.script_before_suite,
            self.reboot_before_case,
            self.case_monitor,
            self.script_before_case,
            self.run_case,
            self.script_after_case,
            self.script_after_suite
        )
        for step in sorted_steps:
            step()

    def get_case_step(self) -> CaseStepList:
        self.build_case_detail_step_in_order()
        if not self.expect_steps:
            raise GenerateDagException("build dag has error, case step is empty!")
        self._check_steps(self.expect_steps)
        return self.expect_steps
