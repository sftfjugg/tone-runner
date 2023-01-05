import json
from constant import (
    StepStage, ServerFlowFields,
    Position, ServerProvider,
    JobCfgFields
)
from tools import utils
from .base_step_plugin import BaseStepPlugin


class JobStepBeforeCase(BaseStepPlugin):
    GET_KERNEL_VERSION_CMD = 'uname -r | sed "s#^kernel-##g"'

    def init_cloud(self):
        if self.job.server_provider == ServerProvider.ALI_CLOUD:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.INIT_CLOUD,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def re_clone(self):
        iclone_info = json.loads(self.job.iclone_info)
        if iclone_info and self.job.server_provider == ServerProvider.ALI_GROUP:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.RE_CLONE,
                    JobCfgFields.ICLONE_INFO: iclone_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def build_pkg(self):
        if self.build_pkg_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.BUILD_PKG,
                    JobCfgFields.CREATOR: self.job.creator_username,
                    JobCfgFields.BUILD_PKG_INFO: self.build_pkg_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def initial(self):
        if self.build_pkg_info or self.kernel_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.INITIAL,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def install_rpm_before_reboot(self):
        rpm_list = json.loads(self.job.rpm_info)
        if rpm_list:
            _rpm_list = []
            for rpm_info in rpm_list:
                if rpm_info.get(JobCfgFields.POS) == Position.BEFORE:
                    _rpm_list.append(rpm_info[JobCfgFields.RPM])
            if _rpm_list:
                self.expect_steps.append(
                    {
                        ServerFlowFields.STEP: StepStage.INSTALL_RPM_BEFORE_REBOOT,
                        JobCfgFields.RPM_INFO: _rpm_list,
                        JobCfgFields.ENV_INFO: self.env_info
                    }
                )

    def script_before_reboot(self):
        scripts = json.loads(self.job.script_info)
        if scripts:
            for script in scripts:
                if script.get(JobCfgFields.POS) == Position.BEFORE:
                    self.expect_steps.append(
                        {
                            ServerFlowFields.STEP: StepStage.SCRIPT_BEFORE_REBOOT,
                            JobCfgFields.SCRIPT: script[JobCfgFields.SCRIPT],
                            JobCfgFields.ENV_INFO: self.env_info
                        }
                    )

    def reboot(self):
        if self.job.need_reboot:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.REBOOT,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def _script_before_install_kernel(self):
        if self.kernel_info:
            scripts = self.kernel_info.get(JobCfgFields.SCRIPTS)
            if scripts:
                for script in scripts:
                    if script.get(JobCfgFields.POS) == Position.BEFORE:
                        self.expect_steps.append(
                            {
                                ServerFlowFields.STEP: StepStage.SCRIPT_BEFORE_INSTALL_KERNEL,
                                JobCfgFields.SCRIPT: script[JobCfgFields.SCRIPT],
                                JobCfgFields.ENV_INFO: self.env_info
                            }
                        )

    def _install_kernel(self):
        if self.build_pkg_info or self.kernel_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.INSTALL_KERNEL,
                    JobCfgFields.KERNEL_INFO: self.kernel_info,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def _install_hot_fix(self):
        # 集团内安装已发布内核时才有hot fix阶段
        if self.job.server_provider == ServerProvider.ALI_GROUP:
            kernel_info = json.loads(self.job.kernel_info)
            if kernel_info:
                hot_fix = kernel_info.get(JobCfgFields.HOT_FIX)
                if hot_fix:
                    self.expect_steps.append(
                        {
                            ServerFlowFields.STEP: StepStage.INSTALL_HOT_FIX,
                            JobCfgFields.KERNEL_INFO: kernel_info,
                            JobCfgFields.ENV_INFO: self.env_info
                        }
                    )

    def _script_after_install_kernel(self):
        if self.kernel_info:
            scripts = self.kernel_info.get(JobCfgFields.SCRIPTS)
            if scripts:
                for script in scripts:
                    if script.get(JobCfgFields.POS) == Position.AFTER:
                        self.expect_steps.append(
                            {
                                ServerFlowFields.STEP: StepStage.SCRIPT_AFTER_INSTALL_KERNEL,
                                JobCfgFields.SCRIPT: script[JobCfgFields.SCRIPT],
                                JobCfgFields.ENV_INFO: self.env_info
                            }
                        )

    def _reboot_for_install_kernel(self):
        if self.build_pkg_info or self.kernel_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.REBOOT_FOR_INSTALL_KERNEL,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def _check_kernel_install(self):
        kernel_version = None
        if self.build_pkg_info or self.kernel_info:
            if self.kernel_info:
                kernel = self.kernel_info.get("kernel_packages", [])[:1]
                if kernel:
                    kernel_version = utils.get_kernel_version(kernel[0])
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.CHECK_KERNEL_INSTALL,
                    JobCfgFields.KERNEL_VERSION: kernel_version,
                    JobCfgFields.SCRIPT: self.GET_KERNEL_VERSION_CMD,
                    JobCfgFields.ENV_INFO: self.env_info
                }
            )

    def install_kernel(self):
        self._script_before_install_kernel()
        self._install_kernel()
        self._install_hot_fix()
        self._script_after_install_kernel()
        self._reboot_for_install_kernel()
        self._check_kernel_install()

    def install_rpm_after_reboot(self):
        rpm_list = json.loads(self.job.rpm_info)
        if rpm_list:
            _rpm_list = []
            for rpm_info in rpm_list:
                if rpm_info.get(JobCfgFields.POS) == Position.AFTER:
                    _rpm_list.append(rpm_info[JobCfgFields.RPM])
            if _rpm_list:
                self.expect_steps.append(
                    {
                        ServerFlowFields.STEP: StepStage.INSTALL_RPM_AFTER_REBOOT,
                        JobCfgFields.RPM_INFO: _rpm_list,
                        JobCfgFields.ENV_INFO: self.env_info
                    }
                )

    def script_after_reboot(self):
        scripts = json.loads(self.job.script_info)
        if scripts:
            for script in scripts:
                if script.get(JobCfgFields.POS) == Position.AFTER:
                    self.expect_steps.append(
                        {
                            ServerFlowFields.STEP: StepStage.SCRIPT_AFTER_REBOOT,
                            JobCfgFields.SCRIPT: script[JobCfgFields.SCRIPT],
                            JobCfgFields.ENV_INFO: self.env_info
                        }
                    )

    def prepare(self):
        self.expect_steps.append(
            {
                ServerFlowFields.STEP: StepStage.PREPARE,
                JobCfgFields.ENV_INFO: self.env_info,
            }
        )

    def console(self):
        pass

    def job_monitor(self):
        if self.monitor_info:
            self.expect_steps.append(
                {
                    ServerFlowFields.STEP: StepStage.JOB_MONITOR,
                    JobCfgFields.MONITOR_INFO: self.monitor_info,
                }
            )

    def _build_steps_before_case_in_order(self):
        sorted_steps = (
            self.build_pkg,
            self.init_cloud,
            self.re_clone,
            self.initial,
            self.install_rpm_before_reboot,
            self.script_before_reboot,
            self.reboot,
            self.install_kernel,
            self.install_rpm_after_reboot,
            self.script_after_reboot,
            self.prepare,
            self.console,
            self.job_monitor
        )
        for step in sorted_steps:
            step()

    def get_expect_steps(self):
        self._build_steps_before_case_in_order()
        self._check_steps(self.expect_steps)
        return self.expect_steps
