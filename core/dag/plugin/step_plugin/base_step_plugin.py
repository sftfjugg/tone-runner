import json
from constant import ServerFlowFields, StepStage
from tools import utils


class BaseStepPlugin:

    def __init__(self, job):
        self.job = job
        self.expect_steps = []
        self.env_info = utils.safe_json_loads(job.env_info, dict())
        self.build_pkg_info = json.loads(self.job.build_pkg_info)
        self.kernel_info = json.loads(self.job.kernel_info)
        self.monitor_info = json.loads(self.job.monitor_info)

    def _check_steps(self, steps):
        steps = [step_info[ServerFlowFields.STEP] for step_info in steps]
        assert StepStage.check_steps(steps) is True, \
            f"Job<{self.job.id}> have some wrong steps: {StepStage.wrong_steps(steps)}"
