class GenerateDagException(Exception):
    pass


class UpdateDagException(Exception):
    pass


class BuildDagException(Exception):
    pass


class DagExistsException(Exception):
    pass


class ProcessDagException(Exception):
    pass


class ProcessDagNodeException(Exception):
    pass


class AllocServerException(Exception):
    pass


class ExecChanelException(Exception):
    pass


class ExecStepException(Exception):
    pass


class CheckStepException(Exception):
    pass


class ICloneServerException(Exception):
    pass


class AsyncCallException(Exception):
    pass


class JobEndException(Exception):
    pass


class JobNotExistsException(Exception):
    pass


class DagNodeEndException(Exception):
    pass


class AgentRequestException(Exception):
    pass


class PlanRunException(Exception):
    pass


class PlanEndException(Exception):
    pass


class PlanNotExistsException(Exception):
    pass


class CreateJobArgsException(Exception):
    pass
