from constant import ProcessDataSource
from .remote_source import BaseRemoteSource


class ClearProcessPlanInstSource(BaseRemoteSource):

    @classmethod
    def remove_process_plan_inst(cls, *plan_inst_id):
        return cls.SOURCE_STORE.hdel(ProcessDataSource.PLAN_INST_CTX, *plan_inst_id)
