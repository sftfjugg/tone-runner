import pickle
from scheduler.plan.processing_plan import SerializePlanInstContext
from constant import ProcessDataSource
from .remote_source import BaseRemoteSource


class RemoteProcessPlanInstSource(BaseRemoteSource):

    @classmethod
    def get_process_plan_inst(cls, plan_inst_id):
        if cls.SOURCE_STORE.hexists(ProcessDataSource.PLAN_INST_CTX, plan_inst_id):
            target = cls.SOURCE_STORE.hget(ProcessDataSource.PLAN_INST_CTX, plan_inst_id)
            return pickle.loads(eval(target))
        else:
            process_plan_inst = SerializePlanInstContext(plan_inst_id)
            cls.update_process_plan_inst(plan_inst_id, process_plan_inst)
            return process_plan_inst

    @classmethod
    def update_process_plan_inst(cls, plan_inst_id, process_plan_inst):
        return cls.SOURCE_STORE.hset(
            ProcessDataSource.PLAN_INST_CTX,
            plan_inst_id,
            str(pickle.dumps(process_plan_inst))
        )

    @classmethod
    def remove_process_plan_inst(cls, *plan_inst_id):
        return cls.SOURCE_STORE.hdel(ProcessDataSource.PLAN_INST_CTX, *plan_inst_id)
