import time
import config
from core.dag.engine.update_dag import JobUpdateDag
from core.dag.engine.processing_dag import ProcessingDag
from core.dag.engine.processing_node import ProcessingNode
from constant import ProcessDataSource
from tools.local_cache import local_cache
from models.dag import Dag
from models.plan import PlanInstance


class LocalInstancePool:
    SOURCE_STORE = local_cache

    @classmethod
    def get_plan_inst(cls, plan_inst_id):
        cache_key = f"{ProcessDataSource.PLAN_INST}_{plan_inst_id}"
        cache_value = cls.SOURCE_STORE.get(cache_key)
        if cache_value:
            plan_inst = cache_value.get(ProcessDataSource.PLAN_INST)
        else:
            plan_inst = PlanInstance.get_by_id(plan_inst_id)
            cls.SOURCE_STORE.set(
                cache_key,
                {
                    ProcessDataSource.PLAN_INST: plan_inst_id,
                    "expire": int(time.time()) + config.PLAN_CACHE_INST_EXPIRE
                }
            )
        return plan_inst

    @classmethod
    def get_process_dag_inst(cls, dag_id):
        process_dag_cache_key = f"{ProcessDataSource.DAG_INST}_{dag_id}"
        process_dag_cache = cls.SOURCE_STORE.get(process_dag_cache_key)
        if process_dag_cache:
            process_dag_inst = process_dag_cache.get(ProcessDataSource.DAG_INST)
        else:
            db_dag_inst = Dag.get_by_id(dag_id)
            if db_dag_inst.is_update:
                update_dag_cache_key = f"{ProcessDataSource.UPDATE_DAG_INST}_{dag_id}"
                update_dag_cache = cls.SOURCE_STORE.get(update_dag_cache_key)
                if update_dag_cache:
                    update_dag_inst = update_dag_cache.get(ProcessDataSource.UPDATE_DAG_INST)
                else:
                    update_dag_inst = JobUpdateDag(db_dag_inst.job_id)
                    cls.SOURCE_STORE.set(
                        update_dag_cache_key,
                        {
                            ProcessDataSource.UPDATE_DAG_INST: update_dag_inst,
                            "expire": int(time.time()) + config.CACHE_INST_EXPIRE
                        }
                    )
                process_dag_inst = ProcessingDag(dag_id, update_dag_inst)
            else:
                process_dag_inst = ProcessingDag(dag_id)
            cls.SOURCE_STORE.set(
                process_dag_cache_key,
                {
                    ProcessDataSource.DAG_INST: process_dag_inst,
                    "expire": int(time.time()) + config.CACHE_INST_EXPIRE
                }
            )
        return process_dag_inst

    @classmethod
    def get_process_dag_node_inst(cls, dag_node_id):
        cache_key = f"{ProcessDataSource.DAG_NODE_INST}_{dag_node_id}"
        cache_value = cls.SOURCE_STORE.get(cache_key)
        if cache_value:
            process_dag_node_inst = cache_value.get(ProcessDataSource.DAG_NODE_INST)
        else:
            process_dag_node_inst = ProcessingNode(dag_node_id)
            cls.SOURCE_STORE.set(
                cache_key,
                {
                    ProcessDataSource.DAG_NODE_INST: process_dag_node_inst,
                    "expire": int(time.time()) + config.CACHE_INST_EXPIRE
                }
            )
        return process_dag_node_inst
