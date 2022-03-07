from core.server.base import BaseServerPool
from constant import ServerFlowFields
from .db_operation import AliGroupDbServerOperation, AliCloudDbServerOperation
from .pool_common import PoolCommonOperation


class AliGroupRandStdPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        server = AliGroupDbServerOperation.get_rand_server(ws_id, job_id)
        if not server:
            # 尝试从单机标签池中获取使用频率小的机器
            server = AliGroupDbServerOperation.get_rand_server_by_tag(ws_id, job_id)
        if not server:
            # 尝试从未使用的集群中获取
            server = AliGroupDbServerOperation.get_rand_server_by_no_used_cluster(ws_id, job_id)
        # 以下注释代码逻辑主要是保证多个job等待同一台机器的情况下，一旦有机器还按job创建的顺序调度。
        # 但因逻辑较复杂且较难维护，暂时不用。机器ok的情况下任务一定是按创建顺序执行的，多个任务等待
        # 同一机器的情况下，理论上也未必非得按原始顺序，既浪费调度资源，又比较难维护，实际可能影响也
        # 不是很大。
        cls._pull_std_server_in_pool(ws_id, job_id, server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池无可用机器"
        AliGroupDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliGroupRandClusterPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        cluster_server = AliGroupDbServerOperation.get_rand_cluster(ws_id, job_id)
        if not cluster_server:
            # 尝试从集群标签池中获取
            cluster_server = AliGroupDbServerOperation.get_rand_cluster_by_tag(ws_id, job_id)
        cls._pull_cluster_server(job_id, cluster_server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池无可用集群"
        AliGroupDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliCloudRandStdPool(BaseServerPool, PoolCommonOperation):
    """云上机器资源获得相对容易，因此随机场景不再去其它机器池中争夺资源。"""

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        server = AliCloudDbServerOperation.get_rand_server(ws_id, job_id)
        cls._pull_cloud_std_server(job_id, server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池无可用机器"
        AliCloudDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliCloudRandClusterPool(BaseServerPool, PoolCommonOperation):
    """云上机器资源获得相对容易，因此随机场景不再去其它机器池中争夺资源。"""

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        cluster_server = AliCloudDbServerOperation.get_rand_cluster(ws_id, job_id)
        cls._pull_cloud_cluster_server(job_id, cluster_server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池无可用集群"
        AliCloudDbServerOperation.set_job_state_desc(job_id, job_state_desc)
