from core.server.base import BaseServerPool
from constant import ServerFlowFields
from .db_operation import AliGroupDbServerOperation, AliCloudDbServerOperation
from .pool_common import PoolCommonOperation


class AliGroupTagStdPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        tag_id_list = kwargs.get(ServerFlowFields.TAG_ID_LIST, [])
        tag_server = AliGroupDbServerOperation.get_spec_tag_server(job_id, tag_id_list)
        cls._pull_std_server_in_pool(ws_id, job_id, tag_server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池中无指定标签的可用机器，请检查机器及agent状态是否可用"
        AliGroupDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliGroupTagClusterPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        tag_id_list = kwargs.get(ServerFlowFields.TAG_ID_LIST, [])
        tag_cluster = AliGroupDbServerOperation.get_spec_tag_cluster(ws_id, job_id, tag_id_list)
        cls._pull_cluster_server(job_id, tag_cluster, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池中无指定标签的可用集群，请检查机器及agent状态是否可用"
        AliGroupDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliCloudTagStdPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        job_id = default_info[ServerFlowFields.JOB_ID]
        tag_id_list = kwargs.get(ServerFlowFields.TAG_ID_LIST, [])
        tag_server = AliCloudDbServerOperation.get_spec_tag_server(job_id, tag_id_list)
        cls._pull_cloud_std_server(job_id, tag_server, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池中无指定标签的可用机器，请检查机器及agent状态是否可用"
        AliCloudDbServerOperation.set_job_state_desc(job_id, job_state_desc)


class AliCloudTagClusterPool(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_state_desc = None
        job_id = default_info[ServerFlowFields.JOB_ID]
        tag_id_list = kwargs.get(ServerFlowFields.TAG_ID_LIST, [])
        tag_cluster = AliCloudDbServerOperation.get_spec_tag_cluster(job_id, tag_id_list)
        cls._pull_cloud_cluster_server(job_id, tag_cluster, default_info)
        if not default_info.get(ServerFlowFields.READY):
            job_state_desc = "当前机器池中无指定标签的可用集群，请检查机器及agent状态是否可用"
        AliCloudDbServerOperation.set_job_state_desc(job_id, job_state_desc)
