from core.server.base import BaseServerPool
from constant import ServerFlowFields, ServerProvider
from .db_operation import AliGroupDbServerOperation as Ag, AliCloudDbServerOperation as Ac
from .pool_common import PoolCommonOperation


class AliGroupSpecUseInPoolByJob(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        server_id = kwargs.get(ServerFlowFields.SERVER_ID)
        spec_server, server_info = Ag.get_spec_std_server_in_pool(ws_id, job_id, server_id)
        cls._pull_std_server_in_pool(ws_id, job_id, spec_server, default_info, server_info)
        cls._set_spec_server_job_state_desc(job_id, spec_server, server_info)


class AliGroupSpecUseNoInPoolByJob(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_id = default_info[ServerFlowFields.JOB_ID]
        server_snapshot_id = kwargs.get(ServerFlowFields.SERVER_SNAPSHOT_ID)
        custom_server, server_info = Ag.get_spec_std_server_no_in_pool(job_id, server_snapshot_id)
        cls._pull_std_server_no_in_pool(job_id, custom_server, default_info, server_info)
        cls._set_spec_server_job_state_desc(job_id, custom_server, server_info)


class AliGroupSpecUseByCluster(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        cluster_id = kwargs.get(ServerFlowFields.CLUSTER_ID)
        cluster_server, cluster_info = Ag.get_spec_cluster_server(ws_id, job_id, cluster_id)
        cls._pull_cluster_server(job_id, cluster_server, default_info, cluster_info)
        cls._set_spec_cluster_job_state_desc(job_id, cluster_server, cluster_info)


class AliCloudSpecUseByJob(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        ws_id = default_info[ServerFlowFields.WS_ID]
        job_id = default_info[ServerFlowFields.JOB_ID]
        server_id = kwargs.get(ServerFlowFields.SERVER_ID)
        spec_server, server_info = Ac.get_spec_std_server_in_pool(ws_id, job_id, server_id)
        cls._pull_cloud_std_server(job_id, spec_server, default_info, server_info)
        cls._set_spec_server_job_state_desc(job_id, spec_server, server_info)


class AliCloudSpecUseNoInPoolByJob(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_id = default_info[ServerFlowFields.JOB_ID]
        server_snapshot_id = kwargs.get(ServerFlowFields.SERVER_SNAPSHOT_ID)
        custom_server, server_info = Ac.get_spec_std_server_no_in_pool(
            job_id,
            server_snapshot_id,
            ServerProvider.ALI_CLOUD
        )
        cls._pull_std_cloud_server_no_in_pool(job_id, custom_server, default_info, server_info)
        cls._set_spec_server_job_state_desc(job_id, custom_server, server_info)


class AliCloudSpecUseByCluster(BaseServerPool, PoolCommonOperation):

    @classmethod
    def pull(cls, default_info, **kwargs):
        job_id = default_info[ServerFlowFields.JOB_ID]
        cluster_id = kwargs.get(ServerFlowFields.CLUSTER_ID)
        cluster_server, cluster_info = Ac.get_spec_cluster_server(job_id, cluster_id)
        cls._pull_cloud_cluster_server(job_id, cluster_server, default_info, cluster_info)
        cls._set_spec_cluster_job_state_desc(job_id, cluster_server, cluster_info)
