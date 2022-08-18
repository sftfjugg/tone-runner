from lib.peewee import fn
from core.cache.remote_source import RemoteAllocServerSource
from constant import (
    ServerState,
    ServerProvider,
    RunMode,
    DbField,
    SpecUseType,
    ClusterRole,
)
from models.job import TestJob
from models.server import (
    TestServer,
    TestServerSnapshot,
    CloudServerSnapshot,
    CloudServer,
    TestCluster,
    TestClusterSnapshot,
    TestClusterServer,
    TestClusterServerSnapshot,
    ServerTagRelation,
    update_server_state_log,
)
from tools.log_util import LoggerFactory
from tools import utils

logger = LoggerFactory.alloc_server()


class CommonDbServerOperation:

    @classmethod
    def create_snapshot_server(cls, server, job_id, provider=ServerProvider.ALI_GROUP):
        snapshot_server_model = cls._get_snapshot_server_model(provider)
        snapshot_server_data = server.__data__.copy()
        source_server_id = snapshot_server_data["id"]
        snapshot_server_data.pop("id")
        snapshot_server_data["source_server_id"] = source_server_id
        snapshot_server_data["job_id"] = job_id
        snapshot_server = snapshot_server_model.create(**snapshot_server_data)
        return snapshot_server.id

    @classmethod
    def update_snapshot_server(cls, snapshot_server, **update_fields):
        snapshot_server.__data__.update(update_fields)
        snapshot_server.save()

    @classmethod
    def create_snapshot_cluster(cls, cluster_id, job_id):
        cluster = TestCluster.get_by_id(cluster_id)
        snapshot_cluster_data = cluster.__data__.copy()
        source_cluster_id = snapshot_cluster_data["id"]
        snapshot_cluster_data["source_cluster_id"] = source_cluster_id
        snapshot_cluster_data["job_id"] = job_id
        snapshot_cluster_data.pop("id")
        snapshot_cluster = TestClusterSnapshot.create(**snapshot_cluster_data)
        return snapshot_cluster.id

    @classmethod
    def create_snapshot_cluster_server(cls, cluster_id, snapshot_cluster_id, snapshot_server_id_dict, job_id):
        cluster_server = TestClusterServer.filter(cluster_id=cluster_id)
        for cs in cluster_server:
            snapshot_cluster_server_data = cs.__data__.copy()
            source_cluster_server_id = snapshot_cluster_server_data["id"]
            server_id = snapshot_cluster_server_data["server_id"]
            snapshot_cluster_server_data["source_cluster_server_id"] = source_cluster_server_id
            snapshot_cluster_server_data["cluster_id"] = snapshot_cluster_id
            snapshot_cluster_server_data["server_id"] = snapshot_server_id_dict.get(server_id)
            snapshot_cluster_server_data["job_id"] = job_id
            snapshot_cluster_server_data.pop("id")
            TestClusterServerSnapshot.create(**snapshot_cluster_server_data)

    @classmethod
    def set_job_state_desc(cls, job_id, state_desc):
        TestJob.update(state_desc=state_desc).where(
            TestJob.id == job_id
        ).execute()

    @classmethod
    def _get_server_model(cls, server_provider):
        if server_provider == ServerProvider.ALI_GROUP:
            return TestServer
        else:
            return CloudServer

    @classmethod
    def _get_snapshot_server_model(cls, server_provider):
        if server_provider == ServerProvider.ALI_GROUP:
            return TestServerSnapshot
        else:
            return CloudServerSnapshot

    @classmethod
    def get_server_by_provider(cls, server_id, provider):
        if provider == ServerProvider.ALI_GROUP:
            return TestServer.get_by_id(server_id)
        else:
            return CloudServer.get_by_id(server_id)

    @classmethod
    def get_snapshot_server_by_provider(cls, server_id, provider):
        if provider == ServerProvider.ALI_GROUP:
            return TestServerSnapshot.get_by_id(server_id)
        else:
            return CloudServerSnapshot.get_by_id(server_id)

    @classmethod
    def get_snapshot_server_id_by_provider(cls, job_id, source_server_id, provider):
        if provider == ServerProvider.ALI_GROUP:
            return TestServerSnapshot.get(
                job_id=job_id,
                source_server_id=source_server_id
            ).id
        else:
            return CloudServerSnapshot.get(
                job_id=job_id,
                source_server_id=source_server_id
            ).id

    @classmethod
    def _get_tag_object_id_set(cls, server_provider, server_mode):
        return {
            st.object_id for st in
            ServerTagRelation.select(
                ServerTagRelation.object_id
            ).filter(
                run_environment=server_provider,
                object_type=server_mode
            )
        }

    @classmethod
    def get_servers_by_cluster(cls, cluster_id, server_provider):
        servers = []
        cluster_servers = cls.get_cluster_server(cluster_id, server_provider, is_run=True)
        if server_provider == ServerProvider.ALI_GROUP:
            for cs in cluster_servers:
                servers.append(cs.testserver)
        else:
            for cs in cluster_servers:
                servers.append(cs.cloudserver)
        return servers

    @classmethod
    def get_cluster_server(cls, cluster_id, server_provider, is_run=False):
        if is_run:
            alloc_server = [ServerState.AVAILABLE, ServerState.RESERVED, ServerState.OCCUPIED]
        else:
            alloc_server = [ServerState.AVAILABLE, ServerState.RESERVED]
        server_model = cls._get_server_model(server_provider)
        cluster_server = TestClusterServer.select(
            TestClusterServer, server_model
        ).join(
            server_model, on=(TestClusterServer.server_id == server_model.id)
        ).where(
            TestClusterServer.cluster_id == cluster_id,
            TestClusterServer.is_deleted == DbField.FALSE,
            server_model.is_deleted == DbField.FALSE,
            server_model.state.in_(alloc_server),
            server_model.real_state.in_(alloc_server),
        )
        roles = {cs.role for cs in cluster_server}
        if ClusterRole.LOCAL in roles and ClusterRole.REMOTE in roles:
            return cluster_server
        return []

    @classmethod
    def _get_spec_tag_cluster(cls, tag_id_list, server_provider, ws_id):
        cluster_li = list()
        for tag_id in tag_id_list:
            clusters = TestCluster.select(
                TestCluster.id
            ).join(
                ServerTagRelation, on=(TestCluster.id == ServerTagRelation.object_id)
            ).where(
                TestCluster.ws_id == ws_id,
                TestCluster.is_deleted == DbField.FALSE,
                TestCluster.is_occpuied == DbField.FALSE,
                ServerTagRelation.run_environment == server_provider,
                ServerTagRelation.object_type == RunMode.CLUSTER,
                ServerTagRelation.server_tag_id == tag_id,
                ServerTagRelation.is_deleted == DbField.FALSE,
                TestCluster.occupied_job_id == DbField.NULL
            ).order_by(fn.Rand())
            cluster_li.append([cluster.id for cluster in clusters])
        cluster_set = set(cluster_li[0])
        for _cluster in cluster_li[1:]:
            cluster_set &= set(_cluster)
        if cluster_set:
            return TestCluster.get_by_id(list(cluster_set)[0])

    @classmethod
    def get_cluster_server_id_set(cls, cluster_id, server_provider):
        server_model = cls._get_server_model(server_provider)
        return {
            sm.id for sm in
            server_model.select(
                server_model.id
            ).join(
                TestClusterServer, on=(server_model.id == TestClusterServer.server_id)
            ).where(
                TestClusterServer.cluster_id == cluster_id,
                TestClusterServer.is_deleted == DbField.FALSE
            )
        }

    @classmethod
    def set_server_broken(cls, server, job_id, error_msg):
        server.history_state = server.state
        server.state = ServerState.BROKEN
        server.spec_use = SpecUseType.NO_SPEC_USE
        server.occupied_job_id = None
        server.broken_at = utils.get_now()
        server.broken_job_id = job_id
        server.broken_reason = error_msg
        server.save()
        job_obj = TestJob.get(id=job_id)
        update_server_state_log(job_obj.server_provider, server.id, to_broken=True)

    @classmethod
    def release_cluster(cls, cluster_id):
        TestCluster.update(
            is_occpuied=DbField.FALSE,
            occupied_job_id=None
        ).where(
            TestCluster.id == cluster_id,
            TestCluster.is_occpuied == DbField.TRUE
        ).execute()

    @classmethod
    def check_server_is_using_by_cache(cls, server):
        server_sn = server.server_sn
        is_using, using_id = False, None
        if server_sn:
            is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
            if is_using:
                logger.warning(f"{server.__repr__()} is using with cache, using "
                               f"job id is {using_id}, server_sn<{server_sn}>")
        return is_using, using_id

    @classmethod
    def add_server_to_using_cache(cls, job_id, *server_list):
        for server in server_list:
            server_sn = server.server_sn
            if server_sn:
                RemoteAllocServerSource.add_server_to_using_cache(server_sn, job_id)
            else:
                logger.warning(f"{server.__repr__()} has not server_sn!")

    @classmethod
    def check_cluster_server_using_by_cache(cls, cluster_server, provider=ServerProvider.ALI_GROUP):
        for cs in cluster_server:
            if provider == ServerProvider.ALI_GROUP:
                server = cs.testserver
            else:
                server = cs.cloudserver
            is_using, using_id = cls.check_server_is_using_by_cache(server)
            if is_using:
                return True, using_id
        return False, None
