import time
import random

from lib.peewee import fn
from core.cache.remote_source import RemoteAllocServerSource
from core.exec_channel import ExecChannel
from core.op_acc_msg import OperationAccompanyMsg as Oam
from constant import (
    ServerState,
    ServerProvider,
    RunMode,
    DbField,
    SpecUseType,
    ServerFlowFields
)
from models.base import db
from models.server import (
    TestServer,
    TestServerSnapshot,
    CloudServer,
    TestCluster,
    TestClusterServer,
    ServerTagRelation, CloudServerSnapshot,
)
from tools.log_util import LoggerFactory
from .base_db_op import CommonDbServerOperation
from ...distr_lock import DistributedLock

logger = LoggerFactory.alloc_server()


class AliGroupDbServerOperation(CommonDbServerOperation):

    @classmethod
    def _get_rand_server(cls, ws_id):
        tag_server_id_set = cls._get_tag_object_id_set(
            ServerProvider.ALI_GROUP, RunMode.STANDALONE
        )
        return TestServer.select().filter(
            TestServer.id.not_in(tag_server_id_set),
            TestServer.state == ServerState.AVAILABLE,
            TestServer.spec_use == SpecUseType.NO_SPEC_USE,
            TestServer.ws_id == ws_id,
            TestServer.in_pool == DbField.TRUE,
            TestServer.occupied_job_id == DbField.NULL
        ).order_by(fn.Rand()).first()

    @classmethod
    def get_rand_server(cls, ws_id, job_id):
        while 1:
            rand_server = cls._get_rand_server(ws_id)
            if rand_server:
                is_using, _ = cls.check_server_is_using_by_cache(rand_server)
                if is_using:
                    # 资源池机器需要校验有没有被非资源池机器的job所占用，
                    # 占用的情况返回None,防止重复分配到同一个在非资源池中的机器，
                    # 导致死循环
                    return None
                if cls._check_server_broken(job_id, rand_server):
                    continue
                if cls._set_server_used(job_id, rand_server):
                    return rand_server
            else:
                return None

    @classmethod
    def _get_rand_cluster(cls, ws_id):
        tag_cluster_id_set = cls._get_tag_object_id_set(
            ServerProvider.ALI_GROUP,
            RunMode.CLUSTER
        )
        return TestCluster.select().join(
            TestCluster, on=(TestCluster.id == TestClusterServer.cluster_id)
        ).join(
            TestServer, on=(TestServer.id == TestClusterServer.server_id)
        ).where(
            TestCluster.ws_id == ws_id,
            TestServer.state == ServerState.AVAILABLE,
            TestCluster.is_deleted == DbField.FALSE,
            TestServer.is_deleted == DbField.FALSE,
            TestCluster.id.not_in(tag_cluster_id_set),
            TestCluster.cluster_type == ServerProvider.ALI_GROUP,
            TestCluster.is_occpuied == DbField.FALSE,
            TestCluster.ws_id == ws_id,
            TestCluster.occupied_job_id == DbField.NULL,
        ).order_by(fn.Rand()).first()

    @classmethod
    def _clu_has_no_available_server(cls, ws_id, cluster_id):
        return TestClusterServer.select().join(
            TestCluster, on=(TestCluster.id == TestClusterServer.cluster_id)
        ).join(
            TestServer, on=(TestServer.id == TestClusterServer.server_id)
        ).where(
            TestCluster.ws_id == ws_id,
            TestCluster.is_occpuied == DbField.FALSE,
            TestClusterServer.cluster_id == cluster_id,
            TestServer.state.in_(ServerState.NO_AVAILABLE),
            TestCluster.is_deleted == DbField.FALSE,
            TestServer.is_deleted == DbField.FALSE,
            TestServer.occupied_job_id != DbField.NULL
        )

    @classmethod
    def get_rand_cluster(cls, ws_id, job_id):
        while 1:
            rand_cluster = cls._get_rand_cluster(ws_id)
            if rand_cluster:
                cluster_id = rand_cluster.id
                has_no_available = cls._clu_has_no_available_server(ws_id, cluster_id)
                if not has_no_available:
                    cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_GROUP)
                    if cluster_server:
                        is_using, _ = cls.check_cluster_server_using_by_cache(cluster_server)
                        if is_using:
                            return None
                        if cls._check_cluster_server_broken(job_id, cluster_server):
                            continue
                        if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                            return cluster_server
            else:
                return None

    @classmethod
    def get_rand_server_by_no_used_cluster(cls, ws_id, job_id):
        spec_server_set = {ts.id for ts in TestServer.select().filter(
            TestServer.ws_id == ws_id,
            TestServer.state == ServerState.AVAILABLE,
            TestServer.spec_use == SpecUseType.USE_BY_CLUSTER,
            TestServer.in_pool == DbField.TRUE,
            TestServer.occupied_job_id == DbField.NULL
        )}
        return cls._get_server_by_freq(ws_id, job_id, spec_server_set)

    @classmethod
    def get_spec_std_server_in_pool(cls, ws_id, job_id, server_id):
        spec_server = TestServer.select().filter(
            id=server_id,
            ws_id=ws_id,
            in_pool=DbField.TRUE,
        ).first()
        if spec_server:
            is_using, using_id = cls.check_server_is_using_by_cache(spec_server)
            if is_using:
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                    ServerFlowFields.USING_ID: using_id
                }
            if spec_server.state == ServerState.OCCUPIED:
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                    ServerFlowFields.USING_ID: spec_server.occupied_job_id
                }
            if spec_server.state == ServerState.BROKEN or cls._check_server_broken(job_id, spec_server):
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.BROKEN,
                }
            if spec_server.state == ServerState.RESERVED:
                time.sleep(random.random() * 2)
                is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(spec_server.server_sn)
                if is_using:
                    return None, {
                        ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                        ServerFlowFields.USING_ID: using_id
                    }
                else:
                    cls.add_server_to_using_cache(job_id, spec_server)
                    return spec_server, None
            if cls._set_server_used(job_id, spec_server):
                return spec_server, None
        return None, None

    @classmethod
    def get_spec_cluster_server(cls, ws_id, job_id, cluster_id):
        oppucied_cluster = TestCluster.filter(
            ws_id=ws_id, id=cluster_id, is_occpuied=DbField.TRUE).first()
        if oppucied_cluster:
            return None, {
                ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                ServerFlowFields.USING_ID: oppucied_cluster.occupied_job_id
            }
        has_no_available = cls._clu_has_no_available_server(ws_id, cluster_id)
        if not has_no_available:
            cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_GROUP)
            if cluster_server:
                is_using, using_id = cls.check_cluster_server_using_by_cache(cluster_server)
                if is_using:
                    return None, {
                        ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                        ServerFlowFields.USING_ID: using_id
                    }
                if cls._check_cluster_server_broken(job_id, cluster_server, cluster_id=cluster_id):
                    return None, {
                        ServerFlowFields.SERVER_STATE: ServerState.BROKEN
                    }
                if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                    return cluster_server, None
        return None, None

    @classmethod
    def _update_cluster_server(cls, job_id, cluster_id, cluster_server):
        update_res = False
        server_id_list = [tcs.testserver.id for tcs in cluster_server if tcs.testserver.state == ServerState.AVAILABLE]
        server_list = [tcs.testserver for tcs in cluster_server]
        server_id_len = len(server_id_list)
        with db.atomic() as transaction:
            update_num = TestServer.update(
                state=ServerState.OCCUPIED,
                occupied_job_id=job_id
            ).where(
                TestServer.id.in_(server_id_list),
            ).execute()
            if update_num == server_id_len:
                TestCluster.update(
                    is_occpuied=DbField.TRUE,
                    occupied_job_id=job_id
                ).where(
                    TestCluster.id == cluster_id,
                ).execute()
                transaction.commit()
                update_res = True
            else:
                transaction.rollback()
        if update_res:
            cls.add_server_to_using_cache(job_id, *server_list)
        return update_res

    @classmethod
    def get_rand_server_by_tag(cls, ws_id, job_id):
        tag_server_set = {
            ts.id for ts in
            TestServer.select().join(
                ServerTagRelation, on=(TestServer.id == ServerTagRelation.object_id)
            ).where(
                TestServer.ws_id == ws_id,
                TestServer.is_deleted == DbField.FALSE,
                TestServer.state == ServerState.AVAILABLE,
                TestServer.spec_use == SpecUseType.NO_SPEC_USE,
                TestServer.in_pool == DbField.TRUE,
                ServerTagRelation.run_environment == ServerProvider.ALI_GROUP,
                ServerTagRelation.object_type == RunMode.STANDALONE,
                ServerTagRelation.is_deleted == DbField.FALSE,
                TestServer.occupied_job_id == DbField.NULL
            )
        }
        return cls._get_server_by_freq(ws_id, job_id, tag_server_set)

    @classmethod
    def _get_server_by_freq(cls, ws_id, job_id, db_server_set):
        server_use_freq = RemoteAllocServerSource.get_server_use_freq(ws_id)  # 机器使用频率列表，从小到大
        cache_server_set = set(server_use_freq)
        no_use_server = list(db_server_set - cache_server_set)
        low_freq_server = list(db_server_set & cache_server_set)
        low_freq_server.sort(key=server_use_freq.index)
        no_use_server.extend(low_freq_server)
        for server_id in no_use_server:
            server = TestServer.select().filter(id=server_id).first()
            if server:
                is_using, _ = cls.check_server_is_using_by_cache(server)
                if is_using:
                    continue
                if cls._check_server_broken(job_id, server):
                    continue
                if cls._set_server_used(job_id, server):
                    return server
        return None

    @classmethod
    def _get_spec_tag_server(cls, tag_id_list, ws_id):
        server_li = list()
        for tag_id in tag_id_list:
            servers = TestServer.select(TestServer.id).join(
                ServerTagRelation,
                on=(TestServer.id == ServerTagRelation.object_id)
            ).where(
                TestServer.ws_id == ws_id,
                TestServer.is_deleted == DbField.FALSE,
                TestServer.state == ServerState.AVAILABLE,
                TestServer.spec_use == SpecUseType.NO_SPEC_USE,
                TestServer.in_pool == DbField.TRUE,
                ServerTagRelation.run_environment == ServerProvider.ALI_GROUP,
                ServerTagRelation.object_type == RunMode.STANDALONE,
                ServerTagRelation.server_tag_id == tag_id,
                ServerTagRelation.is_deleted == DbField.FALSE,
                TestServer.occupied_job_id == DbField.NULL
            ).order_by(fn.Rand())
            server_li.append([server.id for server in servers])
        server_set = set(server_li[0])
        for _server in server_li[1:]:
            server_set &= set(_server)
        if server_set:
            return TestServer.get_by_id(list(server_set)[0])

    @classmethod
    def get_spec_tag_server(cls, job_id, tag_id_list, ws_id):
        while 1:
            tag_server = cls._get_spec_tag_server(tag_id_list, ws_id)
            if tag_server:
                is_using, _ = cls.check_server_is_using_by_cache(tag_server)
                if is_using:
                    return None
                if cls._check_server_broken(job_id, tag_server):
                    continue
                if cls._set_server_used(job_id, tag_server):
                    return tag_server
            else:
                return None

    @classmethod
    def _get_rand_cluster_by_tag(cls, ws_id):
        return TestCluster.select(
            TestCluster.id
        ).join(
            ServerTagRelation,
            on=(TestCluster.id == ServerTagRelation.object_id)
        ).where(
            TestCluster.is_deleted == DbField.FALSE,
            TestCluster.ws_id == ws_id,
            TestCluster.is_occpuied == DbField.FALSE,
            ServerTagRelation.run_environment == ServerProvider.ALI_GROUP,
            ServerTagRelation.object_type == RunMode.CLUSTER,
            ServerTagRelation.is_deleted == DbField.FALSE,
            TestCluster.occupied_job_id == DbField.NULL
        ).order_by(fn.Rand()).first()

    @classmethod
    def get_rand_cluster_by_tag(cls, ws_id, job_id):
        while 1:
            tag_cluster = cls._get_rand_cluster_by_tag(ws_id)
            if tag_cluster:
                cluster_id = tag_cluster.id
                has_no_available = cls._clu_has_no_available_server(ws_id, cluster_id)
                if not has_no_available:
                    cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_GROUP)
                    if cluster_server:
                        is_using, _ = cls.check_cluster_server_using_by_cache(cluster_server)
                        if is_using:
                            return None
                        if cls._check_cluster_server_broken(job_id, cluster_server):
                            continue
                        if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                            return cluster_server
            else:
                return None

    @classmethod
    def _set_server_used(cls, job_id, server):
        server_id = server.id
        time.sleep(random.random() * 2)
        set_res = TestServer.update(
            state=ServerState.OCCUPIED,
            occupied_job_id=job_id
        ).where(
            TestServer.id == server_id,
            TestServer.state == ServerState.AVAILABLE
        ).execute()
        if set_res:
            cls.add_server_to_using_cache(job_id, server)
        return set_res

    @classmethod
    def get_spec_tag_cluster(cls, ws_id, job_id, tag_id_list):
        while 1:
            tag_cluster = cls._get_spec_tag_cluster(tag_id_list, ServerProvider.ALI_GROUP)
            if tag_cluster:
                cluster_id = tag_cluster.id
                has_no_available = cls._clu_has_no_available_server(ws_id, cluster_id)
                if not has_no_available:
                    cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_GROUP)
                    if cluster_server:
                        is_using, _ = cls.check_cluster_server_using_by_cache(cluster_server)
                        if is_using:
                            return None
                        if cls._check_cluster_server_broken(job_id, cluster_server):
                            continue
                        if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                            return cluster_server
            else:
                return None

    @classmethod
    def _check_cluster_server_broken(cls, job_id, cluster_server, cluster_id=None):
        for cs in cluster_server:
            if cls._check_server_broken(job_id, cs.testserver, run_mode=RunMode.CLUSTER, cluster_id=cluster_id):
                return True
        return False

    @classmethod
    def _check_server_broken(cls, job_id, server, in_pool=True, run_mode=RunMode.STANDALONE, cluster_id=None):
        check_success, error_msg = ExecChannel.check_server_status(
            server.channel_type, server.ip, tsn=server.tsn
        )
        if not check_success:
            Oam.set_server_broken_and_send_msg(
                job_id, server, in_pool=in_pool, run_mode=run_mode, cluster_id=cluster_id, error_msg=error_msg)
            return True
        return False

    @classmethod
    def release_server(cls, job_id, server_id, cluster_server=False):
        server = TestServer.get_or_none(id=server_id)
        if server:
            server_sn = server.server_sn
            if cluster_server or server.spec_use == SpecUseType.USE_BY_CLUSTER:
                spec_use = SpecUseType.USE_BY_CLUSTER
            else:
                spec_use = SpecUseType.NO_SPEC_USE
            TestServer.update(
                state=ServerState.AVAILABLE,
                spec_use=spec_use,
                occupied_job_id=None
            ).where(
                TestServer.id == server_id,
                TestServer.state == ServerState.OCCUPIED
            ).execute()
        else:
            logger.warning(f"server_id({server_id}) not in test_server!")
            server = TestServerSnapshot.get_or_none(job_id=job_id, source_server_id=server_id)
            if server:
                server_sn = server.server_sn
            else:
                logger.warning(f"server_id({server_id}) not in test_server_snapshot!")
                return
        if server_sn:
            is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
            if is_using and str(using_id) == str(job_id):
                RemoteAllocServerSource.remove_server_from_using_cache(server_sn)
        else:
            logger.warning(f"{server.__repr__()} has not server_sn!")

    @classmethod
    def get_spec_std_server_no_in_pool(cls, job_id, server_snapshot_id, provider=ServerProvider.ALI_GROUP):
        snapshot_server_model = cls._get_snapshot_server_model(provider)
        snapshot_server = snapshot_server_model.get_by_id(server_snapshot_id)
        with DistributedLock(
                'alloc_custom_server',
                10,
                15,
                purpose='alloc_custom_server'
        ) as Lock:
            _lock = Lock.lock
            if _lock:
                is_using, using_id = cls.check_server_is_using_by_cache(snapshot_server)
                if is_using:
                    if str(using_id) != str(job_id):
                        return None, {
                            ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                            ServerFlowFields.USING_ID: using_id
                        }
                else:
                    server_sn = snapshot_server.server_sn
                    if server_sn:
                        RemoteAllocServerSource.add_server_to_using_cache(server_sn, job_id)
        if snapshot_server.state == ServerState.BROKEN or cls._check_server_broken(
                job_id, snapshot_server, in_pool=False):
            return None, {
                ServerFlowFields.SERVER_STATE: ServerState.BROKEN,
            }
        else:
            return snapshot_server, None


class AliCloudDbServerOperation(CommonDbServerOperation):

    @classmethod
    def get_cloud_server(cls, job_id, server_id, snapshot_server_id=None):
        if snapshot_server_id:
            cloud_server = CloudServerSnapshot.get_by_id(snapshot_server_id)
        else:
            cloud_server = CloudServer.get_by_id(server_id)
        if not cloud_server.is_instance:
            cloud_server = CloudServer.get_or_none(parent_server_id=server_id, job_id=job_id)
        return cloud_server

    @classmethod
    def _get_rand_server(cls, ws_id):
        tag_server_id_set = cls._get_tag_object_id_set(
            ServerProvider.ALI_CLOUD, RunMode.STANDALONE
        )
        return CloudServer.select().filter(
            CloudServer.id.not_in(tag_server_id_set),
            (CloudServer.state == ServerState.AVAILABLE) |
            (CloudServer.state.is_null()),
            CloudServer.spec_use == SpecUseType.NO_SPEC_USE,
            CloudServer.ws_id == ws_id,
            CloudServer.occupied_job_id == DbField.NULL,
            CloudServer.is_instance == DbField.TRUE,
        ).order_by(fn.Rand()).first()

    @classmethod
    def get_rand_server(cls, ws_id, job_id):
        while 1:
            rand_server = cls._get_rand_server(ws_id)
            if rand_server:
                is_using, _ = cls.check_server_is_using_by_cache(rand_server)
                if is_using:
                    return None
                if cls._check_server_broken(job_id, rand_server):
                    continue
                if cls._set_server_used(job_id, rand_server):
                    return rand_server
            else:
                return None

    @classmethod
    def _get_rand_cluster(cls, ws_id):
        tag_cluster_id_set = cls._get_tag_object_id_set(
            ServerProvider.ALI_CLOUD, RunMode.CLUSTER
        )
        return TestCluster.select().filter(
            TestCluster.id.not_in(tag_cluster_id_set),
            TestCluster.cluster_type == ServerProvider.ALI_CLOUD,
            TestCluster.is_occpuied == DbField.FALSE,
            TestCluster.ws_id == ws_id,
            TestCluster.occupied_job_id == DbField.NULL
        ).order_by(fn.Rand()).first()

    @classmethod
    @db.atomic()
    def _update_cluster_server(cls, job_id, cluster_id, cluster_server):
        for cs in cluster_server:
            cloud_server = cs.cloudserver
            if cloud_server.is_instance:
                if cloud_server.state == ServerState.AVAILABLE:
                    cloud_server.state = ServerState.OCCUPIED
                    cloud_server.save()
                cls.add_server_to_using_cache(job_id, cloud_server)
        TestCluster.update(
            is_occpuied=DbField.TRUE,
            occupied_job_id=job_id
        ).where(
            TestCluster.id == cluster_id,
        ).execute()
        return True

    @classmethod
    def get_rand_cluster(cls, ws_id, job_id):
        while 1:
            rand_cluster = cls._get_rand_cluster(ws_id)
            if rand_cluster:
                cluster_id = rand_cluster.id
                cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_CLOUD)
                if cluster_server:
                    is_using, _ = cls.check_cluster_server_using_by_cache(cluster_server, ServerProvider.ALI_CLOUD)
                    if is_using:
                        return None
                    if cls._check_cluster_server_broken(job_id, cluster_server):
                        continue
                    if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                        return cluster_server
            else:
                return None

    @classmethod
    def get_spec_std_server_in_pool(cls, ws_id, job_id, server_id):
        spec_server = CloudServer.filter(
            id=server_id,
            ws_id=ws_id,
            in_pool=DbField.TRUE,
        ).first()
        if spec_server:
            is_using, using_id = cls.check_server_is_using_by_cache(spec_server)
            if is_using:
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                    ServerFlowFields.USING_ID: using_id
                }
            if spec_server.state == ServerState.OCCUPIED:
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                    ServerFlowFields.USING_ID: spec_server.occupied_job_id
                }
            if spec_server.state == ServerState.BROKEN or cls._check_server_broken(job_id, spec_server):
                return None, {
                    ServerFlowFields.SERVER_STATE: ServerState.BROKEN,
                }
            if spec_server.state == ServerState.RESERVED:
                if spec_server.is_instance:
                    cls.add_server_to_using_cache(job_id, spec_server)
                return spec_server, None
            if cls._set_server_used(job_id, spec_server):
                return spec_server, None
        return None, None

    @classmethod
    def get_spec_cluster_server(cls, job_id, cluster_id):
        is_instance = True
        occupied_cluster = TestCluster.filter(id=cluster_id, is_occpuied=DbField.TRUE).first()
        server_model = cls._get_server_model(ServerProvider.ALI_CLOUD)
        cluster_server = TestClusterServer.select(
            TestClusterServer, server_model
        ).join(
            server_model, on=(TestClusterServer.server_id == server_model.id)
        ).where(
            TestClusterServer.cluster_id == cluster_id,
            TestClusterServer.is_deleted == DbField.FALSE,
            server_model.is_deleted == DbField.FALSE)
        cloud_server_ids = [cs.server_id for cs in cluster_server]
        if cloud_server_ids:
            is_instance = CloudServer.get(id=cloud_server_ids[0]).is_instance
        if occupied_cluster and is_instance:
            return None, {
                ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                ServerFlowFields.JOB_ID: occupied_cluster.occupied_job_id
            }
        cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_CLOUD)
        if cluster_server:
            if is_instance:
                is_using, using_id = cls.check_cluster_server_using_by_cache(cluster_server, ServerProvider.ALI_CLOUD)
                if is_using:
                    return None, {
                        ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                        ServerFlowFields.USING_ID: using_id
                    }
                if cls._check_cluster_server_broken(job_id, cluster_server, cluster_id):
                    return None, {
                        ServerFlowFields.SERVER_STATE: ServerState.BROKEN
                    }
                if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                    return cluster_server, None
            else:
                return cluster_server, None
        return None, None

    @classmethod
    def _get_spec_tag_server(cls, tag_id_list):
        server_li = list()
        for tag_id in tag_id_list:
            servers = CloudServer.select().join(
                ServerTagRelation,
                on=(CloudServer.id == ServerTagRelation.object_id)
            ).where(
                CloudServer.is_deleted == DbField.FALSE,
                CloudServer.state == ServerState.AVAILABLE,
                CloudServer.spec_use == SpecUseType.NO_SPEC_USE,
                ServerTagRelation.run_environment == ServerProvider.ALI_CLOUD,
                ServerTagRelation.object_type == RunMode.STANDALONE,
                ServerTagRelation.server_tag_id == tag_id,
                ServerTagRelation.is_deleted == DbField.FALSE,
                CloudServer.occupied_job_id == DbField.NULL
            ).order_by(fn.Rand())
            server_li.append([server.id for server in servers])
        server_set = set(server_li[0])
        for _server in server_li[1:]:
            server_set &= set(_server)
        if server_set:
            return CloudServer.get_by_id(list(server_set)[0])

    @classmethod
    def get_spec_tag_server(cls, job_id, tag_id_list):
        while 1:
            tag_server = cls._get_spec_tag_server(tag_id_list)
            if tag_server:
                is_using, _ = cls.check_server_is_using_by_cache(tag_server)
                if is_using:
                    return None
                if cls._check_server_broken(job_id, tag_server):
                    continue
                if cls._set_server_used(job_id, tag_server):
                    return tag_server
            else:
                return None

    @classmethod
    def get_spec_tag_cluster(cls, job_id, tag_id_list):
        while 1:
            tag_cluster = cls._get_spec_tag_cluster(tag_id_list, ServerProvider.ALI_CLOUD)
            if tag_cluster:
                cluster_id = tag_cluster.id
                cluster_server = cls.get_cluster_server(cluster_id, ServerProvider.ALI_CLOUD)
                if cluster_server:
                    is_using, _ = cls.check_cluster_server_using_by_cache(cluster_server)
                    if is_using:
                        return None
                    if cls._check_cluster_server_broken(job_id, cluster_server):
                        continue
                    if cls._update_cluster_server(job_id, cluster_id, cluster_server):
                        return cluster_server
            else:
                return None

    @classmethod
    def _set_server_used(cls, job_id, server):
        if server.is_instance:
            server_id = server.id
            set_res = CloudServer.update(
                state=ServerState.OCCUPIED,
                occupied_job_id=job_id
            ).where(
                CloudServer.id == server_id,
                CloudServer.state == ServerState.AVAILABLE
            ).execute()
            if set_res:
                cls.add_server_to_using_cache(job_id, server)
            return set_res
        return True

    @classmethod
    def _check_cluster_server_broken(cls, job_id, cluster_server, cluster_id=None):
        for cs in cluster_server:
            if cls._check_server_broken(job_id, cs.cloudserver, run_mode=RunMode.CLUSTER, cluster_id=cluster_id):
                return True
        return False

    @classmethod
    def _check_server_broken(cls, job_id, server, in_pool=True, run_mode=RunMode.STANDALONE, cluster_id=None):
        if server.is_instance:
            check_success, error_msg = ExecChannel.check_server_status(
                server.channel_type, server.private_ip, tsn=server.tsn
            )
            if not check_success:
                Oam.set_server_broken_and_send_msg(
                    job_id, server, in_pool=in_pool,
                    run_mode=run_mode, cluster_id=cluster_id, error_msg=error_msg
                )
                return True
        return False

    @classmethod
    def release_server(cls, job_id, cloud_server, source_server_deleted=False, cluster_server=False):
        server_sn = None
        if cloud_server and not source_server_deleted:
            server_sn = cloud_server.server_sn
            if cluster_server or cloud_server.spec_use == SpecUseType.USE_BY_CLUSTER:
                spec_use = SpecUseType.USE_BY_CLUSTER
            else:
                spec_use = SpecUseType.NO_SPEC_USE
            if cloud_server.release_rule:
                cloud_server.is_deleted = DbField.TRUE
            else:
                if cloud_server.state == ServerState.OCCUPIED:
                    cloud_server.state = ServerState.AVAILABLE
            cloud_server.occupied_job_id = None
            cloud_server.spec_use = spec_use
            cloud_server.save()
        if server_sn:
            is_using, using_id = RemoteAllocServerSource.check_server_in_using_by_cache(server_sn)
            if is_using and str(using_id) == str(job_id):
                RemoteAllocServerSource.remove_server_from_using_cache(server_sn)
        else:
            logger.warning(f"{cloud_server.__repr__()} has not server_sn!")

    @classmethod
    def delete_cloud_server_when_it_destroy(cls, cloud_server):
        cloud_server.is_deleted = DbField.TRUE
        cloud_server.save()

    @classmethod
    def get_spec_std_server_no_in_pool(cls, job_id, server_snapshot_id, provider=ServerProvider.ALI_GROUP):
        snapshot_server_model = cls._get_snapshot_server_model(provider)
        snapshot_server = snapshot_server_model.get_by_id(server_snapshot_id)
        is_using, using_id = cls.check_server_is_using_by_cache(snapshot_server)
        if is_using:
            return None, {
                ServerFlowFields.SERVER_STATE: ServerState.OCCUPIED,
                ServerFlowFields.USING_ID: using_id
            }
        if snapshot_server.state == ServerState.BROKEN or cls._check_server_broken(
                job_id, snapshot_server, in_pool=False):
            return None, {
                ServerFlowFields.SERVER_STATE: ServerState.BROKEN,
            }
        else:
            RemoteAllocServerSource.add_server_to_using_cache(snapshot_server.server_sn, job_id)
            return snapshot_server, None
