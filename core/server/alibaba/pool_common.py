import config
from copy import deepcopy
from core.cache.remote_source import RemoteAllocServerSource
from constant import (
    ServerFlowFields,
    ServerReady,
    ChannelType,
    ServerState,
    ServerProvider,
    RunMode,
    RunStrategy,
    UsingIdFlag
)
from tools.sn_ip_cache import get_sn_by_ip
from .base_db_op import CommonDbServerOperation as Cs


class PoolCommonOperation:

    CLOUD_INST_REQUIRE_FIELDS = {
        "ak_id": "ak_id",
        "provider": "provider",
        "image_id": "image",
        "region": "region",
        "zone_id": "zone",
        "is_instance": "is_instance",
        "instance_id": "instance_id",
        "instance_type": "instance_type",
        "name": "instance_name",
        "internet_max_bandwidth": "bandwidth",
        "data_disk_category": "storage_type",
        "data_disk_count": "storage_number",
        "data_disk_size": "storage_size",
        "system_disk_category": "system_disk_category",
        "system_disk_size": "system_disk_size",
    }

    @classmethod
    def __get_cloud_instance_meta_data(cls, meta_data):
        _meta_data = dict()
        _meta_data["login_password"] = config.ECS_LOGIN_PASSWORD
        for alias, meta_field in cls.CLOUD_INST_REQUIRE_FIELDS.items():
            _meta_data[alias] = meta_data[meta_field]
            if alias == 'name' and not _meta_data[alias]:
                _meta_data[alias] = meta_data['template_name']
        return _meta_data

    @classmethod
    def _pull_std_server_in_pool(cls, ws_id, job_id, server, default_info, server_info=None):
        if server:
            server_id, server_ip = server.id, server.ip
            channel_type = server.channel_type or ChannelType.STAR_AGENT
            RemoteAllocServerSource.set_server_use_freq(ws_id, server_id)  # 设置机器使用频率
            snapshot_server_id = Cs.create_snapshot_server(server, job_id)
            default_info[ServerFlowFields.SERVER_ID] = server_id
            default_info[ServerFlowFields.SERVER_SNAPSHOT_ID] = snapshot_server_id
            default_info[ServerFlowFields.SERVER_IP] = server_ip
            default_info[ServerFlowFields.SERVER_SN] = get_sn_by_ip(server_ip)
            default_info[ServerFlowFields.CHANNEL_TYPE] = channel_type
            default_info[ServerFlowFields.READY] = ServerReady.READY
        else:
            if server_info:
                server_state = server_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def _pull_std_server_no_in_pool(cls, job_id, snapshot_server, default_info, server_info=None):
        if snapshot_server:
            snapshot_server_id = snapshot_server.id
            server_ip = snapshot_server.ip
            channel_type = snapshot_server.channel_type or ChannelType.STAR_AGENT
            default_info[ServerFlowFields.SERVER_ID] = server_ip
            default_info[ServerFlowFields.SERVER_SNAPSHOT_ID] = snapshot_server_id
            default_info[ServerFlowFields.SERVER_IP] = server_ip
            default_info[ServerFlowFields.SERVER_SN] = get_sn_by_ip(server_ip)
            default_info[ServerFlowFields.CHANNEL_TYPE] = channel_type
            default_info[ServerFlowFields.IN_POOL] = False
            default_info[ServerFlowFields.READY] = ServerReady.READY
            Cs.update_snapshot_server(snapshot_server, job_id=job_id)
        else:
            if server_info:
                server_state = server_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def _pull_std_cloud_server_no_in_pool(cls, job_id, snapshot_server, default_info, server_info=None):
        if snapshot_server:
            snapshot_server_id = snapshot_server.id
            server_ip = snapshot_server.server_ip
            channel_type = snapshot_server.channel_type or ChannelType.TONE_AGENT
            default_info[ServerFlowFields.SERVER_ID] = server_ip
            default_info[ServerFlowFields.SERVER_SNAPSHOT_ID] = snapshot_server_id
            default_info[ServerFlowFields.SERVER_IP] = server_ip
            default_info[ServerFlowFields.SERVER_SN] = snapshot_server.server_sn
            default_info[ServerFlowFields.CHANNEL_TYPE] = channel_type
            default_info[ServerFlowFields.IN_POOL] = False
            default_info[ServerFlowFields.READY] = ServerReady.READY
            cloud_inst_meta = cls.get_cloud_instance_meta_data(snapshot_server)
            default_info[ServerFlowFields.CLOUD_INST_META] = cloud_inst_meta
            Cs.update_snapshot_server(snapshot_server, job_id=job_id)
        else:
            if server_info:
                server_state = server_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def _get_servers_from_cluster(cls, cluster_server, snapshot_server_id_dict, job_id):
        servers = []
        for cs in cluster_server:
            test_server = cs.testserver
            server_id = test_server.id
            server_ip = test_server.ip
            snapshot_server_id = Cs.create_snapshot_server(test_server, job_id)
            snapshot_server_id_dict[server_id] = snapshot_server_id
            servers.append(
                {
                    ServerFlowFields.ROLE: cs.role,
                    ServerFlowFields.BASELINE_SERVER: cs.baseline_server,
                    ServerFlowFields.KERNEL_INSTALL: cs.kernel_install,
                    ServerFlowFields.VAR: cs.var_name,
                    ServerFlowFields.SERVER_ID: server_id,
                    ServerFlowFields.SERVER_SNAPSHOT_ID: snapshot_server_id,
                    ServerFlowFields.SERVER_IP: server_ip,
                    ServerFlowFields.SERVER_SN: get_sn_by_ip(server_ip),
                    ServerFlowFields.CHANNEL_TYPE: test_server.channel_type or ChannelType.STAR_AGENT,
                    ServerFlowFields.IN_POOL: test_server.in_pool
                }
            )
        return servers

    @classmethod
    def _pull_cluster_server(cls, job_id, cluster_server, default_info, cluster_info=None):
        if cluster_server:
            cluster_id = cluster_server[0].cluster_id
            snapshot_server_id_dict = {}
            snapshot_cluster_id = Cs.create_snapshot_cluster(cluster_id, job_id)
            default_info[ServerFlowFields.CLUSTER_ID] = cluster_id
            default_info[ServerFlowFields.SNAPSHOT_CLUSTER_ID] = snapshot_cluster_id
            default_info[ServerFlowFields.CLUSTER_SERVERS] = cls._get_servers_from_cluster(
                cluster_server, snapshot_server_id_dict, job_id)
            Cs.create_snapshot_cluster_server(cluster_id, snapshot_cluster_id, snapshot_server_id_dict, job_id)
            default_info[ServerFlowFields.READY] = ServerReady.READY
        else:
            if cluster_info:
                server_state = cluster_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def get_cloud_instance_meta_data(cls, cloud_server):
        meta_data = deepcopy(cloud_server.__data__)
        return cls.__get_cloud_instance_meta_data(meta_data)

    @classmethod
    def _pull_cloud_std_server(cls, job_id, cloud_server, default_info, server_info=None):
        if cloud_server:
            server_ip = cloud_server.server_ip
            private_ip = cloud_server.private_ip
            cloud_inst_meta = cls.get_cloud_instance_meta_data(cloud_server)
            channel_type = cloud_server.channel_type or ChannelType.TONE_AGENT
            snapshot_server_id = Cs.create_snapshot_server(
                cloud_server, job_id, ServerProvider.ALI_CLOUD)
            default_info[ServerFlowFields.IS_INSTANCE] = cloud_server.is_instance
            default_info[ServerFlowFields.SERVER_ID] = cloud_server.id
            default_info[ServerFlowFields.SERVER_SNAPSHOT_ID] = snapshot_server_id
            default_info[ServerFlowFields.SERVER_IP] = server_ip
            default_info[ServerFlowFields.PRIVATE_IP] = private_ip
            default_info[ServerFlowFields.SERVER_SN] = cloud_server.server_sn
            default_info[ServerFlowFields.SERVER_TSN] = cloud_server.server_tsn
            default_info[ServerFlowFields.CHANNEL_TYPE] = channel_type
            default_info[ServerFlowFields.CLOUD_INST_META] = cloud_inst_meta
            default_info[ServerFlowFields.READY] = ServerReady.READY
        else:
            if server_info:
                server_state = server_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def _get_cloud_servers_from_cluster(cls, cluster_server, snapshot_server_id_dict, job_id):
        servers = []
        for cs in cluster_server:
            cloud_server = cs.cloudserver
            server_id = cloud_server.id
            server_ip = cloud_server.server_ip
            private_ip = cloud_server.private_ip
            snapshot_server_id = Cs.create_snapshot_server(cloud_server, job_id, ServerProvider.ALI_CLOUD)
            snapshot_server_id_dict[server_id] = snapshot_server_id
            cloud_inst_meta = cls.get_cloud_instance_meta_data(cloud_server)
            channel_type = cloud_server.channel_type or ChannelType.TONE_AGENT
            servers.append(
                {
                    ServerFlowFields.ROLE: cs.role,
                    ServerFlowFields.BASELINE_SERVER: cs.baseline_server,
                    ServerFlowFields.KERNEL_INSTALL: cs.kernel_install,
                    ServerFlowFields.VAR: cs.var_name,
                    ServerFlowFields.IS_INSTANCE: cloud_server.is_instance,
                    ServerFlowFields.SERVER_ID: server_id,
                    ServerFlowFields.SERVER_SNAPSHOT_ID: snapshot_server_id,
                    ServerFlowFields.SERVER_IP: server_ip,
                    ServerFlowFields.PRIVATE_IP: private_ip,
                    ServerFlowFields.SERVER_SN: cloud_server.server_sn,
                    ServerFlowFields.CHANNEL_TYPE: channel_type,
                    ServerFlowFields.IN_POOL: cloud_server.in_pool,
                    ServerFlowFields.CLOUD_INST_META: cloud_inst_meta
                }
            )
        return servers

    @classmethod
    def _pull_cloud_cluster_server(cls, job_id, cluster_server, default_info, cluster_info=None):
        if cluster_server:
            cluster_id = cluster_server[0].cluster_id
            snapshot_server_id_dict = {}
            snapshot_cluster_id = Cs.create_snapshot_cluster(cluster_id, job_id)
            default_info[ServerFlowFields.CLUSTER_ID] = cluster_id
            default_info[ServerFlowFields.SNAPSHOT_CLUSTER_ID] = snapshot_cluster_id
            default_info[ServerFlowFields.CLUSTER_SERVERS] = cls._get_cloud_servers_from_cluster(
                cluster_server, snapshot_server_id_dict, job_id)
            Cs.create_snapshot_cluster_server(cluster_id, snapshot_cluster_id, snapshot_server_id_dict, job_id)
            default_info[ServerFlowFields.READY] = ServerReady.READY
        else:
            if cluster_info:
                server_state = cluster_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    default_info[ServerFlowFields.BE_USING] = True
                elif server_state == ServerState.BROKEN:
                    default_info[ServerFlowFields.BROKEN] = True
            default_info[ServerFlowFields.READY] = ServerReady.NO_READY

    @classmethod
    def _set_spec_server_job_state_desc(cls, job_id, spec_server, server_info):
        job_state_desc = None
        if not spec_server:
            if not server_info:
                job_state_desc = "当前指定机器不在机器池中。"
            else:
                server_state = server_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    using_id = server_info.get(ServerFlowFields.USING_ID)
                    who_using = cls.who_using_server(using_id)
                    using_id = cls.real_using_id(using_id)
                    job_state_desc = f"当前指定机器被{who_using}({using_id})占用。"
                elif server_state == ServerState.BROKEN:
                    job_state_desc = "当前指定机器状态已Broken，请检查机器及agent状态是否可用。"
        Cs.set_job_state_desc(job_id, job_state_desc)

    @classmethod
    def _set_spec_cluster_job_state_desc(cls, job_id, spec_cluster, cluster_info):
        job_state_desc = None
        if not spec_cluster:
            if not cluster_info:
                job_state_desc = "当前指定集群暂不可用，请检查集群及机器状态是否可用。"
            else:
                server_state = cluster_info.get(ServerFlowFields.SERVER_STATE)
                if server_state == ServerState.OCCUPIED:
                    using_id = cluster_info.get(ServerFlowFields.USING_ID)
                    if not using_id:
                        using_id = cluster_info.get(ServerFlowFields.JOB_ID)
                    who_using = cls.who_using_server(using_id)
                    using_id = cls.real_using_id(using_id)
                    job_state_desc = f"当前指定集群被{who_using}({using_id})占用。"
                elif server_state == ServerState.BROKEN:
                    job_state_desc = "当前指定集群状态已Broken，请检查机器及agent状态是否可用。"
        Cs.set_job_state_desc(job_id, job_state_desc)

    @classmethod
    def _update_wait_server_to_cache(cls, ws_id, job_id, server_object,
                                     run_strategy=RunStrategy.RAND,
                                     run_mode=RunMode.STANDALONE,
                                     provider=ServerProvider.ALI_GROUP,
                                     spec_object_id=None):
        """如果没有获取到job的随机机器时将该job_id存入redis的有序集合中,
        如果获取到job的随机机器则从有序集合中移除"""
        if server_object:
            RemoteAllocServerSource.remove_wait_server_source(
                ws_id, job_id, run_strategy,
                run_mode, provider, spec_object_id
            )

        else:
            RemoteAllocServerSource.set_wait_server_source(
                ws_id, job_id, run_strategy,
                run_mode, provider, spec_object_id
            )

    @classmethod
    def _alloc_by_compare_object_id(cls, ws_id, job_id,
                                    run_strategy=RunStrategy.RAND,
                                    run_mode=RunMode.STANDALONE,
                                    provider=ServerProvider.ALI_GROUP,
                                    spec_object_id=None):
        """取出redis指定分类的有序集合中最小的job_id，如果当前处理的
        job_id小于等于它(考虑小于主要是兼容数据异常的情况)，则继续执行当前随机分配逻辑"""
        priority_first_job_id = RemoteAllocServerSource.get_wait_server_source(
            ws_id, run_strategy, run_mode, provider, spec_object_id
        )
        if priority_first_job_id and job_id > int(priority_first_job_id):
            return False
        return True

    @classmethod
    def who_using_server(cls, using_id):
        if UsingIdFlag.PLAN_INST in str(using_id):
            return "PlanInstance"
        return "Job"

    @classmethod
    def real_using_id(cls, using_id):
        if UsingIdFlag.PLAN_INST in str(using_id):
            return using_id[:-len(UsingIdFlag.PLAN_INST)]
        return using_id
