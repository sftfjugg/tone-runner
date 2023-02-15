import datetime

from core.server.base import BaseAllocServer
from core.exception import AllocServerException
from models.job import TestJobCase
from scheduler.job.base_test import AliCloudStep
from constant import RunMode, RunStrategy, ServerProvider, ServerFlowFields, ReleaseRule, ServerState, ExecState
from models.server import CloudServerSnapshot, ReleaseServerRecord, CloudServer, TestCluster
from tools.log_util import LoggerFactory
from .rand_pool import (
    AliGroupRandStdPool,
    AliCloudRandStdPool,
    AliGroupRandClusterPool,
    AliCloudRandClusterPool
)
from .spec_server import (
    AliGroupSpecUseInPoolByJob,
    AliGroupSpecUseNoInPoolByJob,
    AliCloudSpecUseByJob,
    AliGroupSpecUseByCluster,
    AliCloudSpecUseByCluster, AliCloudSpecUseNoInPoolByJob,
)
from .tag_pool import (
    AliGroupTagStdPool,
    AliGroupTagClusterPool,
    AliCloudTagStdPool,
    AliCloudTagClusterPool
)
from .db_operation import (
    AliGroupDbServerOperation,
    AliCloudDbServerOperation,
)


alloc_server_logger = LoggerFactory.alloc_server()
release_server_logger = LoggerFactory.release_server()


class AllocServer(BaseAllocServer):

    def __init__(self, job_info, job_suite, job_case):
        self.ws_id = job_case.ws_id
        self.job_info = job_info
        self.job_case = job_case
        self.job_id = job_case.job_id
        self.job_suite_id = job_suite.id
        self.job_case_id = job_case.id
        self.run_mode = job_case.run_mode
        self.server_provider = job_case.server_provider
        self.server_object_id = job_case.server_object_id
        self.server_tag_id_list = job_case.server_tag_id_list
        self.server_snapshot_id = job_case.server_snapshot_id
        self.server_info = {ServerFlowFields.WS_ID: self.ws_id,
                            ServerFlowFields.JOB_ID: self.job_id,
                            ServerFlowFields.JOB_SUITE_ID: self.job_suite_id,
                            ServerFlowFields.JOB_CASE_ID: self.job_case_id,
                            ServerFlowFields.SERVER_PROVIDER: self.server_provider,
                            ServerFlowFields.TEST_TYPE: job_case.test_type,
                            ServerFlowFields.IN_POOL: True,
                            ServerFlowFields.SERVER_TSN: ''
                            }

    def rand_standalone_server(self):
        alloc_server_logger.info(
            f"Now alloc random standalone server in pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, server_provider:{self.server_provider}"
        )
        rand_std_key = f"rand_std_{self.job_id}_{self.job_suite_id}"
        if rand_std_key in self.job_info:
            # 如果case属于同一个job且随机分配，则为同一台机器
            self.job_info[rand_std_key].update(self.server_info)
            self.server_info = self.job_info[rand_std_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupRandStdPool.pull(self.server_info)
        else:
            AliCloudRandStdPool.pull(self.server_info)
        self.job_info[rand_std_key] = self.server_info

    def rand_cluster_server(self):
        alloc_server_logger.info(
            f"Now alloc random cluster server in pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, server_provider:{self.server_provider}"
        )
        rand_cluster_key = f"rand_cluster_{self.job_id}_{self.job_suite_id}"
        if rand_cluster_key in self.job_info:
            # 如果case属于同一个job且随机分配集群，则为同一个集群
            self.job_info[rand_cluster_key].update(self.server_info)
            self.server_info = self.job_info[rand_cluster_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupRandClusterPool.pull(self.server_info)
        else:
            AliCloudRandClusterPool.pull(self.server_info)
        self.job_info[rand_cluster_key] = self.server_info

    def spec_standalone_server_in_pool(self):
        alloc_server_logger.info(
            f"Now alloc special standalone server in pool, job_id: {self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, server_id:{self.server_object_id}, "
            f"server_provider:{self.server_provider}"
        )
        spec_std_key = f"spec_std_{self.server_object_id}"
        if spec_std_key in self.job_info:
            # case属于同一个Job且为指定的同一台资源池内机器，直接返回该机器信息
            self.job_info[spec_std_key].update(self.server_info)
            self.server_info = self.job_info[spec_std_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupSpecUseInPoolByJob.pull(
                self.server_info, server_id=self.server_object_id
            )
        else:
            AliCloudSpecUseByJob.pull(
                self.server_info, server_id=self.server_object_id
            )
        self.job_info[spec_std_key] = self.server_info

    def spec_standalone_server_no_in_pool(self):
        alloc_server_logger.info(
            f"Now alloc special standalone server not in pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, server_snapshot_id:{self.server_snapshot_id}"
        )
        spec_std_key_no_in_pool = f"spec_std_{self.server_snapshot_id}_no_in_pool"
        if spec_std_key_no_in_pool in self.job_info:
            # case属于同一个Job且为指定的同一台非资源池机器，直接返回该机器信息
            self.job_info[spec_std_key_no_in_pool].update(self.server_info)
            self.server_info = self.job_info[spec_std_key_no_in_pool]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupSpecUseNoInPoolByJob.pull(
                self.server_info,
                server_snapshot_id=self.server_snapshot_id,
            )
        else:
            AliCloudSpecUseNoInPoolByJob.pull(
                self.server_info,
                server_snapshot_id=self.server_snapshot_id,
            )
        self.job_info[spec_std_key_no_in_pool] = self.server_info

    def spec_cluster_server(self):
        alloc_server_logger.info(
            f"Now alloc special cluster server in pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, cluster_id:{self.server_object_id}, "
            f"server_provider:{self.server_provider}"
        )
        spec_cluster_key = f"spec_cluster_{self.server_object_id}"
        if spec_cluster_key in self.job_info:
            # case属于同一个job且为同一个指定集群，直接返回该集群信息
            self.job_info[spec_cluster_key].update(self.server_info)
            self.server_info = self.job_info[spec_cluster_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupSpecUseByCluster.pull(
                self.server_info, cluster_id=self.server_object_id
            )
        else:
            AliCloudSpecUseByCluster.pull(
                self.server_info, cluster_id=self.server_object_id
            )
        self.job_info[spec_cluster_key] = self.server_info

    def tag_standalone_server(self):
        alloc_server_logger.info(
            f"Now alloc server from standalone tag pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, tag_id:{self.server_tag_id_list}, "
            f"server_provider:{self.server_provider}"
        )
        std_tag_key = f"tag_std_{self.job_suite_id}_{self.server_tag_id_list}"
        if std_tag_key in self.job_info:
            # 如果case属于同一个job且为同一个单机标签池则为同一台机器
            self.job_info[std_tag_key].update(self.server_info)
            self.server_info = self.job_info[std_tag_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupTagStdPool.pull(
                self.server_info, tag_id_list=self.server_tag_id_list
            )
        else:
            AliCloudTagStdPool.pull(
                self.server_info, tag_id_list=self.server_tag_id_list
            )
        self.job_info[std_tag_key] = self.server_info

    def tag_cluster_server(self):
        alloc_server_logger.info(
            f"Now alloc server from cluster tag pool, job_id:{self.job_id}, "
            f"job_suite_id:{self.job_suite_id}, job_case_id:{self.job_case_id}, "
            f"ws_id:{self.ws_id}, tag_id:{self.server_tag_id_list}, "
            f"server_provider:{self.server_provider}"
        )
        cluster_tag_key = f"tag_cluster_{self.job_suite_id}_{self.server_tag_id_list}"
        if cluster_tag_key in self.job_info:
            # 如果case属于同一个job且为同一个集群标签池则为同一个集群
            self.job_info[cluster_tag_key].update(self.server_info)
            self.server_info = self.job_info[cluster_tag_key]
            return
        if self.server_provider == ServerProvider.ALI_GROUP:
            AliGroupTagClusterPool.pull(
                self.server_info, tag_id_list=self.server_tag_id_list
            )
        else:
            AliCloudTagClusterPool.pull(
                self.server_info, tag_id_list=self.server_tag_id_list
            )
        self.job_info[cluster_tag_key] = self.server_info

    @classmethod
    def _release_ali_group_server(cls, job_id, server_id, cluster_server=False):
        AliGroupDbServerOperation.release_server(job_id, server_id, cluster_server)

    @classmethod
    def _release_ali_cloud_server(cls, job_id, server_id, snapshot_cluster_id=None, old_is_instance=None):
        # 云上配置集群场景，server_id为机器配置生成的机器实例id, snapshot_cluster_id为配置集群生成的实例集群快照id
        cluster_server = True if snapshot_cluster_id else False
        source_server_deleted = False
        cloud_server = CloudServer.get_by_id(server_id)
        if not cloud_server:
            source_server_deleted = True
            cloud_server = CloudServerSnapshot.get_or_none(job_id=job_id, source_server_id=server_id)
        if old_is_instance:
            # 机器实例不涉及释放、或失败保留
            AliCloudDbServerOperation.release_server(cloud_server, source_server_deleted, cluster_server)
        else:
            has_fail_case = cls.check_job_case_has_fail(job_id, server_id, snapshot_cluster_id)
            if cloud_server:
                if cloud_server.release_rule == ReleaseRule.RELEASE or \
                        (cloud_server.release_rule == ReleaseRule.DELAY_RELEASE and not has_fail_case):
                    if AliCloudStep.release_instance(cloud_server):
                        AliCloudDbServerOperation.release_server(cloud_server, source_server_deleted, cluster_server)
                        AliCloudDbServerOperation.delete_cloud_server_when_it_destroy(cloud_server)
                    else:
                        release_server_logger.error(f"Release server fail, job_id:{job_id}, server_id:{server_id}")
                elif cloud_server.release_rule == ReleaseRule.DELAY_RELEASE and has_fail_case:
                    # 如果任务失败，延时释放， 保存记录到数据库，tone做释放操作
                    try:
                        if not source_server_deleted:
                            cls.set_release_server_info_and_record(cloud_server, job_id)
                    except Exception as e:
                        release_server_logger.error(f"Release server fail, job_id:{job_id}, server_id:{server_id}"
                                                    f"error: {e}")
                else:
                    AliCloudDbServerOperation.release_server(cloud_server, source_server_deleted, cluster_server)
            else:
                AliCloudDbServerOperation.release_server(cloud_server, source_server_deleted, cluster_server)

    @classmethod
    def set_release_server_info_and_record(cls, cloud_server, job_id):
        ReleaseServerRecord.create(
            server_id=cloud_server.id,
            server_instance_id=cloud_server.instance_id,
            estimated_release_at=datetime.datetime.now() + datetime.timedelta(days=1)
        )
        cloud_server.state = ServerState.UNUSABLE
        cloud_server.description = f'跑Job({job_id})时有失败case，故延时24小时释放'
        cloud_server.save()

    @classmethod
    def check_job_case_has_fail(cls, job_id, server_id, snapshot_cluster_id):
        has_fail_case = False
        if not snapshot_cluster_id:
            cloud_server_snapshot_id_set = {cloud_server_snapshot.id for cloud_server_snapshot in
                                            CloudServerSnapshot.select(CloudServerSnapshot.id).filter(
                                                job_id=job_id, source_server_id=server_id)}
            if cloud_server_snapshot_id_set:
                has_fail_case = TestJobCase.filter(TestJobCase.job_id == job_id,
                                                   TestJobCase.state == ExecState.FAIL,
                                                   TestJobCase.run_mode == RunMode.STANDALONE,
                                                   TestJobCase.server_snapshot_id.in_(
                                                       cloud_server_snapshot_id_set)).exists()

        else:
            has_fail_case = TestJobCase.filter(TestJobCase.job_id == job_id,
                                               TestJobCase.state == ExecState.FAIL,
                                               TestJobCase.run_mode == RunMode.CLUSTER,
                                               TestJobCase.server_snapshot_id == snapshot_cluster_id).exists()
        return has_fail_case

    @classmethod
    def release_server(cls, job_id, server_object_id, run_mode, server_provider, snapshot_cluster_id=None,
                       old_is_instance=None):
        if run_mode == RunMode.CLUSTER:
            cls._release_cluster_server(job_id, server_object_id, server_provider,
                                        snapshot_cluster_id=snapshot_cluster_id, old_is_instance=old_is_instance)
        else:
            cls._release_std_server(job_id, server_object_id, server_provider, old_is_instance)

    @classmethod
    def _release_std_server(cls, job_id, server_id, server_provider, old_is_instance):
        try:
            release_server_logger.info(
                f"Release standalone server, server_id:{server_id}, "
                f"server_provider:{server_provider}, job_id:{job_id}"
            )
        except (Exception, PermissionError):
            pass
        if server_provider == ServerProvider.ALI_GROUP:
            cls._release_ali_group_server(job_id, server_id)
        else:
            cls._release_ali_cloud_server(job_id, server_id, old_is_instance=old_is_instance)

    @classmethod
    def _release_ali_group_cluster(cls, job_id, cluster_id):
        cluster_server_id_set = AliGroupDbServerOperation.get_cluster_server_id_set(
            cluster_id, ServerProvider.ALI_GROUP)
        for server_id in cluster_server_id_set:
            cls._release_ali_group_server(job_id, server_id, cluster_server=True)
        AliGroupDbServerOperation.release_cluster(cluster_id)

    @classmethod
    def _release_ali_cloud_cluster(cls, job_id, cluster_id, snapshot_cluster_id, old_is_instance):
        is_instance = TestCluster.get_by_id(cluster_id).is_instance
        if is_instance == 1:
            cluster_server_id_set = AliCloudDbServerOperation.get_cluster_server_id_set(
                cluster_id, ServerProvider.ALI_CLOUD)
        else:
            cluster_server_id_set = AliCloudDbServerOperation.get_ali_cloud_cluster_server_id_set(job_id,
                                                                                                  snapshot_cluster_id)
        for server_id in cluster_server_id_set:
            cls._release_ali_cloud_server(job_id, server_id, snapshot_cluster_id=snapshot_cluster_id,
                                          old_is_instance=old_is_instance)
        AliCloudDbServerOperation.release_cluster(cluster_id)

    @classmethod
    def _release_cluster_server(cls, job_id, cluster_id, server_provider, snapshot_cluster_id=None,
                                old_is_instance=None):
        release_server_logger.info(
            f"Release cluster, cluster_id:{cluster_id}, "
            f"server_provider:{server_provider}, job_id:{job_id}"
        )
        if server_provider == ServerProvider.ALI_GROUP:
            cls._release_ali_group_cluster(job_id, cluster_id)
        else:
            cls._release_ali_cloud_cluster(job_id, cluster_id, snapshot_cluster_id, old_is_instance)

    def _get_standalone_server_info(self):
        self.server_info[ServerFlowFields.RUN_MODE] = RunMode.STANDALONE
        if self.server_tag_id_list:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.TAG
            self.tag_standalone_server()
        elif self.server_object_id:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.SPEC
            self.spec_standalone_server_in_pool()
        elif self.server_snapshot_id:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.SPEC
            self.spec_standalone_server_no_in_pool()
        else:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.RAND
            self.rand_standalone_server()

    def _get_cluster_server_info(self):
        self.server_info[ServerFlowFields.RUN_MODE] = RunMode.CLUSTER
        if self.server_tag_id_list:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.TAG
            self.tag_cluster_server()
        elif self.server_object_id:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.SPEC
            self.spec_cluster_server()
        else:
            self.server_info[ServerFlowFields.RUN_STRATEGY] = RunStrategy.RAND
            self.rand_cluster_server()

    def update_job_case(self):
        if ServerFlowFields.SERVER_SNAPSHOT_ID in self.server_info:
            self.job_case.server_snapshot_id = self.server_info[ServerFlowFields.SERVER_SNAPSHOT_ID]
        if ServerFlowFields.SNAPSHOT_CLUSTER_ID in self.server_info:
            self.job_case.server_snapshot_id = self.server_info[ServerFlowFields.SNAPSHOT_CLUSTER_ID]
        self.job_case.save()

    def get_server_info(self):
        try:
            if self.run_mode == RunMode.STANDALONE:
                self._get_standalone_server_info()
            else:
                self._get_cluster_server_info()
            self.update_job_case()
            alloc_server_logger.info(f"Get server info:{self.server_info}")
            return self.server_info
        except Exception as error:
            alloc_server_logger.exception("Alloc server exception:")
            raise AllocServerException(error)
