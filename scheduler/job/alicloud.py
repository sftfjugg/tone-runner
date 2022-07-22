import json
import time
import traceback

import config
from constant import (
    ExecState,
    StepStage,
    ServerFlowFields,
    ServerState
)
from core.agent.tone_agent import ToneAgentClient
from core.exception import ExecStepException
from core.agent import tone_agent
from core.server.alibaba.base_db_op import CommonDbServerOperation as Cs
from core.server.alibaba.pool_common import PoolCommonOperation
from lib.cloud.provider import get_cloud_driver, Provider
from lib.cloud.provider_info import ProviderInfo
from models.job import TestStep, TestJob
from models.server import CloudServer, CloudServerSnapshot, CloudAk
from models.user import User
from tools.log_util import LoggerFactory
from .aligroup import AliGroupStep
from .step_common import check_step_exists


logger = LoggerFactory.scheduler()


class AliCloudStep(AliGroupStep):

    @classmethod
    def _ecs_cloud_info(cls, driver, cloud_server, instance_name):
        node = driver.describe_instance(cloud_server.instance_id)
        cloud_server.instance_name = instance_name
        vpc_attributes = node.extra.get("vpc_attributes")
        cloud_server.private_ip = vpc_attributes.get("private_ip_address") \
            if vpc_attributes else None
        cloud_server.pub_ip = node.public_ips[0] if node.public_ips else None
        cloud_server.hostname = node.extra.get("hostname")
        cloud_server.sn = node.extra.get("serial_number")

    @classmethod
    def _eci_cloud_info(cls, driver, cloud_server):
        node = driver.describe_instance(cloud_server.instance_id)
        cloud_server.instance_name = node["Containers"][0]["Name"]
        cloud_server.private_ip = node["IntranetIp"]
        cloud_server.pub_ip = node["InternetIp"]
        cloud_server.hostname = node["EniInstanceId"]  # useless field

    @classmethod
    def _create_cloud_instance(cls, driver, provider_info, job_id, server_id, snapshot_server_id=None):
        try:
            provider_info_dict = provider_info.to_dict()
            logger.info(f"Create cloud instance, provider_info:{provider_info_dict}")
            if server_id:
                template_cloud_server = CloudServer.get_by_id(server_id)
            else:
                template_cloud_server = CloudServerSnapshot.get_by_id(snapshot_server_id)
            ak_id = template_cloud_server.ak_id
            cloud_ak = CloudAk.select().filter(id=ak_id).first()
            if cloud_ak and cloud_ak.vm_quota and cloud_ak.vm_quota != '*':
                ak_server_count = CloudServer.select(CloudServer.id).where(CloudServer.ak_id == ak_id,
                                                                           CloudServer.is_instance == 1).count()
                if isinstance(cloud_ak.vm_quota, int) and ak_server_count >= int(cloud_ak.vm_quota):
                    logger.error(
                        f"job<{job_id}> create cloud instance by server <{server_id}> out of vm_quota."
                    )
                    raise ExecStepException('create cloud instance out of vm_quota.')
            new_cloud_server_data = template_cloud_server.__data__.copy()
            job_obj = TestJob.get_by_id(job_id)
            user = User.get_or_none(id=job_obj.creator)
            instance_name = f"tone-testbox-{user.username}-{job_id}"
            provider_info_dict.update({'name': instance_name})
            # image_id如果是动态配置，则获取最新image
            if ':latest' in provider_info_dict['image_id']:
                latest_image = cls._get_latest_image_by_expression(
                    driver, provider_info_dict['instance_type'],
                    provider_info_dict['image_id'])
                provider_info_dict['image_id'] = latest_image
                new_cloud_server_data['image'] = latest_image
            if new_cloud_server_data.get('extra_param'):
                provider_info_dict.update(
                    {'extra_param': json.loads(new_cloud_server_data.get('extra_param'))}
                )
            provider_info_dict.update({'name': instance_name})
            logger.info('create ecs request data:{}'.format(provider_info_dict))
            instance_id = driver.create_instance(**provider_info_dict)
            new_cloud_server_data.pop("id")
            new_cloud_server_data.update({
                "job_id": job_id,
                "occupied_job_id": job_id,
                "parent_server_id": server_id or snapshot_server_id,
                "instance_id": instance_id,
                "is_instance": True,
                "state": ServerState.OCCUPIED,
                "template_name": f"{provider_info_dict.get('template_name', '')}_{time.time()}",
            })
            cloud_server = CloudServer(**new_cloud_server_data)
            if provider_info.provider == Provider.ALIYUN_ECS:
                cls._ecs_cloud_info(driver, cloud_server, instance_name) 
            else:
                cls._eci_cloud_info(driver, cloud_server)
            cloud_server.save()
            logger.info(f"Cloud instance save to db, cloud_server_id:{cloud_server.id}")
            return cloud_server
        except Exception as error:
            logger.error(
                f"job<{job_id}> create cloud instance by server<{server_id}> has exception: "
            )
            raise ExecStepException(error)

    @classmethod
    def _deploy_cloud_agent(cls, cloud_server, cloud_driver):
        provider = cloud_server.provider
        cloud_server_ip = cloud_server.pub_ip
        cloud_private_ip = cloud_server.private_ip
        try:
            image = cloud_server.image_name.lower() or cloud_server.image.lower()
            arch = 'aarch_64' if 'arm64' in image or 'aarch' in image else 'x86_64'
            os_type = 'debian' if 'debian' in image or 'ubuntu' in image else 'linux'
            logger.info(
                f"Cloud server({cloud_server.instance_id}-{cloud_server_ip})"
                f" deploying, provider:{provider}.image:{image}, arch:{arch}, os_type:{os_type}"
            )
            tone_agent.deploy_agent_by_ecs_assistant(
                instance_id=cloud_server.instance_id,
                ip=cloud_private_ip,
                public_ip=cloud_server_ip,
                cloud_driver=cloud_driver,
                arch=arch,
                os_type=os_type
            )
        except Exception as error:
            logger.exception(
                f"Cloud server({cloud_server.instance_id}-{cloud_server_ip}) "
                f"deploy agent has exception, provider:{provider}. error: {error}.detail: {traceback.print_exc()}"
            )
            raise ExecStepException(error)

    @classmethod
    def _init_cloud(cls, meta_data):
        cloud_server = None
        job_id = meta_data[ServerFlowFields.JOB_ID]
        server_id = meta_data[ServerFlowFields.SERVER_ID]
        snapshot_cluster_id = meta_data.get(ServerFlowFields.SNAPSHOT_CLUSTER_ID, 0)
        snapshot_server_id = meta_data[ServerFlowFields.SERVER_SNAPSHOT_ID]
        cloud_inst_meta = meta_data[ServerFlowFields.CLOUD_INST_META]
        dag_step_id = meta_data[ServerFlowFields.DAG_STEP_ID]
        provider_info = ProviderInfo(**cloud_inst_meta)
        provider_info.name.replace(".", "")
        instance_id = cloud_inst_meta["instance_id"]
        cloud_driver = get_cloud_driver(provider_info)
        job_suite_id = meta_data.get(ServerFlowFields.JOB_SUITE_ID, 0)
        try:
            if not instance_id:
                cloud_server = cls._create_cloud_instance(
                    cloud_driver, provider_info, job_id, server_id, snapshot_server_id
                )
                snapshot_cloud_server = CloudServerSnapshot.get_by_id(snapshot_server_id)
                update_snapshot_fields = cloud_server.__data__.copy()
                update_snapshot_fields.pop("id")
                update_snapshot_fields["source_server_id"] = cloud_server.id
                Cs.update_snapshot_server(
                    snapshot_cloud_server,
                    **update_snapshot_fields
                )
            else:
                cloud_server = CloudServer.get(instance_id=instance_id)
            if not instance_id:
                cls._deploy_cloud_agent(cloud_server, cloud_driver)
            TestStep.create(
                job_id=job_id,
                state=ExecState.SUCCESS,
                stage=StepStage.INIT_CLOUD,
                job_suite_id=job_suite_id,
                cluster_id=snapshot_cluster_id,
                server=snapshot_server_id,
                dag_step_id=dag_step_id,
            )
        except ExecStepException as error:
            cls.release_instance(cloud_server)
            raise ExecStepException(error)
        return meta_data

    @classmethod
    def release_instance(cls, cloud_server):
        if cloud_server:
            try:
                logger.info(f"Release instance, cloud_server_data:{cloud_server.__data__}")
                instance_id = cloud_server.instance_id
                cloud_inst_meta = PoolCommonOperation.get_cloud_instance_meta_data(cloud_server)
                provider_info = ProviderInfo(**cloud_inst_meta)
                provider_info.name.replace(".", "")
                cloud_driver = get_cloud_driver(provider_info)
                try:
                    ToneAgentClient('remove').remove_agent(cloud_server.private_ip)
                except Exception as e:
                    error_msg = f"remove toneagent for {instance_id} has error: {str(e)}"
                    logger.error(error_msg)
                return cloud_driver.destroy_instance(instance_id)
            except Exception as error:
                error_msg = f"destroy instance for {instance_id} has error: {str(error)}"
                logger.error(error_msg)
                return False

    @classmethod
    def existed_instance(cls, cloud_server):
        if cloud_server:
            try:
                logger.info(f"Release instance, cloud_server_data:{cloud_server.__data__}")
                instance_id = cloud_server.instance_id
                cloud_inst_meta = PoolCommonOperation.get_cloud_instance_meta_data(cloud_server)
                provider_info = ProviderInfo(**cloud_inst_meta)
                provider_info.name.replace(".", "")
                cloud_driver = get_cloud_driver(provider_info)
                exist_instance = cloud_driver.show_instance([instance_id])
                if not exist_instance or len(exist_instance) == 0:
                    return False
                return True
            except Exception as error:
                error_msg = f"check instance for {instance_id} has error: {str(error)}"
                logger.error(error_msg)
                return False
        return False

    @classmethod
    @check_step_exists
    def _prepare(cls, meta_data):
        cls._cluster_ssh_free_login(meta_data, cls._ssh_free_login)
        channel_type = meta_data[ServerFlowFields.CHANNEL_TYPE]
        agent_script_obj = cls.get_agent_script_obj(channel_type)
        script_flag = agent_script_obj.PREPARE_DEBIAN if \
            meta_data[ServerFlowFields.SERVER_OS_TYPE] == 'debian' else agent_script_obj.PREPARE
        args = config.TONE_PATH + f" {meta_data[ServerFlowFields.SERVER_PROVIDER]}"
        logger.info(f'prepare step info:{agent_script_obj}|{args}|{meta_data}')
        success, result = cls._exec_spec_script(
            meta_data,
            script_flag=script_flag,
            args=args,
            timeout=config.PREPARE_TIMEOUT
        )
        return cls._update_step(meta_data, success, result)

    @classmethod
    def _get_latest_image_by_expression(cls, driver, instance_type, image_expression):
        """
        根据表达式 -> Anolis:Anolis OS  8.4 RHCK 64位:latest
        获取最新镜像 -> anolisos_8_4_x64_20G_rhck_alibase_20220518.vhd
        """
        platform = image_expression.split(':')[0]
        os_name = image_expression.split(':')[1]
        latest_image = ''
        images = driver.get_images(instance_type=instance_type)
        for image in images:
            if image['platform'] == platform and image['os_name'] == os_name:
                latest_image = image['id']
        logger.info(f'get latest image by expression:{image_expression}, result:{latest_image}')
        return latest_image