import json
import os
import re
import time
import traceback
from aliyunsdkcore.acs_exception.exceptions import ServerException, ClientException
from aliyunsdkcore.client import AcsClient
from aliyunsdkeci.request.v20180808 import CreateContainerGroupRequest
from aliyunsdkeci.request.v20180808 import DeleteContainerGroupRequest
from aliyunsdkeci.request.v20180808 import DescribeContainerGroupsRequest
from lib.cloud.tools.aliyun_api_tools import AliCloudAPIRequest
from tools.log_util import LoggerFactory
from .driver_lib import ExceptionHandler, AliYunException
from .eci_conf import get_eci_conf


logger = LoggerFactory.scheduler()


class EciDriver(object):
    def __init__(self, access_id, access_key, region, zone, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.zone = zone
        self.resource_group_id = resource_group_id
        self.client = AcsClient(self.access_id, self.access_key, region_id=self.region)

    def show_regions(self):
        r = AliCloudAPIRequest(access_id=self.access_id, access_key=self.access_key)
        response = r.get('DescribeRegions')
        response = json.loads(response)
        return response['Regions']

    def show_zones(self, region_id=None):
        regions_info = list(filter(lambda item: item.get('RegionId') == region_id,
                                   self.show_regions()))[0]
        return [{'id': zone, 'name': zone} for zone in regions_info.get('Zones')]

    def show_instance(self, container_group_ids=None):
        nodes = self.list_nodes(container_group_ids)
        nodes = [n for n in nodes if n.get('InternetIp', None)]
        for node in nodes:
            node['id'] = node['ContainerGroupId']
            node['public_ips'] = [node['InternetIp']]
        nodes = list(sorted(nodes, key=lambda x: x['id']))
        return nodes

    def list_nodes(self, container_group_ids=None):
        try:
            request = DescribeContainerGroupsRequest.DescribeContainerGroupsRequest()
            if container_group_ids:
                if isinstance(container_group_ids, list):
                    request.set_ContainerGroupIds(json.dumps(container_group_ids))
                else:
                    raise AttributeError(
                        'container_group_ids should be a list of container group ids.')
            response = self.client.do_action_with_exception(request)
            response = json.loads(response)
            return response['ContainerGroups']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_instance(self, instance_id):
        return self.list_nodes([instance_id])[0]

    def destroy_instance(self, container_group_id, timeout=300):
        node = self.describe_instance(container_group_id)
        eip_addr = node['InternetIp']
        try:
            request = DeleteContainerGroupRequest.DeleteContainerGroupRequest()
            request.set_ContainerGroupId(container_group_id)
            response = self.client.do_action_with_exception(request)
            start_time = time.time()
            while time.time() - start_time < timeout:
                nodes = self.list_nodes([container_group_id])
                if len(nodes) == 0:
                    break
            else:
                logger.error("eci destroy instance timeout, container_group_id is: {}".format(
                    container_group_id))
            return response
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())
        finally:
            self.release_eip_address(eip_addr)

    def create_instance(self, name, image_id, instance_type,
                        volumes=None,
                        extra_param=None):
        container_name = os.path.basename(image_id).split(':')[0]
        container_name = re.sub(r'[\'\"\.\__\s\*\[\]\+\)\(:\?/]+', '', container_name.decode())
        result = re.findall(r'(\d+)C(\d+)G', instance_type.upper())
        if result:
            cpu_num, memory_size = result[0]
        else:
            raise AttributeError('eci container size wrong: %s' % instance_type)
        cpu_num = float(cpu_num)
        memory_size = float(memory_size)
        return self.create_node(
            container_group_name=name,
            container_name=container_name,
            image=image_id,
            cpu_num=cpu_num,
            memory_size=memory_size,
            volumes=volumes,
            extra_param=extra_param,
        )

    def create_node(self, container_group_name, container_name,
                    image, cpu_num, memory_size,
                    volumes=None,
                    extra_param=None):
        security_group_id, vswitch_id = self.get_or_create_default_sg_vs()
        request = CreateContainerGroupRequest.CreateContainerGroupRequest()
        try:
            return self._create_node(
                container_group_name, container_name, image, cpu_num, memory_size,
                volumes, security_group_id, extra_param, vswitch_id, request
            )
        except ServerException as e:
            time.sleep(1.234)
            ExceptionHandler.server_exception(e)
            logger.error('create eci node failed, ServerException error: %s' % str(e))
            logger.error(traceback.format_exc())
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            time.sleep(1.234)
            ExceptionHandler.client_exception(e)
            logger.error('create eci node failed, ClientException error: %s' % str(e))
            logger.error(traceback.format_exc())
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def _create_node(self, container_group_name, container_name, image,
                     cpu_num, memory_size, volumes, security_group_id,
                     extra_param, vswitch_id, request):
        request.set_SecurityGroupId(security_group_id)
        request.set_VSwitchId(vswitch_id)
        request.set_ContainerGroupName(container_group_name)
        eip_id = self.get_or_create_eip()
        request.set_EipInstanceId(eip_id)
        if ':' not in image:
            image = image + ':latest'
        org_name = image.split(':')[0]
        org_conf = get_eci_conf(org_name)
        container = {
            'Image': image,
            'Name': container_name,
            'Cpu': cpu_num,
            'Memory': memory_size,
            'WorkingDir': org_conf.workdir,
            'Args': org_conf.args,
            'ImagePullPolicy': 'IfNotPresent',
            'EnvironmentVars': org_conf.env,
            'Commands': org_conf.commands,
            'Ports': org_conf.ports,
            'VolumeMounts': org_conf.volume_mount,
        }
        containers = [container]
        request.set_RestartPolicy('Always')
        if volumes:
            # 安装volume到containerGroup
            request.set_Volumes(volumes)
        # set extra param
        if extra_param and isinstance(extra_param, list):
            for param_item in extra_param:
                param_key = param_item.get('param_key').strip()
                param_value = param_item.get('param_value').strip()
                request.add_query_param(param_key, param_value)
        request.set_Containers(containers)
        response = self.client.do_action_with_exception(request)
        container_group_id = json.loads(response)['ContainerGroupId']
        time.sleep(2)
        try:
            is_up = self._wait_until_state(container_group_id, 'Running', 1, 900)
        except AliYunException as e:
            raise e
        pub_ip = self.get_ip_by_eip_id(eip_id)
        if is_up:
            logger.info('eci %s (ip:%s name: %s) is reachable ...' %
                        (container_group_id, pub_ip, container_group_name))
        else:
            raise RuntimeError('eci server %s (ip:%s name: %s) is not up!' %
                               (container_group_id, pub_ip, container_group_name))
        return container_group_id

    def _wait_until_state(self, container_group_id, state, wait_period=3, timeout=600):
        start = time.time()
        end = start + timeout
        node_ids = [container_group_id]

        while time.time() < end:
            matched_nodes = self.list_nodes(node_ids)
            if len(matched_nodes) > len(node_ids):
                found_ids = [node.id for node in matched_nodes]
                msg = ('found multiple nodes with same ids, '
                       'desired ids: %(ids)s, found ids: %(found_ids)s' %
                       {'ids': node_ids, 'found_ids': found_ids})
                raise AliYunException(msg=msg, code='NodeNotFoundError')
            desired_nodes = [node for node in matched_nodes
                             if node['Status'] == state]

            if len(desired_nodes) == len(node_ids):
                return True
            else:
                time.sleep(wait_period)
                continue
        raise AliYunException(msg='Timed out after %s seconds' % timeout, code='Timeout')

    def list_regions_brief_info(self):
        return [item.get('RegionId') for item in self.show_regions()]

    def list_instances_brief_info(self):
        return [
            {
                'name': item['Name'], 'cpu': item['Cpu']
            } for groups in self.list_nodes() for item in groups['Containers']
        ]
