import datetime
import json
import time

from aliyunsdkcore.request import RpcRequest
from aliyunsdkecs.request.v20140526.DescribeImagesRequest import DescribeImagesRequest

from config import ECS_LOGIN_PASSWORD
from libcloud.common.exceptions import BaseHTTPError
from libcloud.common.types import LibcloudError
from libcloud.compute.base import NodeAuthPassword, NodeSize, NodeImage, Node
from libcloud.compute.drivers.ecs import ECSDriver as LibCloudECSDriver
from libcloud.compute.types import NodeState
from libcloud.utils.xml import findall, findtext
from lib.cloud.drivers.aliyun.aliyun_network import AliYunNetwork
from lib.cloud.drivers.aliyun.driver_lib import AliYunException
from lib.cloud.tools.aliyun_api_tools import AliCloudAPIRequest
from tools.misc import wait_until_socket_up
from tools.log_util import LoggerFactory
from models.server import CloudAk


logger = LoggerFactory.scheduler()


def get_object_items(obj, remove=None):
    if remove:
        return [n for n in dir(obj) if not n.startswith('_') and n not in remove]
    else:
        return [n for n in dir(obj) if not n.startswith('_')]


class EcsDriver(LibCloudECSDriver, RpcRequest):
    def __init__(self, access_id, access_key, region, zone, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.resource_group_id = resource_group_id
        self.region = region
        self.zone = zone
        super(EcsDriver, self).__init__(access_id, access_key, region=region)

    def show_regions(self, to_dict=False):
        location = self.list_locations()
        if to_dict:
            location = [{n: getattr(lc, n) for n in get_object_items(lc, ('driver',))} for lc in location]
            location = list(sorted(location, key=lambda x: (x.get('id', '') or x.get('name', ''))))
        return location

    list_regions = show_regions

    def show_zones(self, region_id=None, to_dict=False):
        zone = self.ex_list_zones(region_id)
        if to_dict:
            zone = [{n: getattr(z, n) for n in get_object_items(z, ('driver',))}
                    for z in zone]
            zone = list(sorted(zone, key=lambda x: (x.get('id', '') or x.get('name', ''))))
        return zone

    def show_sizes(self, to_dict=False):
        size = self.list_sizes()
        if to_dict:
            size = [{n: getattr(s, n) for n in get_object_items(s, ('driver', 'get_uuid'))}
                    for s in size]
            size = list(sorted(size, key=lambda x: (x.get('id', '') or x.get('name', ''))))
        return size

    def show_sizes_available(self, region_id, zone_id, ddh_id=None, ddh_type=None):
        if ddh_id:
            ddh_list = self.describe_dedicated_hosts(region_id, ddh_id)
            if ddh_list:
                ddh_type = ddh_list[0]['ddh_type']
                return self.describe_dedicated_host_instance_types(region_id, ddh_type, zone_id)
            else:
                return []
        elif ddh_type:
            return self.describe_dedicated_host_instance_types(region_id, ddh_type, zone_id)
        else:
            return self._show_sizes_available(region_id, zone_id)

    def _show_sizes_available(self, region_id, zone_id):
        sizes = self.list_available_resource(region_id, zone_id,
                                             destination_resource='InstanceType')
        sizes = [self._new_size(s) for s in sizes]
        sizes = list(sorted(sizes, key=lambda x: x['value']))
        return sizes

    def _new_size(self, element):
        status = findtext(element, 'Status', namespace=self.namespace)
        value = findtext(element, 'Value', namespace=self.namespace)
        # for more element, use _show_elements for help
        return {'status': status, 'value': value}

    def list_available_resource(
            self, region_id, zone_id=None,
            destination_resource='InstanceType',
    ):
        params = {'Action': 'DescribeAvailableResource',
                  'DestinationResource': destination_resource,
                  'RegionId': region_id}
        if zone_id:
            params['ZoneId'] = zone_id
        path = {'InstanceType': './/SupportedResource'}  # only support InstanceType now
        resp_body = self.connection.request(self.path, params).object
        resource_elements = findall(resp_body, path[destination_resource],
                                    namespace=self.namespace)
        return resource_elements

    def show_images(self, to_dict=False):
        image = self.list_images()
        if to_dict:
            image = [{n: getattr(i, n) for n in get_object_items(i, ('driver', 'get_uuid'))}
                     for i in image]
            image = list(sorted(image, key=lambda x: (x.get('id', '') or x.get('name', ''))))
        return image

    def show_images_platform(self):
        images = self.list_images()
        return [image.extra.get('platform') for image in images]

    def show_security_groups(self, to_dict=False):
        group = self.ex_list_security_groups()
        if to_dict:
            group = [{n: getattr(g, n) for n in get_object_items(g, ('driver',))}
                     for g in group]
            group = list(sorted(group, key=lambda x: x['id']))
        return group

    def show_instance(self, ex_node_ids=None, many='exist', to_dict=False):
        nodes = self.list_nodes(ex_node_ids)
        nodes_list = []
        if to_dict:
            remove = ('driver', 'get_uuid', 'destroy', 'reboot')
            nodes = [{n: getattr(no, n) for n in get_object_items(no, remove)}
                     for no in nodes]
            nodes = list(sorted(nodes, key=lambda x: x['id']))
            if many == 'exist':
                now = datetime.datetime.today()
                expired_format = '%Y-%m-%dT%H:%M%fZ'
                nodes = [
                    i for i in nodes if
                    datetime.datetime.strptime(i['extra']['expired_time'], expired_format) > now and
                    i['public_ips']
                ]
                for node in nodes:
                    if datetime.datetime.strptime(
                            node['extra']['expired_time'], expired_format) > now and \
                            node['public_ips']:
                        for un_serializable_item_key in ['stop_node', 'state', 'start']:
                            if node.get(un_serializable_item_key):
                                node.pop(un_serializable_item_key)
                        nodes_list.append(node)
        else:
            return nodes
        return nodes_list

    def stop_instance(self, instance_id, force_stop=True):
        node = Node(instance_id, instance_id, '', 0, 0, '', '', '')
        return self.ex_stop_node(node, ex_force_stop=force_stop)

    def destroy_instance(self, instance_id, timeout=300):
        _res = False
        node = Node(instance_id, instance_id, '', 0, 0, '', '', '')
        start_time = time.time()
        while time.time() - start_time < timeout:
            _res = self.destroy_node(node)
            if _res:
                break
        else:
            logger.error("ecs destroy instance timeout, instance_id is: {}".format(instance_id))
        return _res

    def start_instance(self, instance_id):
        return self.ex_start_node(instance_id)

    def describe_instance(self, instance_id):
        return self.list_nodes([instance_id])[0]

    def __get_or_create_sg_vs(self):
        net = AliYunNetwork(self.access_id, self.access_key, self.region, zone=self.zone,
                            resource_group_id=self.resource_group_id)
        sg_id, vs_id = net.get_or_create_default_sg_vs()
        return sg_id, vs_id

    def create_instance(self, name, image_id, instance_type,
                        system_disk_size=None,
                        system_disk_category='cloud_ssd',
                        data_disk_category='cloud_ssd',
                        data_disk_size=80,
                        data_disk_count=0,
                        login_password=ECS_LOGIN_PASSWORD,
                        internet_max_bandwidth=5,
                        ddh_id=None,
                        ddh_type=None,
                        extra_param=None,
                        **kwargs):

        ex_security_group_id, ex_vswitch_id = self.__get_or_create_sg_vs()

        image = NodeImage(id=image_id, name=image_id, driver=self)
        size = NodeSize(instance_type, instance_type, 0, 0, 0, 0, None)

        ex_internet_charge_type = self.internet_charge_types.BY_TRAFFIC
        ex_internet_max_bandwidth_out = ex_internet_max_bandwidth_in = internet_max_bandwidth
        system_disk_category = system_disk_category
        system_disk = {
            'category': system_disk_category,
            'disk_name': 'system_dist',
            'description': 'tone default system disk'}
        if system_disk_size:
            system_disk.update({'size': system_disk_size})

        data_disks = list()
        for i in range(int(data_disk_count)):
            data_disk = {
                'category': data_disk_category,
                'size': data_disk_size,
                'disk_name': 'data_disk_%s' % i,
                'delete_with_instance': True}
            data_disks.append(data_disk)
        try:
            auth = NodeAuthPassword(login_password)
            node = self.create_node(
                name=name, image=image, size=size,
                ex_security_group_id=ex_security_group_id,
                ex_vswitch_id=ex_vswitch_id,
                ex_internet_charge_type=ex_internet_charge_type,
                ex_internet_max_bandwidth_out=ex_internet_max_bandwidth_out,
                ex_internet_max_bandwidth_in=ex_internet_max_bandwidth_in,
                ex_system_disk=system_disk,
                ex_data_disks=data_disks,
                auth=auth,
                ddh_id=ddh_id,
                ddh_type=ddh_type,
                extra_param=extra_param,
                resource_group_id=self.resource_group_id,
                **kwargs)
            pub_ip = self.create_public_ip(node.id)
            is_up = wait_until_socket_up(pub_ip, 22, timeout=300, interval=0.5)
            if is_up:
                logger.info('ecs %s (ip:%s name: %s) is reachable ...' % (node.id, pub_ip, name))
            else:
                raise RuntimeError('ecs %s (ip:%s name: %s) is not up in 300s!' % (
                    node.id, pub_ip, name))
            return node.id
        except BaseHTTPError as e:
            logger.error('create ecs node failed, BaseHTTPError error: %s' % str(e))
            time.sleep(1.234)
            raise AliYunException(msg=e.message, code=e.code)

    def create_node(self, name, size, image, auth=None,
                    ex_security_group_id=None, ex_description=None,
                    ex_internet_charge_type=None,
                    ex_internet_max_bandwidth_out=None,
                    ex_internet_max_bandwidth_in=None,
                    ex_hostname=None, ex_io_optimized=None,
                    ex_system_disk=None, ex_data_disks=None,
                    ex_vswitch_id=None, ex_private_ip_address=None,
                    ex_client_token=None, ddh_id=None, ddh_type=None,
                    extra_param=None, resource_group_id=None, **kwargs):
        params = {'Action': 'CreateInstance',
                  'RegionId': self.region,
                  'ImageId': image.id,
                  'InstanceType': size.id,
                  'InstanceName': name,
                  'ResourceGroupId': resource_group_id}
        self.update_params(params, ex_security_group_id, ex_description,
                           ex_internet_charge_type, ex_internet_max_bandwidth_in,
                           ex_internet_max_bandwidth_out, ex_hostname, auth,
                           ex_io_optimized, ex_system_disk, ex_data_disks,
                           ex_vswitch_id, ex_private_ip_address, ex_client_token,
                           name, ddh_id, ddh_type, extra_param)
        resp = self.connection.request(self.path, params=params)
        nodes = self.get_list_nodes(resp)
        node = nodes[0]
        self.start_node(node)
        return node

    def get_list_nodes(self, resp):
        node_id = findtext(resp.object, xpath='InstanceId', namespace=self.namespace)
        nodes = list()
        cnt = 300
        while cnt > 0:
            nodes = self.list_nodes(ex_node_ids=[node_id])
            if len(nodes) == 1:
                break
            else:
                cnt -= 1
                time.sleep(0.1)
                if cnt == 0:
                    raise LibcloudError(f'could not find the new created node with id {node_id}. ',
                                        driver=self)
        return nodes

    def start_node(self, node):
        for _ in range(20):
            try:
                self._start_node(node)
            except BaseHTTPError as error:
                if 'IncorrectInstanceStatus' in str(error):
                    time.sleep(0.1)
                    continue
                raise error
            except LibcloudError as error:
                raise error
        self._wait_until_state([node], NodeState.RUNNING, 1, 300)

    def update_params(self, params, ex_security_group_id,
                      ex_description, ex_internet_charge_type,
                      ex_internet_max_bandwidth_in,
                      ex_internet_max_bandwidth_out,
                      ex_hostname, auth, ex_io_optimized,
                      ex_system_disk, ex_data_disks,
                      ex_vswitch_id, ex_private_ip_address,
                      ex_client_token, name, ddh_id, ddh_type,
                      extra_param):
        self.update_auth_params(params, ex_security_group_id, auth, ex_client_token)
        self.update_network_params(
            params, ex_internet_charge_type,
            ex_internet_max_bandwidth_in,
            ex_internet_max_bandwidth_out,
            ex_hostname, ex_vswitch_id,
            ex_private_ip_address
        )
        self.update_disk_params(params, ex_system_disk, ex_data_disks)
        self.update_ddh_params(params, name, ddh_id, ddh_type)
        self.update_other_params(params, ex_description, ex_io_optimized)
        self.update_extra_params(params, extra_param)

    def update_auth_params(self, params, ex_security_group_id, auth, ex_client_token):
        if not ex_security_group_id:
            raise AttributeError('ex_security_group_id is mandatory')
        params['SecurityGroupId'] = ex_security_group_id
        if auth:
            auth = self._get_and_check_auth(auth)
            params['Password'] = auth.password
        if ex_client_token:
            params['ClientToken'] = ex_client_token

    def update_network_params(self, params, ex_internet_charge_type,
                              ex_internet_max_bandwidth_in,
                              ex_internet_max_bandwidth_out,
                              ex_hostname, ex_vswitch_id,
                              ex_private_ip_address):
        inet_params = self._get_internet_related_params(
            ex_internet_charge_type,
            ex_internet_max_bandwidth_in,
            ex_internet_max_bandwidth_out)
        if inet_params:
            params.update(inet_params)
        if ex_hostname:
            params['HostName'] = ex_hostname
        if ex_vswitch_id:
            params['VSwitchId'] = ex_vswitch_id
        if ex_private_ip_address:
            if not ex_vswitch_id:
                raise AttributeError('must provide ex_private_ip_address and ex_vswitch_id at the same time')
            else:
                params['PrivateIpAddress'] = ex_private_ip_address

    def update_disk_params(self, params, ex_system_disk, ex_data_disks):
        if ex_system_disk:
            system_disk = self._get_system_disk(ex_system_disk)
            if system_disk:
                params.update(system_disk)

        if ex_data_disks:
            data_disks = self._get_data_disks(ex_data_disks)
            if data_disks:
                params.update(data_disks)

    def update_ddh_params(self, params, name, ddh_id, ddh_type):
        ddh_info = {}
        if ddh_id:
            params['DedicatedHostId'] = ddh_id
            ddh_list = self.describe_dedicated_hosts(self.region, ddh_id)
            ddh_info['ddh_id'], ddh_info['ddh_type'] = ddh_id, ddh_list[0]['ddh_type']
        elif ddh_type:
            ddh_info['ddh_type'] = ddh_type
            success, result = self.allocate_dedicated_hosts(ddh_type=ddh_type, name=name)
            if success:
                time.sleep(15)
                params['DedicatedHostId'] = ddh_info['ddh_id'] = result[0]
            else:
                raise AttributeError('create ddh failed: {}'.format(str(result)))

    def update_other_params(self, params, ex_description, ex_io_optimized):
        if ex_description:
            params['Description'] = ex_description
        if ex_io_optimized is not None:
            optimized = ex_io_optimized
            if isinstance(optimized, bool):
                optimized = 'optimized' if optimized else 'none'
            params['IoOptimized'] = optimized

    def update_extra_params(self, params, extra_param):
        # set extra param
        if extra_param and isinstance(extra_param, list):
            for param_item in extra_param:
                param_key = param_item.get('param_key').strip()
                param_value = param_item.get('param_value').strip()
                params[param_key] = param_value

    def _start_node(self, node):
        """
        Start node to running state.

        :param node: the ``Node`` object to start
        :type node: ``Node``

        :return: starting operation result.
        :rtype: ``bool``
        """
        resp_info = "\n\n"
        try:
            params = {'Action': 'StartInstance',
                      'InstanceId': node.id}
            resp = self.connection.request(self.path, params)
            resp_key = ["resp_status", "resp_request_id", "resp_error"]
            resp_value = [resp.status, resp.request_id, resp.error]
            resp_info += "\n\n".join(map(lambda k, v: "{}: {}".format(k, v), resp_key, resp_value))
            return resp.success() and \
                self._wait_until_state([node], NodeState.RUNNING, timeout=900)
        except LibcloudError as err:
            err.value += resp_info
            raise err

    def get_bandwidth(self, region_id):
        try:
            r = AliCloudAPIRequest(
                url='http://ecs.aliyuncs.com/',
                version='2014-05-26',
                access_id=self.access_id,
                access_key=self.access_key
            )
            s = json.loads(r.get('DescribeInstances', config={'RegionId': region_id}))
            return s.get('Instances').get('Instance')[0].get('InternetMaxBandwidthOut')
        except Exception as error:
            logger.error(error)
            return None

    def allocate_dedicated_hosts(self, ddh_type, region_id=None, zone_id=None, name=None):
        """
        :param region_id: 'cn-beijing'
        :return: ['dh-2ze3tzh7zcosaulmnd01']
        """
        region_id = self.region if region_id is None else region_id
        zone_id = self.zone if zone_id is None else zone_id
        if ddh_type is None:
            raise AttributeError('ddh_type is required')
        params = {
            'Action': 'AllocateDedicatedHosts',
            'RegionId': region_id,
            'ZoneId': zone_id,
            'DedicatedHostType': ddh_type,
        }
        if name:
            params['DedicatedHostName'] = name
        try:
            resp_object = self.connection.request(self.path, params).object
            ddh_elements = findall(resp_object, 'DedicatedHostIdSets/DedicatedHostId',
                                   namespace=self.namespace)
            return True, [el.text for el in ddh_elements]
        except BaseHTTPError as e:
            return False, json.loads(e.message.replace("'", '"')).get('message')

    def release_dedicated_hosts(self, ddh_id, region_id=None):
        """
        :param region_id: 'cn-beijing'
        :return: { "zone_id": "cn-beijing-f", "
        ddh_id": "dh-2ze3tzh7zcosaulmnd01", "ddh_type": "ddh.c5" }
        """
        region_id = self.region if region_id is None else region_id
        params = {
            'Action': 'ReleaseDedicatedHost',
            'RegionId': region_id,
            'DedicatedHostId': ddh_id
        }
        try:
            resp_object = self.connection.request(self.path, params).object
            req_el = findall(resp_object, 'RequestId', namespace=self.namespace)
            logger.info(req_el[0].text)
            return True, 'success'
        except Exception as e:
            return False, str(e)

    def describe_dedicated_hosts(self, region_id=None, ddh_id=None):
        """
        :param region_id: 'cn-beijing'
        :param ddh_id: ["dh- xxxxxxxxx", "dh- yyyyyyyyy", … "dh- zzzzzzzzz"]
        :return: {
            "zone_id": "cn-beijing-f",
            "ddh_id": "dh-2ze3tzh7zcosaulmnd01",
            "ddh_type": "ddh.c5"
        }
        """
        region_id = self.region if region_id is None else region_id
        params = {'Action': 'DescribeDedicatedHosts', 'RegionId': region_id}
        if ddh_id:
            params.update({'DedicatedHostIds': '["{}"]'.format(ddh_id)})

        resp_object = self.connection.request(self.path, params).object
        ddh_elements = findall(
            resp_object, 'DedicatedHosts/DedicatedHost', namespace=self.namespace)
        return [
            {
                'ddh_id': el.find('DedicatedHostId').text,
                'name': el.find('DedicatedHostName').text,
                'zone_id': el.find('ZoneId').text,
                'ddh_type': el.find('DedicatedHostType').text,
                'cpu': el.find('Capacity/TotalVcpus').text,
                'ddh_name': el.find('DedicatedHostName').text,
            }
            for el in ddh_elements
        ]

    def describe_dedicated_host_types(self, region_id, zone_id):
        data = []
        available_data = self._show_sizes_available(region_id, zone_id)
        available_data = [ad['value'] for ad in available_data if ad['status'] == 'Available']
        params = {'Action': 'DescribeDedicatedHostTypes', 'RegionId': region_id}
        resp_object = self.connection.request(self.path, params).object
        ddh_types = findall(
            resp_object,
            'DedicatedHostTypes/DedicatedHostType',
            namespace=self.namespace
        )
        for dt in ddh_types:
            ddh_type = dt.find('DedicatedHostType').text
            instance_types_list = dt.find('SupportedInstanceTypesList')
            for it in instance_types_list:
                if it.text in available_data:
                    data.append(ddh_type)
                    break
        return data

    def describe_dedicated_host_instance_types(self, region_id, ddh_type, zone_id):
        available_data = self._show_sizes_available(region_id, zone_id)
        available_data = [ad['value'] for ad in available_data if ad['status'] == 'Available']
        params = {
            'Action': 'DescribeDedicatedHostTypes',
            'RegionId': region_id,
            'DedicatedHostType': ddh_type
        }
        resp_object = self.connection.request(self.path, params).object
        instance_types = findall(
            resp_object,
            './/SupportedInstanceTypesList',
            namespace=self.namespace
        )
        instance_types = [el.text for el in instance_types]
        available_instance_types = [
            {'value': it, 'status': 'Available'}
            for it in instance_types if it in available_data
        ]
        return available_instance_types

    def list_regions_brief_info(self):
        params = {'Action': 'DescribeRegions'}
        resp_object = self.connection.request(self.path, params).object
        region_el = findall(resp_object, 'Regions/Region', namespace=self.namespace)
        return [el.find('RegionId').text for el in region_el]

    def list_instances_brief_info(self):
        params = {'Action': 'DescribeInstances', 'RegionId': self.region, 'PageSize': 100}
        resp_object = self.connection.request(self.path, params).object
        instance_el = findall(resp_object, 'Instances/Instance', namespace=self.namespace)
        return [
            {
                'instance_id': el.find('InstanceId').text,
                'name': el.find('InstanceName').text,
                'cpu': int(el.find('Cpu').text),
            } for el in instance_el
        ]

    def get_images(self, instance_type=None):
        try:
            request = DescribeImagesRequest()
            request.set_PageSize(100)
            if instance_type:
                request.set_InstanceType(instance_type)
            response = self.client.do_action_with_exception(request)
            images = json.loads(response)['Images']['Image']
            return [
                {
                    'id': item['ImageId'],
                    'name': item['ImageName'],
                    'platform': item['Platform'],
                    'os_name': item['OSName'],
                    'owner_alias': item['ImageOwnerAlias']
                } for item in images
            ]
        except Exception as e:
            logger.error('ecs sdk get_images failed: {}'.format(str(e)))
            return []
