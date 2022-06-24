import json
import config
from aliyunsdkcore.acs_exception.exceptions import ServerException, ClientException
from aliyunsdkcore.client import AcsClient
from aliyunsdkecs.request.v20140526 import AttachNetworkInterfaceRequest
from aliyunsdkecs.request.v20140526 import AuthorizeSecurityGroupRequest
from aliyunsdkecs.request.v20140526 import CreateNetworkInterfaceRequest
from aliyunsdkecs.request.v20140526 import CreateSecurityGroupRequest
from aliyunsdkecs.request.v20140526 import DescribeInstancesRequest
from aliyunsdkecs.request.v20140526 import DescribeNetworkInterfacesRequest
from aliyunsdkecs.request.v20140526 import DeleteNetworkInterfaceRequest
from aliyunsdkecs.request.v20140526 import DescribeSecurityGroupsRequest
from aliyunsdkvpc.request.v20160428 import CreateVSwitchRequest
from aliyunsdkvpc.request.v20160428 import CreateVpcRequest
from aliyunsdkvpc.request.v20160428 import DeleteVSwitchRequest
from aliyunsdkvpc.request.v20160428 import DeleteVpcRequest
from aliyunsdkecs.request.v20140526 import DeleteSecurityGroupRequest
from aliyunsdkvpc.request.v20160428 import DescribeVSwitchAttributesRequest
from aliyunsdkvpc.request.v20160428 import DescribeVSwitchesRequest
from aliyunsdkvpc.request.v20160428 import DescribeVpcAttributeRequest
from aliyunsdkvpc.request.v20160428 import DescribeVpcsRequest
from aliyunsdkvpc.request.v20160428 import AllocateEipAddressRequest
from aliyunsdkvpc.request.v20160428 import DescribeEipAddressesRequest
from aliyunsdkvpc.request.v20160428 import ReleaseEipAddressRequest
from aliyunsdkvpc.request.v20160428 import ModifyEipAddressAttributeRequest
from tools.log_util import LoggerFactory
from .driver_lib import CheckStatus, ExceptionHandler, AliYunException


logger = LoggerFactory.scheduler()
AVAILABLE = 'Available'
InUse = 'InUse'


class NetWorkControl(object):
    def __init__(self, access_id, access_key, region, zone=None, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.zone = zone
        self.resource_group_id = resource_group_id
        self.client = AcsClient(self.access_id, self.access_key, self.region)

    def create_vpc(self, vpc_name=config.DEFAULT_VPC_NAME, cidr_block=config.DEFAULT_VPC_CIDR_BLOCK):
        """
        create_vpc：创建VPC
        官网API参考链接:https://help.aliyun.com/document_detail/35737.html
        """
        try:
            request = CreateVpcRequest.CreateVpcRequest()
            request.set_CidrBlock(cidr_block)
            request.set_VpcName(vpc_name)
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            vpc_id = response_json['VpcId']
            # 判断VPC状态是否可用
            if CheckStatus.check_status(60, 2,
                                        self.describe_vpc_status,
                                        AVAILABLE, vpc_id):
                return vpc_id
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(
                msg=e.get_error_msg(), code=e.get_error_code(), request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def wait_vpc_reach_status(self, vpc_id, status=AVAILABLE, timeout=60, interval=1):
        if CheckStatus.check_status(timeout, interval, self.describe_vpc_status, status, vpc_id):
            return True
        raise AliYunException('Wait for vpc({}) reach ({}) timeout({}s)'.format(
            vpc_id, status, timeout))

    def delete_vpc(self, vpc_id):
        """
        delete_vpc: 删除VPC
        官网API参考链接: https://help.aliyun.com/document_detail/35738.html
        """
        try:
            request = DeleteVpcRequest.DeleteVpcRequest()
            # 要删除的VPC的ID
            request.set_VpcId(vpc_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vps(self, vpc_id=None):
        """
        list_vpc: 查询所有VPC
        官网API参考链接: https://help.aliyun.com/document_detail/35739.html
        """
        try:
            request = DescribeVpcsRequest.DescribeVpcsRequest()
            # 要删除的VPC的ID
            if vpc_id:
                request.set_VpcId(vpc_id)
                request.set_PageSize(100)
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json['Vpcs']['Vpc']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vpc_attribute(self, vpc_id):
        """
        describe_vpc_attribute: 查询指定地域已创建的vpc信息
        官网API参考: https://help.aliyun.com/document_detail/94565.html
        """
        try:
            request = DescribeVpcAttributeRequest.DescribeVpcAttributeRequest()
            request.set_VpcId(vpc_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vpc_status(self, vpc_id):
        """
        describe_vpc_status: 查询指定地域已创建的vpc的状态
        官网API参考: https://help.aliyun.com/document_detail/94565.html
        """
        response = self.describe_vpc_attribute(vpc_id)
        return response['Status']

    def create_vswitch(self, name, vpc_id, cidr_block):
        """
        create_vswitch: 创建vswitch实例
        官网API参考: https://help.aliyun.com/document_detail/35745.html
        """
        try:
            request = CreateVSwitchRequest.CreateVSwitchRequest()
            request.set_ZoneId(self.zone)
            request.set_VpcId(vpc_id)

            request.set_VSwitchName(name)
            request.set_CidrBlock(cidr_block)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            vswitch_id = response_json['VSwitchId']
            if CheckStatus.check_status(60, 2, self.describe_vswitch_status, AVAILABLE, vswitch_id):
                return vswitch_id
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vswitchs(self, vpc_id, vswitch_id=None):
        try:
            request = DescribeVSwitchesRequest.DescribeVSwitchesRequest()
            request.set_VpcId(vpc_id)
            if vswitch_id:
                request.set_VSwitchId(vswitch_id)
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json['VSwitches']['VSwitch']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vswitch_attribute(self, vswitch_id):
        """
        describe_vswitch_attribute: 查询指定地域已创建的vswitch的状态
        官网API参考: https://help.aliyun.com/document_detail/94567.html
        """
        try:
            request = DescribeVSwitchAttributesRequest.DescribeVSwitchAttributesRequest()
            request.set_VSwitchId(vswitch_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_vswitch_status(self, vswitch_id):
        """
        describe_vswitch_status: 查询指定地域已创建的vswitch的状态
        官网API参考: https://help.aliyun.com/document_detail/94567.html
        """
        response = self.describe_vswitch_attribute(vswitch_id)
        return response['Status']

    def delete_vswitch(self, vswitch_id):
        """
        delete_vswitch: 删除vswitch实例
        官网API参考: https://help.aliyun.com/document_detail/35746.html
        """
        try:
            request = DeleteVSwitchRequest.DeleteVSwitchRequest()
            request.set_VSwitchId(vswitch_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            if CheckStatus.check_status(15, 2,
                                        self.describe_vswitch_status,
                                        '', vswitch_id):
                return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def wait_vswitch_reach_status(self, vswitch_id, status=AVAILABLE, timeout=60, interval=1):
        if CheckStatus.check_status(timeout, interval, self.describe_vswitch_status,
                                    status, vswitch_id):
            return True
        raise AliYunException('Wait for vswitch({}) reach ({}) timeout({}s)'.format(
            vswitch_id, status, timeout))

    def describe_security_groups(self, vpc_id=None):
        count = 0
        is_first = True
        total_count = 0
        page_size = 50
        result = []
        while is_first or count < total_count:
            is_first = False
            request = DescribeSecurityGroupsRequest.DescribeSecurityGroupsRequest()
            if vpc_id:
                request.set_VpcId(vpc_id)
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            request.set_PageSize(page_size)
            try:
                response = self.client.do_action_with_exception(request)
                response = json.loads(response)
                groups = response['SecurityGroups']['SecurityGroup']
                result.extend(groups)
                count += len(groups)
                total_count = response['TotalCount']
            except ServerException as e:
                ExceptionHandler.server_exception(e)
                raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                      request_id=e.get_request_id())
            except ClientException as e:
                ExceptionHandler.client_exception(e)
                raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())
        return result

    def create_security_group(self, name, vpc_id):
        try:
            request = CreateSecurityGroupRequest.CreateSecurityGroupRequest()
            request.set_SecurityGroupName(name)
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            request.set_VpcId(vpc_id)
            request.set_Description(name)

            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            security_group_id = response_json['SecurityGroupId']
            return security_group_id
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def delete_security_group(self, security_group_id):
        try:
            request = DeleteSecurityGroupRequest.DeleteSecurityGroupRequest()
            request.set_SecurityGroupId(security_group_id)
            response = self.client.do_action_with_exception(request)
            return json.loads(response)
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def authorize_security_group_rule(self, sg_id, ip_protocol='all', port_range='-1/-1',
                                      source_cidr_ip='0.0.0.0/0', policy='accept', priority='1',
                                      nic_type='intranet'):
        try:
            request = AuthorizeSecurityGroupRequest.AuthorizeSecurityGroupRequest()
            request.set_SecurityGroupId(sg_id)
            request.set_IpProtocol(ip_protocol)
            request.set_PortRange(port_range)
            request.set_SourceCidrIp(source_cidr_ip)
            request.set_NicType(nic_type)
            request.set_Policy(policy)
            request.set_Priority(priority)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_instance(self, instance_id):
        request = DescribeInstancesRequest.DescribeInstancesRequest()
        request.set_InstanceIds(json.dumps([instance_id]))
        try:
            response = self.client.do_action_with_exception(request)
            response = json.loads(response)
            instances = response['Instances']['Instance']
            if instances:
                instance = instances[0]
                return instance
            raise AliYunException('{} failed mesage: {}'.format(request.get_action_name(),
                                                                response['Message']))
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def get_instance_vs_sg(self, instance_id):
        """
        获取交换机id及安全组id
        :param instance_id:
        :return:
        """
        instance = self.describe_instance(instance_id)
        if instance:
            return instance['VpcAttributes']['VSwitchId'], \
                   instance['SecurityGroupIds']['SecurityGroupId'][0]
        return False, None

    def create_network_interface(self, name, vswitch_id, security_group_id):
        request = CreateNetworkInterfaceRequest.CreateNetworkInterfaceRequest()
        request.set_NetworkInterfaceName(name)
        request.set_VSwitchId(vswitch_id)
        request.set_SecurityGroupId(security_group_id)
        if self.resource_group_id:
            request.set_ResourceGroupId(self.resource_group_id)
        try:
            response = self.client.do_action_with_exception(request)
            response = json.loads(response)
            return response['NetworkInterfaceId']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def describe_network_interface_status(self, eni_id):
        request = DescribeNetworkInterfacesRequest.DescribeNetworkInterfacesRequest()
        request.RegionId = self.region
        request.set_NetworkInterfaceIds(json.dumps([eni_id]))
        if self.resource_group_id:
            request.set_ResourceGroupId(self.resource_group_id)
        try:
            response = self.client.do_action_with_exception(request)
            response = json.loads(response)
            eni_list = response['NetworkInterfaceSets']['NetworkInterfaceSet']
            match_ids = filter(lambda eni: eni['NetworkInterfaceId'] == eni_id, eni_list)
            if match_ids:
                return match_ids[0]['Status']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def wait_network_interface_available(self, eni_id, timeout=120, interval=1):
        return self.wait_network_interface_reach_status(
            eni_id, status=AVAILABLE, timeout=timeout, interval=interval)

    def wait_network_interface_reach_status(self, eni_id, status=AVAILABLE, timeout=60, interval=1):
        if CheckStatus.check_status(
                timeout, interval, self.describe_network_interface_status, status, eni_id):
            return True
        raise AliYunException('Wait for eni({}) reach ({}) timeout({}s)'.format(
            eni_id, status, timeout))

    def attach_network_interface(self, eni_id, instance_id):
        try:
            request = AttachNetworkInterfaceRequest.AttachNetworkInterfaceRequest()
            request.set_NetworkInterfaceId(eni_id)
            request.set_InstanceId(instance_id)
            response = self.client.do_action_with_exception(request)
            return json.loads(response)
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def delete_network_interface(self, eni_id):
        try:
            request = DeleteNetworkInterfaceRequest.DeleteNetworkInterfaceRequest()
            request.set_NetworkInterfaceId(eni_id)
            response = self.client.do_action_with_exception(request)
            return json.loads(response)
        except ServerException as e:
            ExceptionHandler.server_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code(),
                                  request_id=e.get_request_id())
        except ClientException as e:
            ExceptionHandler.client_exception(e)
            raise AliYunException(msg=e.get_error_msg(), code=e.get_error_code())

    def allocate_eip_address(self):
        """
        allocate_eip_address: 申请弹性公网IP（EIP)
        官网API参考: https://help.aliyun.com/document_detail/36016.html
        """
        try:
            request = AllocateEipAddressRequest.AllocateEipAddressRequest()
            if self.resource_group_id:
                request.set_ResourceGroupId(self.resource_group_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            eip_id = response_json['AllocationId']
            if CheckStatus.check_status(120, 1, self.describe_eip_status, AVAILABLE, eip_id):
                return eip_id
            raise ServerException(
                400,
                'check eip_id {eip_id} status timeout for {timeout} seconds.'.format(
                    eip_id=eip_id, timeout=120)
            )
        except ServerException as e:
            ExceptionHandler.server_exception(e)
        except ClientException as e:
            ExceptionHandler.client_exception(e)

    def modify_eip_address(self, eip_id, name=None, bandwidth=None):
        """
        modify_eip_address: 修改指定EIP的名称、描述信息和带宽峰值
        官网API参考: https://help.aliyun.com/document_detail/36019.html
        """
        try:
            request = ModifyEipAddressAttributeRequest.ModifyEipAddressAttributeRequest()
            # 弹性公网IP的ID
            request.set_AllocationId(eip_id)
            # EIP的带宽峰值，单位为Mbps
            if bandwidth:
                request.set_Bandwidth(bandwidth)
            # EIP的名称
            if name:
                request.set_Name(name)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
        except ClientException as e:
            ExceptionHandler.client_exception(e)

    def describe_eip_address(self, eip_id=None, eip_addr=None):
        """
        describe_eip_status: 查询指定地域已创建的EIP。
        官网API参考: https://help.aliyun.com/document_detail/36018.html
        """
        try:
            request = DescribeEipAddressesRequest.DescribeEipAddressesRequest()
            request.set_PageSize(100)
            if eip_id:
                request.set_AllocationId(eip_id)
            if eip_addr:
                request.set_EipAddress(eip_addr)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json['EipAddresses']['EipAddress']
        except ServerException as e:
            ExceptionHandler.server_exception(e)
        except ClientException as e:
            ExceptionHandler.client_exception(e)

    def describe_eip_status(self, allocation_id):
        """
        describe_eip_status: 查询指定地域已创建的EIP的状态
        官网API参考: https://help.aliyun.com/document_detail/36018.html
        """
        # EIP的ID
        response = self.describe_eip_address(allocation_id)
        return response[0]['Status']

    def release_eip_address(self, eip_id):
        """
        release_eip_address: 释放指定的EIP。
        官网API参考: https://help.aliyun.com/document_detail/36020.html
        """
        try:
            request = ReleaseEipAddressRequest.ReleaseEipAddressRequest()
            # 要释放的弹性公网IP的ID
            request.set_AllocationId(eip_id)
            response = self.client.do_action_with_exception(request)
            response_json = json.loads(response)
            return response_json
        except ServerException as e:
            ExceptionHandler.server_exception(e)
        except ClientException as e:
            ExceptionHandler.client_exception(e)
