import time
import config
from tools.log_util import LoggerFactory
from tools.redis_cache import redis_cache
from .driver_lib import AliYunException
from .network_control import NetWorkControl


logger = LoggerFactory.scheduler()


class AliYunNetwork(object):
    def __init__(self, access_id, access_key, region, zone, resource_group_id=None):
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.zone = zone
        self.resource_group_id = resource_group_id
        self.net_ctl = NetWorkControl(self.access_id, self.access_key, self.region, self.zone, self.resource_group_id)

    def get_or_create_vpc(self, vpc_name=config.DEFAULT_VPC_NAME, cidr_block=config.DEFAULT_VPC_CIDR_BLOCK):
        vpcs = self.net_ctl.describe_vps()
        for vpc in vpcs:
            if vpc['VpcName'] == vpc_name:
                return vpc['VpcId'], vpc['CidrBlock']
        else:
            cidr = config.DEFAULT_VPC_CIDR_BLOCK
            vpc_id = self.net_ctl.create_vpc(vpc_name, cidr_block)
            return vpc_id, cidr

    def get_or_create_vswitch(self, vpc_id, vswitch_name=config.DEFAULT_VSWITCH_NAME):
        switches = self.net_ctl.describe_vswitchs(vpc_id)
        for switch in switches:
            if switch['VSwitchName'] == vswitch_name and switch['ZoneId'] == self.zone:
                return switch['VSwitchId']
        else:
            exists_cidr_list = [x['CidrBlock'] for x in switches]
            default_cidr = config.DEFAULT_VSWITCH_CIDR.split('/')
            sub_net = 0
            loop = 0
            # calc and create vswitch_id
            while sub_net < 255:
                default_cidr_ip = default_cidr[0].split('.')
                sub_jump = (24 - int(default_cidr[1]))
                sub_net = int(default_cidr_ip[2]) + (2 ** sub_jump) * loop
                loop += 1
                target_cidr_ip = default_cidr_ip[:2] + [str(sub_net)] + ['0']
                target_cidr = '.'.join(target_cidr_ip) + '/' + default_cidr[1]
                if target_cidr not in exists_cidr_list:
                    return self.net_ctl.create_vswitch(vswitch_name, vpc_id, target_cidr)
            raise AliYunException(
                '{} failed mesage: {}'.format('get_or_create_vswitch', 'no empty sub net can create'))

    def _get_security_group_id(self, vpc_id, cidr_block):
        sg_name = 'sg-tone-default-security_group'
        sg_list = self.net_ctl.describe_security_groups(vpc_id)
        for sg in sg_list:
            if sg['SecurityGroupName'] == sg_name:
                return sg['SecurityGroupId']
        sg_id = self.net_ctl.create_security_group(sg_name, vpc_id)
        self.setup_security_group_rule(sg_id, config.SECURITY_GROUP_DROP_CIDR_BLOCK_LIST, cidr_block)
        return sg_id

    def setup_security_group_rule(self, sg_id, drop_port_range_list, cidr_block):
        for cidr_ip in config.WORK_ACCESS_CIDR_BLOCK_LIST + [cidr_block]:
            self.net_ctl.authorize_security_group_rule(
                sg_id, 'all', '-1/-1', cidr_ip, 'accept', '1')
        for port_range in drop_port_range_list:
            self.net_ctl.authorize_security_group_rule(
                sg_id, 'tcp', port_range, '0.0.0.0/0', 'drop', '1')
            self.net_ctl.authorize_security_group_rule(
                sg_id, 'udp', port_range, '0.0.0.0/0', 'drop', '1')

    def get_or_create_default_sg_vs(self):
        vpc_id, cidr_block = self.get_or_create_vpc()
        self.net_ctl.wait_vpc_reach_status(vpc_id, timeout=30, interval=1)
        vswitch_id = self.get_or_create_vswitch(vpc_id)
        self.net_ctl.wait_vswitch_reach_status(vswitch_id, timeout=30, interval=1)
        sg_id = self._get_security_group_id(vpc_id, cidr_block)
        return sg_id, vswitch_id

    def attach_network_interface(self, instance_id):
        vsw_id, sg_id = self.net_ctl.get_instance_vs_sg(instance_id)
        eni_id = self.create_network_interface(instance_id, vsw_id, sg_id)
        if self.wait_network_interface_available(eni_id):
            return self.net_ctl.attach_network_interface(eni_id, instance_id)
        else:
            return False

    def create_network_interface(self, name_flag, vsw_id, sg_id):
        eni_name = 'eni-tone-' + name_flag
        eni_id = self.net_ctl.create_network_interface(eni_name, vsw_id, sg_id)
        return eni_id

    def delete_network_interface(self, eni_id):
        return self.net_ctl.delete_network_interface(eni_id)

    def get_or_create_eip(self):
        eip_list = self.net_ctl.describe_eip_address()
        for eip in eip_list:
            eip_id = eip['AllocationId']
            if (
                    eip and
                    eip['Status'] == 'Available' and
                    eip['RegionId'] == self.region and
                    eip['Name'] == config.DEFAULT_EIP_NAME and not
                    redis_cache.sismember('used_eip_ids', eip_id) and
                    redis_cache.sadd('used_eip_ids', eip_id)
            ):
                return eip_id
        return self.allocate_eip_address()

    def allocate_eip_address(self, name=config.DEFAULT_EIP_NAME):
        eip_id = self.net_ctl.allocate_eip_address()
        self.net_ctl.modify_eip_address(eip_id, name=name)
        if eip_id:
            redis_cache.sadd('used_eip_ids', eip_id)
        return eip_id

    def release_eip_id(self, eip_id):
        eips = self.net_ctl.describe_eip_address(eip_id)
        start_time = time.time()
        while time.time() - start_time < 30:
            if len(eips) == 1:
                eip = eips[0]
                if eip['Status'] == 'Available' and eip['InstanceId'] == '' and \
                        eip['RegionId'] == self.region:
                    break
            else:
                break
        return self.net_ctl.release_eip_address(eip_id)

    def release_eip_address(self, eip_addr):
        eip_id = self.get_id_by_eip_addr(eip_addr)
        redis_cache.srem('used_eip_ids', eip_id)
        return self.release_eip_id(eip_id)

    def get_ip_by_eip_id(self, eip_id):
        resp = self.net_ctl.describe_eip_address(eip_id)
        return resp[0]['IpAddress']

    def get_id_by_eip_addr(self, eip_addr):
        resp = self.net_ctl.describe_eip_address(eip_addr=eip_addr)
        return resp[0]['AllocationId']

    def wait_network_interface_available(self, eni_id):
        if self.net_ctl.wait_network_interface_available(eni_id, timeout=120, interval=1):
            return True
        else:
            return False

    def list_vswitchs(self, vpc_id):
        return self.net_ctl.describe_vswitchs(vpc_id)

    def delete_vswitch(self, vswitch_id):
        return self.net_ctl.delete_vswitch(vswitch_id)

    def delete_vpc(self, vpc_id):
        return self.net_ctl.delete_vpc(vpc_id)

    def delete_security_group(self, security_group_id):
        return self.net_ctl.delete_security_group(security_group_id)

    def list_security_groups(self, vpc_id=None):
        return self.net_ctl.describe_security_groups(vpc_id=None)

    def list_vpcs(self):
        return self.net_ctl.describe_vps()
