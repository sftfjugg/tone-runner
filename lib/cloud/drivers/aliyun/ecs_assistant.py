import base64
import json
import os
import time
from aliyunsdkcore.client import AcsClient
from aliyunsdkecs.request.v20140526.CreateCommandRequest import CreateCommandRequest
from aliyunsdkecs.request.v20140526.DeleteCommandRequest import DeleteCommandRequest
from aliyunsdkecs.request.v20140526.DescribeInvocationResultsRequest import \
    DescribeInvocationResultsRequest
from aliyunsdkecs.request.v20140526.InvokeCommandRequest import InvokeCommandRequest
from aliyunsdkecs.request.v20140526.StopInvocationRequest import StopInvocationRequest
from aliyunsdkecs.request.v20140526.RunCommandRequest import RunCommandRequest

from constant import ToneAgentScriptTone
from core.decorator import retry
from models.sysconfig import BaseConfig
from tools.config_parser import cp
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


class EcsAssistant(object):

    def __init__(self, access_key, secret_key, region, zone, resource_group_id=None):
        self.region = region
        self.zone = zone
        self.resource_group_id = resource_group_id
        self.client = AcsClient(access_key, secret_key, region)

    def __send_request(self, request):
        request.set_accept_format('json')
        result = self.client.do_action_with_exception(acs_request=request)
        return json.loads(result)

    def __create_command(self, cmd, name, work_dir, timeout=10, cmd_type='RunShellScript'):
        request = CreateCommandRequest()
        request.set_accept_format('json')
        request.set_CommandContent(cmd)
        request.set_Type(cmd_type)
        request.set_Name(name)
        request.set_Timeout(timeout)
        request.set_WorkingDir(work_dir)
        request.set_accept_format('json')
        response = self.__send_request(request)
        command_id = response.get('CommandId')
        return command_id

    def __run_command(self, instance_ids, cmd, work_dir, timeout=10, cmd_type='RunShellScript'):
        # doc: https://help.aliyun.com/document_detail/141751.html
        request = RunCommandRequest()
        request.set_accept_format('json')
        request.set_CommandContent(cmd)
        request.set_Type(cmd_type)
        request.set_Timeout(timeout)
        request.set_WorkingDir(work_dir)
        request.set_accept_format('json')
        request.set_InstanceIds(instance_ids)
        response = self.__send_request(request)
        command_id = response.get('CommandId')
        return command_id

    def __invoke_command(self, instance_id, command_id):
        request = InvokeCommandRequest()
        instance_id = [instance_id]
        request.set_InstanceIds(instance_id)
        request.set_CommandId(command_id)
        request.set_accept_format('json')
        response = self.__send_request(request)
        event_id = response.get('InvokeId')
        return event_id

    def __delete_command(self, command_id):
        request = DeleteCommandRequest()
        request.set_CommandId(command_id)
        request.set_accept_format('json')
        response = self.__send_request(request)
        return response

    def __query_command(self, instance_id, event_id):
        request = DescribeInvocationResultsRequest()
        request.set_InstanceId(instance_id)
        request.set_InvokeId(event_id)
        try:
            response = self.__send_request(request)
            if response is not None:
                result_list = response.get('Invocation').get(
                    'InvocationResults').get('InvocationResult')
                for result in result_list:
                    if result and result['InvokeId'] == event_id and result['InvokeRecordStatus']:
                        return True, result
            return False, None
        except Exception as err:
            logger.info('ecs query command error ! %s' % err)
            return False, str(err)

    def __stop_command(self, instance_id, event_id):
        request = StopInvocationRequest()
        request.set_InstanceIds([instance_id])
        request.set_InvokeId(event_id)
        return self.__send_request(request)

    def stop_command(self, instance_id, event_id):
        return self.__stop_command(instance_id, event_id)

    def query_command(self, instance_id, event_id, timeout=10):
        t_begin = time.time()
        while time.time() - t_begin <= timeout:
            status, result = self.__query_command(instance_id, event_id)
            if not status:
                continue
            if result['InvokeRecordStatus'].lower() == 'finished':
                return base64.b64decode(result['Output'])
            elif result['InvokeRecordStatus'].lower() == 'failed':
                raise RuntimeError(result)
            else:
                continue

    def exec_command(self, instance_id, command, workdir='/root', timeout=10, sync=True):
        command = base64.b64encode(command)
        cmd_id = None
        try:
            cmd_id = self.__create_command(command, 'command', workdir, timeout)
            event_id = self.__invoke_command(instance_id, cmd_id)
            if sync:
                return self.query_command(instance_id, event_id)
            return event_id
        except Exception as err:
            logger.warn('run command with err: %s' % err)
            raise err
        finally:
            if cmd_id:
                try:
                    resp = self.__delete_command(cmd_id)
                    logger.debug('delete command: %s' % resp)
                except Exception as error:
                    logger.error(error)

    def run_command(self, instance_id, command, workdir='/root', timeout=10):
        return self.__run_command([instance_id], command, workdir, timeout=timeout)

    @retry(5, 10)
    def deploy_agent(self, instance_id, tsn, rpm_link, os_type):
        if os_type == 'debian':
            deploy_agent_script_name = ToneAgentScriptTone.DEPLOY_AGENT_DEBIAN
        else:
            deploy_agent_script_name = ToneAgentScriptTone.DEPLOY_AGENT
        toneagent_deploy_script = BaseConfig.get(
            config_type="script",
            config_key=deploy_agent_script_name
        ).config_value
        command = toneagent_deploy_script.format(
            rpm_url=rpm_link,
            tsn=tsn,
            mode='active',
            proxy=cp.get('toneagent_outside_domain')
        ).encode()
        ret = self.run_command(instance_id, command, '/tmp', timeout=60)
        logger.info('deploy agent in instance: %s ret: %s' % (instance_id, ret))
        if ret and 'toneagent deploy success' in ret:
            return True, 'success'
        else:
            return False, 'toneagent deploy failed in ecs instance: %s details: %s' % (instance_id, ret)
