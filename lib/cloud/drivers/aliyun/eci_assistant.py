import json
import ssl
import time
import websocket
from aliyunsdkcore.client import AcsClient
from aliyunsdkeci.request.v20180808.ExecContainerCommandRequest import ExecContainerCommandRequest
from core.decorator import retry
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()


class EciAssistant(object):

    def __init__(self, access_key, secret_key, region, zone=None):
        self.region = region
        self.zone = zone
        self.client = AcsClient(access_key, secret_key, region)

    def exec_command(self, container_group_id, container_name, command, args=[],
                     timeout=60, is_sync=True):
        request = ExecContainerCommandRequest()
        if isinstance(command, list):
            cmd = command
        else:
            cmd = [command]
        if isinstance(args, list):
            cmd += args
        else:
            cmd.append(args)
        request.set_Command(cmd)
        request.set_ContainerGroupId(container_group_id)
        request.set_ContainerName(container_name)
        response = self.client.do_action_with_exception(request)
        jd = json.loads(response)
        ws_uri = jd['WebSocketUri']
        if is_sync:
            return self.query_command(ws_uri, timeout)
        return ws_uri

    def query_command(self, ws_uri, timeout):
        t_begin = time.time()
        ws = None
        result = ''
        try:
            ws = websocket.create_connection(ws_uri, timeout=timeout,
                                             sslopt={'cert_reqs': ssl.CERT_NONE})
            while time.time() - t_begin < timeout:
                if not ws.getstatus():
                    break
                tmp = ws.recv()
                if tmp:
                    result += tmp
                time.sleep(0.5)
        except websocket.WebSocketConnectionClosedException:
            logger.warn('ws socket(%s) is closed!' % ws_uri)
        except websocket.WebSocketTimeoutException:
            logger.warn('ws socket(%s) is timeout(%s)!' % (ws_uri, timeout))
        except websocket.WebSocketException as err:
            logger.warn('ws socket exception: %s' % str(err))
        finally:
            if ws and ws.connected:
                ws.shutdown()
                ws.close()
        return result

    @retry(5, 10)
    def deploy_agent(self, container_group_id, container_name, cmd):
        ret = self.exec_command(container_group_id, container_name, '/bin/sh',
                                ['-c', cmd],
                                30, True)
        logger.info('deploy agent for container_group: %s container_name: %s: ' % (
            container_group_id, container_name))
        logger.info(ret)
        if 'deploy succeed' in ret:
            return True
        else:
            raise RuntimeError('agent deploy failed in eci(%s/%s) details: %s'
                               % (container_group_id, container_name, ret))
