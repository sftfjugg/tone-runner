import unittest
import config
from constant import ToneAgentRes
from core.agent.tone_agent import SendTaskRequest, QueryTaskRequest, ToneAgentClient, do_exec, deploy_agent


class TestToneAgentRequest(unittest.TestCase):
    test_ip = config.TONE_AGENT_DOMAIN
    test_tid = "API_032f8442facf4187_runner-1"
    test_access_key = 'key_xxx'
    test_secret_key = 'secret_xxx'

    @unittest.skip("ignore")
    def test_agent_exec_by_request(self):
        res = self._tone_agent_exec_by_request()
        success = res["SUCCESS"]
        self.assertEqual(True, success)
        self.assertIn(ToneAgentRes.TID, res)

    # @unittest.skip("ignore")
    def test_agent_query_by_request(self):
        success, res = self._tone_agent_query_by_request(self.test_tid)
        self.assertEqual('', res["RESULT"]["ERROR_CODE"])

    @unittest.skip("ignore")
    def test_tone_agent_by_request(self):
        _, res = self._tone_agent_exec_by_request()
        success, tid = res["SUCCESS"], res["TID"]
        self.assertEqual(True, success)
        _, res = self._tone_agent_query_by_request(tid)
        self.assertEqual(None, res["ERROR_CODE"])

    @unittest.skip("ignore")
    def test_tone_agent_do_exec(self):
        test_cmd = "uptime"
        agent_client = ToneAgentClient()
        res = agent_client.do_exec(ip=self.test_ip, command=test_cmd, sync="true")
        print(res)

    @unittest.skip("ignore")
    def test_tone_agent_do_query(self):
        agent_client = ToneAgentClient("query")
        res = agent_client.do_query(tid=self.test_tid)
        print(res)

    @unittest.skip("ignore")
    def test_do_exec(self):
        test_cmd = "uptime"
        res = do_exec(ip=self.test_ip, command=test_cmd, sync="true")
        print(res)

    def test_deploy(self):
        res = deploy_agent(ip_list=['ip_xxx'])
        print(res)

    def _tone_agent_exec_by_request(self):
        test_script = """
        #!/bin/sh
        echo $1; echo $2; echo $A
        """
        request = SendTaskRequest(self.test_access_key, self.test_secret_key)
        request.set_ip(self.test_ip)
        request.set_script(test_script)
        request.set_script_type('shell')
        request.set_args('args1 args2')
        request.set_sync('false')
        request.set_cwd('/tmp')
        request.set_env('A=100')
        result = request.send_request()
        print(result)
        return result

    def _tone_agent_query_by_request(self, tid):
        request = QueryTaskRequest(self.test_access_key, self.test_secret_key)
        request.set_tid(tid)
        bret, res = request.send_request()
        print(bret, res)
        return bret, res


if __name__ == "__main__":
    unittest.main()
