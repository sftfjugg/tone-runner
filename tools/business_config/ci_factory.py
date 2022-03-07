# -*- coding: utf-8 -*-
import json
import re
import abc
import jenkins

from core.agent.star_agent import do_query, do_exec
from tools.log_util import LoggerFactory

logger = LoggerFactory.summary_error_log()


def get_ci_inst(kwargs):
    ci_type = kwargs.get('ci_type')
    if ci_type == 'jenkins':
        return JenkinsCI(**kwargs)
    elif ci_type == 'script':
        return ScriptCI(**kwargs)


class BaseCI(object):
    __metaclass__ = abc.ABCMeta
    """
    Aone case result:['fail','success','cancel'], status:['running','restarting','done']
    Jenkins job result:['SUCCESS','FAIL','ABORTED'] map to ['fail','success','cancel']
    """
    DEFAULT_EMPID = '135466'
    CI_PASS = 'success'
    CI_FAIL = 'fail'
    CI_CANCEL = 'stop'

    @abc.abstractmethod
    def start(self, *args, **kwargs):
        """start CI project"""

    @abc.abstractmethod
    def query(self, *args, **kwargs):
        """query build result"""

    @abc.abstractmethod
    def stop(self, *args, **kwargs):
        """stop build"""

    def convert_result(self, result):
        if not isinstance(result, bool):
            result = result.lower()
        if result in ('success', 'pass', True):
            common_result = self.CI_PASS
        elif result in ('fail', 'failure', False):
            common_result = self.CI_FAIL
        elif result in ('cancel', 'aborted'):
            common_result = self.CI_CANCEL
        else:
            common_result = result
        return common_result


class JenkinsCI(BaseCI):
    def __init__(self, *_, **kwargs):
        self.user = kwargs.get('user', None)
        self.token = kwargs.get('token', None)
        self.host = kwargs.get('host', None)
        self.job_name = kwargs.get('job_name', None)
        self.server = jenkins.Jenkins(self.host, username=self.user, password=self.token)

    def start(self, *args, **kwargs):
        result = dict()
        is_success = True
        job_info = self.server.get_job_info(self.job_name)
        logger.info(job_info)
        concurrent_build = job_info['concurrentBuild']
        logger.info('concurrentBuild: %s' % concurrent_build)
        try:
            last_build_number = job_info['lastBuild']['number']
            logger.info('last_build_number is %s' % last_build_number)
            build_info = self.server.get_build_info(self.job_name, last_build_number, 1)
            is_building = build_info.get('building')
        except Exception as err:
            logger.info('start build jenkins job failed ... %s' % str(err))
            last_build_number = 0
            is_building = False
        queue_info = self.server.get_queue_info()
        build_id = None
        queue_id = None
        params = kwargs.get('params', None)
        if params:
            params = json.loads(params)
        logger.info('is_building: %s queue_info: %s' % (is_building, queue_info))
        if is_building is False or (is_building is True and not queue_info) or concurrent_build:
            self.server.build_job(self.job_name, token=self.token, parameters=params)
            build_id = last_build_number + 1
            if is_building is True:
                try:
                    queue_id = self.server.get_queue_info()[0].get('id')
                except Exception as err:
                    logger.warn('get queue info failed ! maybe a multi configuration job? error: %s ' % err)
                result['status'] = 'pending'
            else:
                result['status'] = 'running'
            result['message'] = 'success'
            logger.info('jenkins %s build job start success... build_id is %s' % (self.job_name, build_id))
        else:
            is_success = False
            result['message'] = 'queue is not empty, trigger failed.'
            result['status'] = 'fail'
            logger.warn('trigger jenkins job build for %s failed, cannot trigger new when queue is not empty!'
                        % self.job_name)
        if not build_id:
            raise RuntimeError('trigger jenkins job build for %s failed !!! no build_id returned !' % build_id)
        result['build_id'] = build_id
        result['success'] = is_success
        result['queue_id'] = queue_id
        result['ci_system'] = 'jenkins'
        result['ci_project'] = self.host + '/job/' + self.job_name
        result['build_url'] = '%s/%s' % (result['ci_project'], result['build_id'])
        logger.info('jenkins job start result: %s' % json.dumps(result))
        return result

    def stop(self, *args, **kwargs):
        build_id = kwargs.get('build_id')
        queue_id = kwargs.get('queue_id', None)
        try:
            queue_info = self.server.get_queue_info()
            if queue_info:
                for info in queue_info:
                    if info.get('id') == queue_id:
                        self.server.cancel_queue(queue_id)
                        logger.warn('queue is cancelled')
                        return True

            build_info = self.server.get_build_info(self.job_name, build_id)
            is_building = build_info.get('building')
            if is_building:
                self.server.stop_build(self.job_name, build_id)
                logger.warn('build %s is cancelled' % build_id)
            else:
                logger.warn('Build %s is already finished, no need to cancel' % build_id)
            return True
        except Exception as e:
            logger.error('error info is %s' % e)
            return False

    def query(self, *args, **kwargs):
        build_id = kwargs.get('build_id')
        queue_id = kwargs.get('queue_id', None)
        result_data = dict()
        result_data['ci_system'] = 'jenkins'
        result_data['build_id'] = build_id
        result_data['ci_project'] = self.host + '/job/' + self.job_name
        queue_info = self.server.get_queue_info()
        if queue_info and any(d['id'] == queue_id for d in queue_info):
            result_data['status'] = 'pending'
            result_data['build_url'] = self.host + 'job/' + self.job_name
        else:
            result_data['build_url'] = self.host + '/job/' + self.job_name + '/' + str(build_id)
            build_info = self.server.get_build_info(self.job_name, build_id)
            is_building = build_info.get('building')
            if is_building is False:
                result = build_info.get('result').lower()
                result_data['status'] = 'done'
                result_data['ci_result'] = self.convert_result(result)
                phase_data = []
                phase = dict()
                phase['job_name'] = self.job_name
                phase['log'] = []
                phase['result'] = result_data.get('ci_result')
                job_console_log = self.server.get_build_console_output(self.job_name, build_id)
                if job_console_log:
                    try:
                        for item in job_console_log.strip().split('\n'):
                            report_log = re.findall(r'TEST_REPORT(\s*:\s*|\s*=\s*)(.*)', item)
                            if report_log:
                                phase['log'].append(report_log[0][1])
                                phase_data.append(phase)
                                break
                    except Exception as e:
                        logger.error('parse job_console_log failed, error is %s' % str(e))
                result_data['details'] = phase_data
            else:
                result_data['status'] = 'running'
        logger.info('jenkins %s job: %s build: %s result data is %s' %
                    (self.host, self.job_name, build_id, result_data))
        return result_data


class ScriptCI(BaseCI):
    def __init__(self, *_, **kwargs):
        self.host = kwargs.get('host', None)
        self.script = kwargs.get('params', None)
        self.timeout = kwargs.get('timeout', 10800)

    def start(self, *args, **kwargs):
        result = dict()
        success, resp = do_exec(ip_list=[self.host], command=self.script, timeout=self.timeout)
        if success:
            result['build_id'] = resp[0]['UID']
            result['build_url'] = resp[0].get('IP') if resp[0].get('IP') else resp[0]['SERVICETAG']
        else:
            result['build_id'] = None
            result['build_url'] = None
            result['message'] = resp
        result['success'] = success
        result['ci_system'] = 'script'
        logger.info('custom script job start result: %s' % json.dumps(result))
        return result

    def stop(self, *args, **kwargs):
        raise NotImplementedError

    def query(self, *args, **kwargs):
        build_id = kwargs.get('build_id')
        result_data = dict()
        result_data['ci_system'] = 'script'
        result_data['build_id'] = build_id
        success, resp = do_query(uid_list=[build_id])
        if success:
            for data in resp:
                uid = data['UID']
                if uid == '':
                    continue
                status = data['STATUS']
                success = data['SUCCESS']
                job_name = data['JOBNAME']
                if status == 'finish':
                    result_data['status'] = 'done'
                    result_data['ci_project'] = job_name
                    result_data['ci_system'] = 'script'
                else:
                    result_data['status'] = 'running'
                    result_data['ci_project'] = job_name
                    result_data['ci_system'] = 'script'
                result_data['job_data'] = json.dumps(data)
                result_data['ci_result'] = self.convert_result(success)
        else:
            result_data['status'] = 'running'
            result_data['ci_system'] = 'script'
        logger.info('custom script %s build: %s result data is %s' % (self.host, build_id, result_data))
        return result_data

