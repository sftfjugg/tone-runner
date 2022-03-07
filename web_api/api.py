import os
import time
import json
import pickle
import hashlib
import threading

import redis

import config
from flask_restful import Resource, reqparse
from core.cache.remote_source import RemoteFlowSource
from core.server.alibaba.alloc_server import AllocServer
from models.server import TestServer, CloudServer
from scheduler.job.check_job_step import CheckJobStep
from constant import TestType, ExecState, ServerProvider, RunMode, StepStage, ProcessDataSource
from models.dag import Dag, DagStepInstance
from models.job import TestStep, TestJob, TestJobSuite, TestJobCase
from models.result import FuncResult, PerfResult, ResultFile, ArchiveFile
from tools.config_parser import cp
from tools.redis_cache import RedisCache
from tools import utils


parser = reqparse.RequestParser()


class ApiCommon:
    api_secret_key = config.API_SECRET_KEY

    @staticmethod
    def now_timestamp():
        return int(time.time())

    @staticmethod
    def datetime_to_str(data):
        data["gmt_created"] = data["gmt_created"].strftime("%Y-%m-%d %H:%M:%S")
        data["gmt_modified"] = data["gmt_modified"].strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def convert_arg_to_int(arg, default=300):
        if arg:
            arg = int(arg)
        else:
            arg = default
        return arg

    def check_api_token(self, result, args):
        check_res = False
        api_token = args.get("api_token")
        timestamp = self.convert_arg_to_int(args.get("timestamp") or int(time.time()))
        if not timestamp:
            result["err_msg"] = "Not timestamp found!"
        elif not api_token:
            result["err_msg"] = "Not api token found!"
        elif self.now_timestamp() - timestamp > 300:
            result["err_msg"] = "request is expired!"
        else:
            encrypt_source = self.api_secret_key + str(timestamp)
            hash_obj = hashlib.md5()
            hash_obj.update(encrypt_source.encode())
            if hash_obj.hexdigest() == api_token:
                check_res = True
                result["result"] = "request is success!"
            else:
                result["err_msg"] = "Invalid api token!"
        return check_res


class QueryCacheData(Resource):
    source = RedisCache
    parser.add_argument("name")
    parser.add_argument("type")

    def get(self):
        args = parser.parse_args()
        name = args.get("name")
        que_type = args.get("type")
        database = self.source(config.REDIS_CACHE_DB)
        if not name:
            return "name is not empty!"
        if name == "all_keys":
            return database.keys("*")
        if que_type == "hash":
            return database.hgetall(name)
        if que_type == "sort_set":
            return database.zrange(name, 0, -1)
        if que_type == "set":
            return list(database.smembers(name))
        if que_type == "list":
            return database.lrange(name, 0, -1)
        return database.get(name)


class CleanCacheData(Resource):
    source = RedisCache
    parser.add_argument("name")
    parser.add_argument("type")
    parser.add_argument("key")

    def get(self):
        args = parser.parse_args()
        name = args.get("name")
        clean_type = args.get("type")
        clean_key = args.get("key")
        database = self.source(config.REDIS_CACHE_DB)
        if not name:
            return "name is not empty!"
        if clean_type == "del_all":
            return database.delete(name)
        if clean_type == "hash":
            return database.hdel(name, clean_key)
        if clean_type == "sort_set":
            return database.zrem(name, clean_key)
        if clean_type == "set":
            return database.srem(name, clean_key)
        return database.lrem(name, 1, clean_key)


class QueryDagInfo(Resource, ApiCommon):
    parser.add_argument("job_id")
    result = {"api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        self.result.clear()
        args = parser.parse_args()
        job_id = args.get("job_id")
        result = dict()
        db_dag = Dag.get_or_none(job_id=job_id)
        if db_dag:
            dag_inst = pickle.loads(db_dag.graph)
            result["id"] = db_dag.id
            result["graph"] = str(dag_inst.graph)
            result["kwargs"] = dag_inst.kwargs
            result["is_update"] = db_dag.is_update
            result["block"] = db_dag.block
            self.result["dag_info"] = str(result)
            dag_steps = DagStepInstance.filter(dag_id=db_dag.id)
            self.result["dag_step_info"] = []
            for ds in dag_steps:
                _ds_info = ds.__data__
                self.datetime_to_str(_ds_info)
                _ds_info["step_data"] = json.loads(_ds_info["step_data"])
                test_step = TestStep.get_or_none(dag_step_id=ds.id)
                if test_step:
                    ts_info = test_step.__data__
                    self.datetime_to_str(ts_info)
                    _ds_info["test_step_info"] = ts_info
                self.result["dag_step_info"].append(_ds_info)
        return self.result


class QueryJobResult(Resource, ApiCommon):
    parser.add_argument("job_id")
    parser.add_argument("flag")
    result = {"err_msg": "", "results": [], "api_server_ip": utils.get_ip_or_host_name()}

    def query_conf_result(self, test_job):
        if test_job.test_type == TestType.FUNCTIONAL:
            self.query_func_result(test_job.id)
        else:
            self.query_perf_result(test_job.id)

    def query_func_result(self, job_id):
        func_result = FuncResult.filter(test_job_id=job_id)
        for fr in func_result:
            fd = fr.__data__
            self.datetime_to_str(fd)
            self.result["results"].append(fd)

    def query_perf_result(self, job_id):
        perf_result = PerfResult.filter(test_job_id=job_id)
        for pr in perf_result:
            pd = pr.__data__
            self.datetime_to_str(pd)
            self.result["results"].append(pd)

    def query_result_file(self, job_id):
        result_file = ResultFile.filter(test_job_id=job_id)
        for rf in result_file:
            rd = rf.__data__
            self.datetime_to_str(rd)
            self.result["results"].append(rd)

    def query_archive_file(self, job_id):
        archive_file = ArchiveFile.filter(test_job_id=job_id)
        for af in archive_file:
            ad = af.__data__
            self.datetime_to_str(ad)
            self.result["results"].append(ad)

    def get(self):
        args = parser.parse_args()
        job_id = args.get("job_id")
        flag = args.get("flag")
        test_job = TestJob.get_or_none(id=job_id)
        if test_job:
            self.result["results"] = []
            if flag == "result":
                self.query_conf_result(test_job)
            elif flag == "file":
                self.query_result_file(job_id)
            elif flag == "archive":
                self.query_archive_file(job_id)
            else:
                self.result["err_msg"] = "Not support this flag!"
        return self.result


class QueryLog(Resource, ApiCommon):
    parser.add_argument("log_name")
    parser.add_argument("offset")
    parser.add_argument("cnt")
    parser.add_argument("app")
    parser.add_argument("search")
    max_offset = 10000  # 最大可查看的字节数
    max_line_cnt = 1000  # 最大可查看行数
    allow_logs = (
        "scheduler.log",
        "alloc_server.log",
        "release_server.log",
        "job_complete.log",
        "job_result.log",
        "exec_channel.log",
        "build_package.log",
        "query_package.log",
        "notice_msg.log",
        "check_machine.log",
        "tone-runner.log",
        "plan_complete.log",
        "dist_lock.log",
        "build_package.log",
        "query_package.log",
        "program_init.log",
        "storage.log",
        "queue.log",
        "summary_error.log"
    )

    @staticmethod
    def read_lines_by_spec(filename, read_from_offset, read_line_cnt):
        results = []
        with open(filename, "rb") as f:
            f.seek(read_from_offset, 0)
            while read_line_cnt > 0:
                results.append(f.readline().decode().replace("\r\n", ""))
                read_line_cnt -= 1
        return results

    @staticmethod
    def read_lines_by_tail(filename, offset):
        results = []
        with open(filename, "rb") as f:
            f.seek(-offset, 2)
            tmp_results = f.readlines()
            for result in tmp_results:
                results.append(result.decode().replace("\r\n", ""))
        return results

    @staticmethod
    def get_absolute_filename(log_name, app=None):
        if app:
            dir_name = os.path.dirname(config.LOG_PATH)
            filename = os.path.join(dir_name, f"app/{log_name}")
        else:
            filename = os.path.join(config.LOG_PATH, log_name)
        return filename

    def check_log_allow(self, log_name):
        for al in self.allow_logs:
            if al in log_name:
                return True
        return False

    @staticmethod
    def search_in_file(log_name, search):
        """返回匹配字符串位于文件中的字节位置"""
        command = f'grep -b -o "{search}" {log_name}'
        p = os.popen(command)
        return p.read()

    def get(self):
        args = parser.parse_args()
        flag = args.get("flag") or "tail"
        offset = self.convert_arg_to_int(args.get("offset"))
        read_line_cnt = self.convert_arg_to_int(args.get("cnt"))
        log_name = args.get("log_name")
        search = args.get("search")
        app = args.get("app")
        if not self.check_log_allow(log_name):
            return "This log has no permissions to view!"

        filename = self.get_absolute_filename(log_name, app)
        if not os.path.exists(filename):
            return "This log is not exists!"

        file_size = os.path.getsize(filename)
        if flag == "search":
            results = self.search_in_file(log_name, search)
            return {"file_size": file_size, "results": results, "api_server_ip": utils.get_ip_or_host_name()}
        if flag == "spec" and read_line_cnt > self.max_line_cnt:
            return "Read_line_cnt the maximum allowable value!"
        if offset > file_size or (flag == "tail" and offset > self.max_offset):
            return "Offset exceeds the maximum allowable value!"

        if flag == "spec":
            results = self.read_lines_by_spec(filename, offset, read_line_cnt)
        else:
            results = self.read_lines_by_tail(filename, offset)

        return {"file_size": file_size, "results": results, "api_server_ip": utils.get_ip_or_host_name()}


class RunnerStatus(Resource, ApiCommon):
    result = {"api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        command = "/usr/bin/supervisorctl status"
        p = os.popen(command)
        self.result["results"] = p.read()
        return self.result


class ReloadRunner(Resource, ApiCommon):
    parser.add_argument("op")
    result = {"api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        op = args.get("op")
        command = "/usr/bin/supervisorctl restart tone-runner:*"
        if op == "start":
            command = "/usr/bin/supervisorctl start tone-runner:*"
        if op == "stop":
            command = "/usr/bin/supervisorctl stop tone-runner:*"
        p = os.popen(command)
        self.result["results"] = p.read()
        return self.result


class CleanDirtyDag(Resource, ApiCommon):
    parser.add_argument("job_ids")
    result = {"api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        job_ids = args.get("job_ids")
        job_ids = job_ids.split(",")
        Dag.delete().where(Dag.job_id.in_(job_ids)).execute()
        self.result["results"] = "success!"
        return self.result


class StopJob(Resource, ApiCommon):
    parser.add_argument("job_ids")
    result = {"api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        job_ids = args.get("job_ids")
        job_ids = job_ids.split(",")
        TestJob.update(state=ExecState.STOP).where(TestJob.id.in_(job_ids)).execute()
        self.result["results"] = "success!"
        return self.result


class ReleaseServer(Resource, ApiCommon):
    parser.add_argument("job_id")
    parser.add_argument("run_mode")
    parser.add_argument("provider")
    parser.add_argument("object_ids")
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        job_id = args.get("job_id")
        run_mode = args.get("run_mode") or RunMode.STANDALONE
        provider = args.get("provider") or ServerProvider.ALI_GROUP
        object_ids = args.get("object_ids").split(",")
        for object_id in object_ids:
            AllocServer.release_server(job_id, object_id, run_mode, provider)
        return self.result


class CorrectData(Resource, ApiCommon):
    parser.add_argument("flag")
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        flag = args.get("flag") or "usually"
        if flag == "usually":
            RemoteFlowSource.correct_running_data()
        elif flag == "cache_clean":
            # 遇到redis缓存被清空的故障时使用，使用时最好将runner服务停掉，
            # 或者希望清空任务执行队列数据，重新初始化数据时使用。
            RemoteFlowSource.correct_data_by_cache_clean()
        else:
            self.result["results"] = f"Not support this flag: {flag}!"
        return self.result


class CorrectJobCaseResult(Resource, ApiCommon):
    parser.add_argument("job_ids")
    max_process_job_num = 3
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        job_ids = args.get("job_ids")
        job_ids = job_ids.split(",")
        if len(job_ids) > self.max_process_job_num:
            self.result["results"] = f"Process up to {self.max_process_job_num} jobs!"
            return self.result
        for job_id in job_ids:
            threading.Thread(
                target=self._correct_job_case_result_by_job,
                args=(job_id, ),
            ).start()
        return self.result

    @staticmethod
    def _correct_job_case_result_by_job(job_id):
        test_steps = []
        test_job = TestJob.get_by_id(job_id)
        test_type = test_job.test_type
        if test_type == TestType.FUNCTIONAL:
            result_model = FuncResult
        else:
            result_model = PerfResult
        if not result_model.filter(test_job_id=job_id).exists():
            test_steps = TestStep.filter(
                job_id=job_id,
                stage=StepStage.RUN_CASE,
            )
        for test_step in test_steps:
            CheckJobStep.check_step_with_ali_group(
                test_step,
                test_step.dag_step_id,
                test_step.tid,
                correct=True
            )


class QueryRedisInfo(Resource, ApiCommon):
    parser.add_argument("flag")
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        flag = args.get("flag")
        port = cp.get('redis_port')
        host = cp.get('redis_host')
        redis_password = cp.get("redis_password")
        pool = redis.ConnectionPool(host=host, port=port, password=redis_password)
        redis_obj = redis.Redis(connection_pool=pool)
        redis_info = redis_obj.info()
        if flag == 'memory':
            self.result['used_memory_human'] = redis_info.get('used_memory_human')
            self.result['used_memory_rss_human'] = redis_info.get('used_memory_rss_human')
            self.result['used_memory_peak_human'] = redis_info.get('used_memory_peak_human')
            self.result['maxmemory_human'] = redis_info.get('maxmemory_human')
            self.result['maxmemory_policy'] = redis_info.get('maxmemory_policy')
        else:
            self.result.update(redis_info)
        return self.result


class QueryDataServer:
    def query_data(self, job_id):
        res_data = dict()
        res_data.update(self.get_runner_status())
        res_data.update(self.query_redis_data())
        if job_id:
            res_data.update(self.get_job_data(job_id))
        return res_data

    @staticmethod
    def get_job_data(job_id):
        job_data = dict()
        test_job = TestJob.get_or_none(id=job_id)
        if test_job:
            job_data.update({
                'job_data': True,
                'job_id': test_job.id,
                'ws_id': test_job.ws_id,
                'state': test_job.state,
                'state_desc': test_job.state_desc,
            })
            suite_list = list()
            machine_set = set()
            server_provider = None
            for tmp_suite in TestJobSuite.filter(job_id=test_job.id):
                case_list = list()
                for tmp_case in TestJobCase.filter(job_id=test_job.id, test_suite_id=tmp_suite.test_suite_id):
                    if not server_provider:
                        server_provider = tmp_case.server_provider
                    case_list.append({
                        'case_id': tmp_case.id,
                        'server_provider': tmp_case.server_provider,
                        'server_object_id': tmp_case.server_object_id,
                        'server_tag_id': tmp_case.server_tag_id,
                        'server_snapshot_id': tmp_case.server_snapshot_id,
                        'state': tmp_case.state,
                    })
                    machine_set.add(tmp_case.server_object_id)
                suite_list.append({
                    'suite_id': tmp_suite.id,
                    'suite_state': tmp_suite.state,
                    'case_list': case_list,
                })
            job_data.update({'suite_list': suite_list})
            model_class = TestServer if server_provider == 'aligroup' else CloudServer
            machine_data = []
            for machine in machine_set:
                machine_obj = model_class.get_or_none(id=machine)
                if machine_obj:
                    machine_data.append({
                        'server_id': machine_obj.id,
                        'ip': machine_obj.ip if server_provider == 'aligroup' else machine_obj.pub_ip,
                        'state': machine_obj.state,
                        'occupied_job_id': machine_obj.occupied_job_id,
                    })
            job_data.update({'machine_data': machine_data})
            dag = Dag.get_or_none(job_id=job_id)
            if dag:
                dag_inst = pickle.loads(dag.graph)
                job_data.update({
                    'dag_id': dag.id,
                    'is_update': dag.is_update,
                    'block': dag.block,
                    'graph': str(dag_inst.graph),
                    'kwargs': dag_inst.kwargs,
                })
                dag_steps = DagStepInstance.filter(dag_id=dag.id)
                dag_step_info = list()
                for ds in dag_steps:
                    _ds_info = ds.__data__
                    _ds_info["step_data"] = json.loads(_ds_info["step_data"])
                    test_step = TestStep.get_or_none(dag_step_id=ds.id)
                    if test_step:
                        ts_info = test_step.__data__
                        _ds_info["test_step_info"] = ts_info
                    dag_step_info.append(_ds_info)
                job_data.update({'dag_step_info': dag_step_info})
        return job_data

    @staticmethod
    def get_runner_status():
        result = dict()
        command = "/usr/bin/supervisorctl status"
        p = os.popen(command)
        result["runner_status"] = p.read()
        return result

    @staticmethod
    def get_redis_status():
        redis_status = dict()
        port = cp.get('redis_port')
        host = cp.get('redis_host')
        redis_password = cp.get("redis_password")
        pool = redis.ConnectionPool(host=host, port=port, password=redis_password)
        redis_obj = redis.Redis(connection_pool=pool)
        redis_info = redis_obj.info()
        redis_status['used_memory_human'] = redis_info.get('used_memory_human')
        redis_status['used_memory_rss_human'] = redis_info.get('used_memory_rss_human')
        redis_status['used_memory_peak_human'] = redis_info.get('used_memory_peak_human')
        redis_status['maxmemory_human'] = redis_info.get('maxmemory_human')
        redis_status['maxmemory_policy'] = redis_info.get('maxmemory_policy')
        return redis_status

    def query_redis_data(self):
        redis_data = dict()
        # redis status
        redis_data.update(self.get_redis_status())
        redis_keys = self.query_redis('all_keys', add_name=False)
        set_keys = [key for key in redis_keys if key.startswith('tone-runner-server_use_freq_ws_')]
        ws_use_freq = []
        for queue_name in set_keys:
            ws_use_freq.append({
                'ws_id': queue_name.split('_')[-1],
                'data': self.query_redis(queue_name, que_type='sort_set', add_name=False)})
        redis_data.update({'ws_use_freq': ws_use_freq})
        # str
        for queue_name in ['last_plugin_version', 'last_test_server_id_with_real_state',
                           'last_cloud_server_id_with_real_state', 'master_fingerprint']:
            redis_data.update({queue_name: self.query_redis(queue_name)})
        # queue
        for queue_name in ['pending_job', 'running_job', 'pending_plan', 'running_plan',
                           'fast_pending_dag', 'fast_running_dag', 'slow_pending_dag', 'slow_running_dag',
                           'pending_dag_node', 'running_dag_node']:
            redis_data.update({queue_name: self.query_redis(queue_name, que_type='list')})
        # set
        for queue_name in ['scheduler_master', 'scheduler_slave']:
            redis_data.update({queue_name: self.query_redis(queue_name, que_type='set')})
        # sort_set
        for queue_name in ['server_use_freq']:
            redis_data.update({queue_name: self.query_redis(queue_name, que_type='sort_set')})
        # hash
        for queue_name in ['job_release_server', 'using_server', 'plan_server_lock',
                           'plan_inst_ctx', 'h_ip2sn', 'h_sn2ip']:
            redis_data.update({queue_name: self.query_redis(queue_name, que_type='hash')})
        return redis_data

    @staticmethod
    def query_redis(name, que_type='', add_name=True):
        database = RedisCache(config.REDIS_CACHE_DB)
        if not name:
            return "name is not empty!"
        if add_name and name not in ['last_plugin_version', 'h_ip2sn', 'h_sn2ip']:
            name = 'tone-runner-{}'.format(name)
        if name == "all_keys":
            return database.keys("*")
        if que_type == "hash":
            return database.hgetall(name)
        if que_type == "sort_set":
            return database.zrange(name, 0, -1)
        if que_type == "set":
            return list(database.smembers(name))
        if que_type == "list":
            return database.lrange(name, 0, -1)
        return database.get(name)


class TransRedisInfo(Resource, ApiCommon):
    parser.add_argument("host")
    parser.add_argument("password")
    parser.add_argument("port")
    parser.add_argument("db")
    parser.add_argument("flag")
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        # source redis
        args = parser.parse_args()
        host = args.get('host')
        port = cp.get('redis_port')
        redis_password = args.get('password')
        db = args.get("db")
        flag = args.get("flag", 0)
        conf_port = cp.get('redis_port')
        conf_host = cp.get('redis_host')
        conf_db = cp.get('cache_db')
        conf_redis_password = cp.get('redis_password')
        pool = redis.ConnectionPool(host=host, port=port, password=redis_password, db=db, decode_responses=True)
        source_redis_obj = redis.Redis(connection_pool=pool)
        modify_data = dict()
        # target redis
        pool = redis.ConnectionPool(host=conf_host, port=conf_port,
                                    password=conf_redis_password, db=conf_db, decode_responses=True)
        target_redis_obj = redis.Redis(connection_pool=pool)
        # running --> pending 不存在的dag_node 会根据dag 重新添加
        if flag:
            for tmp_queue in ['tone-runner-fast_running_dag', 'tone-runner-slow_running_dag',
                              'tone-runner-running_dag_node', 'tone-runner-running_job', 'tone-runner-running_plan']:
                running_to_pending = list(set(target_redis_obj.lrange(tmp_queue, 0, -1)))
                for name in running_to_pending:
                    with target_redis_obj.pipeline() as pip_line:
                        pip_line.lrem(tmp_queue, 1, name)
                        pip_line.lpush(tmp_queue.replace('running', 'pending'), name)
                        pip_line.execute()
                modify_data[f'{tmp_queue}_to_pending'] = running_to_pending

        # source --> target
        self.move_data(source_redis_obj, target_redis_obj, 'dag', modify_data, run_type='fast_')
        self.move_data(source_redis_obj, target_redis_obj, 'dag', modify_data, run_type='slow_')
        self.move_data(source_redis_obj, target_redis_obj, 'dag_node', modify_data)
        self.move_data(source_redis_obj, target_redis_obj, 'job', modify_data)
        self.move_data(source_redis_obj, target_redis_obj, 'plan', modify_data)
        self.result.update(**modify_data)
        return self.result

    @staticmethod
    def move_data(source_redis_obj, target_redis_obj, queue_type, modify_data, run_type=''):
        origin_pending = set()
        target_pending = set()
        for name in [f'tone-runner-{run_type}pending_{queue_type}', f'tone-runner-{run_type}running_{queue_type}']:
            origin_pending |= set(source_redis_obj.lrange(name, 0, -1))
            target_pending |= set(target_redis_obj.lrange(name, 0, -1))
        modify_list = list(origin_pending - target_pending)
        for tmp_queue in modify_list:
            target_redis_obj.lpush(f'tone-runner-{run_type}pending_{queue_type}', tmp_queue)
        modify_data[f'move-{run_type}pending_{queue_type}'] = modify_list


class AdjustRedisInfo(Resource, ApiCommon):
    source = RedisCache
    parser.add_argument("name")
    parser.add_argument("value")
    parser.add_argument("all_flag")
    parser.add_argument("push_key")
    parser.add_argument("push_value")
    parser.add_argument("rem_key")
    parser.add_argument("rem_value")
    parser.add_argument("hash_key")
    result = {"results": "success!", "api_server_ip": utils.get_ip_or_host_name()}

    def get(self):
        args = parser.parse_args()
        name = args.get('name')
        value = args.get('value')
        all_flag = args.get('all_flag', 0)
        push_key = args.get('push_key')
        push_value = args.get('push_value')
        rem_key = args.get('rem_key')
        rem_value = args.get('rem_value')
        hash_key = args.get('hash_key')
        database = self.source(config.REDIS_CACHE_DB)
        if hash_key:
            database.hdel(ProcessDataSource.USING_SERVER, hash_key)
            self.result.update({
                'hash_key': hash_key,
            })
        if push_key and push_value:
            database.lpush(push_key, push_value)
            self.result.update({
                'push_key': push_key,
                'push_value': push_value,
            })
        if rem_key and rem_value:
            database.lrem(rem_key, 1, rem_value)
            self.result.update({
                'rem_key': rem_key,
                'rem_value': rem_value,
            })
        if name and value:
            # running --> pending
            running_name = 'tone-runner-{}'.format(name)
            pending_name = running_name.replace('running', 'pending')
            with database.pipeline() as pipe:
                database.lrem(running_name, 1, value)
                database.lpush(pending_name, value)
                pipe.execute()
            self.result.update({
                'running': running_name,
                'pending': pending_name,
                'value': value,
            })
        if all_flag:
            modify_data = {}
            # running -- > pending
            for tmp_queue in ['tone-runner-fast_running_dag', 'tone-runner-slow_running_dag',
                              'tone-runner-running_dag_node', 'tone-runner-running_job', 'tone-runner-running_plan']:
                running_to_pending = list(set(database.lrange(tmp_queue, 0, -1)))
                for name in running_to_pending:
                    with database.pipeline() as pip_line:
                        pip_line.lrem(tmp_queue, 1, name)
                        pip_line.lpush(tmp_queue.replace('running', 'pending'), name)
                        pip_line.execute()
                modify_data[f'{tmp_queue}_to_pending'] = running_to_pending
            self.result.update({'modify_data': modify_data})
        return self.result
