from .api import (
    QueryCacheData,
    CleanCacheData,
    QueryDagInfo,
    QueryJobResult,
    QueryLog,
    RunnerStatus,
    ReloadRunner,
    CleanDirtyDag,
    StopJob,
    ReleaseServer,
    CorrectData,
    CorrectJobCaseResult,
    QueryRedisInfo,
    TransRedisInfo,
    AdjustRedisInfo,
)


class RouteHandler:

    def __init__(self, api):
        self.api = api

    def maintain(self):
        self.api.add_resource(QueryCacheData, r"/query_cache")
        self.api.add_resource(CleanCacheData, r"/clean_cache")
        self.api.add_resource(QueryDagInfo, r"/query_dag_info")
        self.api.add_resource(QueryJobResult, r"/query_job_result")
        self.api.add_resource(QueryLog, r"/query_log")
        self.api.add_resource(RunnerStatus, r"/runner_status")
        self.api.add_resource(ReloadRunner, r"/reload_runner")
        self.api.add_resource(CleanDirtyDag, r"/clean_dirty_dag")
        self.api.add_resource(StopJob, r"/stop_job")
        self.api.add_resource(ReleaseServer, r"/release_server")
        self.api.add_resource(CorrectData, r"/correct_data")
        self.api.add_resource(CorrectJobCaseResult, r"/correct_job_case_result")
        self.api.add_resource(QueryRedisInfo, r"/redis_info")
        self.api.add_resource(TransRedisInfo, r"/trans_redis_info")
        self.api.add_resource(AdjustRedisInfo, r"/adjust_redis_info")

    def set_route(self):
        self.maintain()
