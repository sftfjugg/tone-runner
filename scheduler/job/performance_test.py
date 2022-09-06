import json

import requests
from constant import (
    TestType,
    PerfResultType,
    DbField,
    TestMetricType,
    TrackResult, ExecState
)
from models.job import TestJobCase
from models.result import PerfResult
from models.baseline import PerfBaselineDetail
from models.case import TestMetric
from tools.log_util import LoggerFactory
from .base_test import BaseTest


logger = LoggerFactory.job_result()


class PerformanceTest(BaseTest):

    TEST_TYPE = TestType.PERFORMANCE

    @classmethod
    def save_result(cls, step, result):
        job_id, step_id = step.job_id, step.id
        logger.info(f"job: {job_id} perf test save_result step: {step_id} result: {result}")
        test_job_case = TestJobCase.get_by_id(step.job_case_id)
        dag_step_id, sn = step.dag_step_id, step.server_sn
        test_suite_id = test_job_case.test_suite_id
        test_case_id = test_job_case.test_case_id
        repeat = test_job_case.repeat
        lines = result.splitlines()
        url = lines[0]
        idx = 0
        for index, ln in enumerate(lines):
            if ln.startswith('http'):
                url = ln
                break
            idx = index
        lines = lines[idx + 1:]
        for line in lines:
            if not line:
                continue
            if line != url:
                cls._save_result_file(job_id, test_suite_id, test_case_id, sn, url, line)
        cls._save_perf_data(job_id, step_id, dag_step_id, step, url, repeat)

    @classmethod
    def _save_perf_data(cls, job_id, step_id, dag_step_id, step, url, repeat):
        logger.info(f"job: {job_id} save_perf_data step: {step_id} url: {url}")
        test_job_case = TestJobCase.get_by_id(step.job_case_id)
        test_suite_id, test_case_id = test_job_case.test_suite_id, test_job_case.test_case_id
        cls._parse_data_by_statistic(job_id, step_id, dag_step_id, url, test_suite_id, test_case_id, repeat, step)

    @classmethod
    def _parse_data_by_statistic(cls, job_id, step_id, dag_step_id, url, test_suite_id, test_case_id, repeat, step):
        logger.info(f"parse data by statistic, job_id: {job_id}, step_id: {step_id}, url: {url}")
        try:
            statistic_url = url + 'statistic.json'
            response = requests.get(statistic_url)
            logger.info(f"request by avg, statistic_url:{statistic_url}, response: {response.text}")
            if response.ok:
                data = json.loads(response.text)
                if data["status"] == 'fail':
                    cls._set_step_state_fail(step)
                cls._save_data_to_db(job_id, step_id, dag_step_id, test_suite_id, test_case_id, data, repeat)
            else:
                cls._set_step_state_fail(step)
        except Exception as error:
            cls._set_step_state_fail(step)
            error = str(error)
            logger.error(f"job: {job_id} step: {step_id} save_perf_data calc avg.json error: {error} !!!")

    @classmethod
    def _set_step_state_fail(cls, step):
        step.state = ExecState.FAIL
        step.save()

    @classmethod
    def _save_data_to_db(cls, job_id, step_id, dag_step_id, test_suite_id, test_case_id, data, repeat):
        logger.info(
            f"save data to db, job_id: {job_id}, step_id: {step_id}, "
            f"test_suite_id: {test_suite_id}, test_case_id: {test_case_id}, data:{data}"
        )
        if "results" in data:
            results = data["results"]
            for result in results:
                metric = result["metric"]
                avg = result["average"]
                cv_value = result["cv"]
                value_list = result["matrix"]
                unit = result["unit"]
                cv_value = '±{0:.2f}%'.format(cv_value * 100)

                if PerfResult.filter(
                    test_job_id=job_id,
                    test_suite_id=test_suite_id,
                    test_case_id=test_case_id,
                    metric=metric,
                ).exists():
                    logger.error(f'_save_data_to_db re-parse result, job_id: {job_id}, '
                                 f'step_id: {step_id}, metric: {metric}')
                else:
                    logger.info(
                        f"save data to perf_result, job_id:{job_id}, "
                        f"test_suite_id:{test_suite_id}, test_case_id:{test_case_id}, "
                        f" metric:{metric}, test_value:{avg}, cv_value:{cv_value}, "
                        f"value_list:{value_list}, unit:{unit}"
                    )
                    PerfResult.create(
                        test_job_id=job_id,
                        test_suite_id=test_suite_id,
                        test_case_id=test_case_id,
                        metric=metric,
                        test_value=avg,
                        cv_value=cv_value,
                        value_list=value_list,
                        unit=unit,
                        max_value=max(value_list),
                        min_value=min(value_list),
                        repeat=repeat,
                        dag_step_id=dag_step_id
                    )

    @classmethod
    def _get_perf_result_type(cls, key, value, suite_name):
        perf_result_type = None
        if key.startswith(suite_name) and not (
                value.startswith('time') or
                key.startswith('perf-profile') or
                key.startswith('perf-stat') or
                key.startswith('vmstat')
        ):
            perf_result_type = PerfResultType.INDICATOR
        return perf_result_type

    @classmethod
    def compare_baseline(cls, dag_step):
        baseline_id = dag_step.baseline_id
        baseline_job_id = dag_step.baseline_job_id
        if not (baseline_id or baseline_job_id):
            return
        results_perf = PerfResult.filter(dag_step_id=dag_step.id)
        for result in results_perf:
            test_suite_id = result.test_suite_id
            test_case_id = result.test_case_id
            metric = result.metric
            if baseline_id:
                perf_baseline_detail = PerfBaselineDetail.filter(
                    baseline_id=baseline_id,
                    test_suite_id=test_suite_id,
                    test_case_id=test_case_id,
                    metric=metric
                )
                if perf_baseline_detail.exists():
                    cls._compare_baseline_to_result(
                        result, test_suite_id, test_case_id, perf_baseline_detail.first()
                    )
            else:
                perf_baseline_job = PerfResult.filter(
                    test_job_id=baseline_job_id,
                    test_suite_id=test_suite_id,
                    test_case_id=test_case_id,
                    metric=metric
                )
                if perf_baseline_job.exists():
                    cls._compare_baseline_to_result(
                        result, test_suite_id, test_case_id, perf_baseline_job.first(), baseline_type='job'
                    )

    @classmethod
    def _compare_baseline_to_result(cls, result, test_suite_id, test_case_id,
                                    perf_baseline_obj, baseline_type='baseline'):
        track_result = TrackResult.NA
        test_value = result.test_value
        baseline_value = perf_baseline_obj.test_value
        baseline_cv_value = perf_baseline_obj.cv_value
        test_value_, baseline_value_ = float(test_value), float(baseline_value)
        cv_value = float(result.cv_value.split("%")[0][1:])
        compare_result = (test_value_ - baseline_value_) / baseline_value_ if baseline_value_ else 0.00
        has_case_metric = TestMetric.filter(
            object_type=TestMetricType.CASE, object_id=test_case_id, name=result.metric
        ).first()
        if has_case_metric:
            track_result = cls._get_track_result(compare_result, has_case_metric, cv_value)
        else:
            has_suite_metric = TestMetric.filter(
                object_type=TestMetricType.SUITE, object_id=test_suite_id, name=result.metric
            ).first()
            if has_suite_metric:
                track_result = cls._get_track_result(compare_result, has_suite_metric, cv_value)
        result.match_baseline = DbField.TRUE
        if baseline_type == 'baseline':
            result.compare_baseline = perf_baseline_obj.baseline_id
        result.baseline_value = baseline_value
        result.baseline_cv_value = baseline_cv_value
        result.compare_result = compare_result
        result.track_result = track_result
        result.save()

    @classmethod
    def _get_track_result(cls, compare_result, test_metric, cv_value):
        cmp_threshold = test_metric.cmp_threshold
        direction = test_metric.direction
        cv_threshold = test_metric.cv_threshold
        if cv_value > cv_threshold * 100:
            track_result = TrackResult.INVALID
        else:
            if compare_result < -cmp_threshold:
                track_result = TrackResult.DECLINE if direction == TrackResult.INCREASE else TrackResult.INCREASE
            elif compare_result > cmp_threshold:
                track_result = TrackResult.INCREASE if direction == TrackResult.INCREASE else TrackResult.DECLINE
            else:
                track_result = TrackResult.NORMAL
        return track_result
