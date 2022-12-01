import time
import config
import base64
import requests
import urllib.parse as urlparse
from core.exception import CreateJobArgsException
from tools.log_util import LoggerFactory


logger = LoggerFactory.scheduler()
summary_logger = LoggerFactory.summary_error_log()


class CreateJob:
    tone_home = config.TONE_DOMAIN
    create_job_api = urlparse.urljoin(tone_home, "/api/job/create/")
    REQUIRE_REQUEST_FIELDS = ("username", "token", "template_id", "project_id")

    def __init__(self, **request_data):
        self.request_data = request_data
        self.plan_inst_id = request_data.get("plan_inst_id")

    def validate_request_data(self):
        for rf in self.REQUIRE_REQUEST_FIELDS:
            if rf not in self.request_data:
                raise CreateJobArgsException(f"{rf} must be required!")

    def set_token(self):
        username = self.request_data["username"]
        token = self.request_data["token"]
        token = username + '|' + token + '|' + str(time.time())
        signature = base64.b64encode(token.encode('utf-8')).decode('utf-8')
        self.request_data.update({"signature": signature})

    def create(self):
        success, job_id, err_msg = False, None, None
        try:
            self.validate_request_data()
            self.set_token()
            res = requests.post(
                url=self.create_job_api,
                json=self.request_data,
                verify=False
            )
            if res.status_code == 200:
                res = res.json()
                if res["code"] == 200:
                    success = True
                    job_id = res["data"]["job_id"]
                else:
                    err_msg = res["msg"]
            else:
                err_msg = res.text
        except Exception as error:
            logger.exception(error)
            summary_logger.exception(error)
            err_msg = str(error)
        if self.plan_inst_id:
            logger.info(f"Create job by plan instance, plan_inst_id:{self.plan_inst_id}, "
                        f"success:{success}, job_id:{job_id}, error: {err_msg}")
        else:
            logger.info(f"Create job, success:{success}, job_id:{job_id}, error: {err_msg}")
        return success, job_id, err_msg
