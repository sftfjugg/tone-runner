import time
import json
import base64
import requests
import config
from urllib import parse
from core.decorator import retry
from tools.log_util import LoggerFactory


build_pkg_logger = LoggerFactory.build_package()
query_pkg_logger = LoggerFactory.query_package()


class BuildPackage:
    domain = config.BUILD_PKG_API_DOMAIN
    token = config.BUILD_PKG_API_TOKEN
    caller = "tone-runner"
    task_type = "base"
    query_type = "detail"
    headers = {"Content-Type": "application/json"}
    build_pkg_api = "/api/?_to_link=service/cbp/task/"
    query_pkg_api = "/api/?_to_link=service/cbp/task/"

    def __init__(self, username, build_task_name=None, code_repo=None, code_branch=None,
                 commit_id=None, cpu_arch="x86_64", compile_branch="master", build_config="default",
                 build_machine=None, ignore_cache=True, build_task_id=None):
        self.username = username
        self.build_task_name = build_task_name
        self.repo = code_repo
        self.branch = code_branch
        self.builder_branch = compile_branch
        self.commit = commit_id
        self.arch = cpu_arch
        self.build_config = build_config
        self.build_machine = build_machine
        self.ignore_cache = ignore_cache
        self.build_pkg_url = parse.urljoin(self.domain, self.build_pkg_api)
        self.query_pkg_url = parse.urljoin(self.domain, self.query_pkg_api)
        self.secret_token = self.get_secret_token()
        self.build_task_id = build_task_id

    def get_secret_token(self):
        token_info = f"{self.username}|{self.token}|{time.time()}"
        return base64.b64encode(token_info.encode("utf-8")).decode("utf-8")

    @retry(6, 1)
    def query_build_result(self):
        params = {
            "token": self.secret_token,
            "id": self.build_task_id,
            "query_type": self.query_type
        }
        query_pkg_logger.info(
            f"Now query build package.\nurl:{self.query_pkg_url}\n"
            f"headers:{self.headers}\nparams{params}\n username:{self.username}")
        response = requests.get(self.query_pkg_url, params=params, verify=False)
        query_pkg_logger.info(f"Query build package result:\n{response.text}\n\n")
        result = response.json()
        if result["code"] != 200:
            err_msg = result["msg"]
            query_pkg_logger.error(f"Query build package has error:{err_msg}\n\n")
            return False, {"result": err_msg}
        else:
            result = response.json()["data"]
            result["result"] = "build package success."
            return True, result

    @retry(6, 1)
    def build_package(self):
        data = {
            "name": self.build_task_name,
            "repo": self.repo,
            "branch": self.branch,
            "builder_branch": self.builder_branch,
            "commit": self.commit,
            "arch": self.arch,
            "build_config": self.build_config,
            "caller": self.caller,
            "task_type": self.task_type,
            "token": self.secret_token,
            "ignore_cache": self.ignore_cache
        }
        if self.build_machine:
            data.update({"server_ip": self.build_machine})
        build_pkg_logger.info(
            f"Now Build package.\nurl:{self.build_pkg_url}\nheaders:{self.headers}\ndata:{data}")
        response = requests.post(
            self.build_pkg_url,
            headers=self.headers,
            data=json.dumps(data),
            verify=False
        )
        build_pkg_logger.info(f"Build package result:\n{response.text}\n\n")
        result = response.json()
        if result["code"] != 200:
            err_msg = result["msg"]
            build_pkg_logger.error(f"Build package has error:{err_msg}\n\n")
            return False, {"result": err_msg}
        else:
            result = result["data"]
            self.build_task_id = result["id"]
            result["result"] = "build package success."
            return True, result
