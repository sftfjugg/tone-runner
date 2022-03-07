import unittest
from scheduler.job.build_package import BuildPackage


class TestBuildPackage(unittest.TestCase):

    def test_build_package(self):
        username = "test_user"
        build_task_name = "test001-by-tone-runner"
        repo = ""
        branch = "linux-next"
        commit = ""
        arch = "x86_64"
        ignore_cache = True
        build_inst = BuildPackage(
            username, build_task_name=build_task_name,
            code_repo=repo, code_branch=branch, commit_id=commit,
            cpu_arch=arch, ignore_cache=ignore_cache
        )
        print(build_inst.build_package())
        print(build_inst.query_build_result())

    def test_query_build_task(self):
        username = "test_user"
        build_task_id = 90
        build_inst = BuildPackage(username=username, build_task_id=build_task_id)
        print(build_inst.query_build_result())


if __name__ == "__main__":
    unittest.main()
