from abc import ABC, abstractmethod


class BaseServerPool(ABC):

    @classmethod
    @abstractmethod
    def pull(cls, default_info, **kwargs):
        """Gets information about the execution machine"""


class BaseAllocServer(ABC):

    @abstractmethod
    def spec_standalone_server_in_pool(self):
        """Specify a single machine for allocation"""

    @abstractmethod
    def spec_cluster_server(self):
        """Specify the cluster for allocation"""

    @abstractmethod
    def rand_standalone_server(self):
        """Randomly assign a machine to assign"""

    @abstractmethod
    def rand_cluster_server(self):
        """Randomly assign a cluster for allocation"""

    @abstractmethod
    def tag_standalone_server(self):
        """Randomly assign a machine based on the tag"""

    @abstractmethod
    def tag_cluster_server(self):
        """Randomly assign a cluster based on the tag"""

    @classmethod
    def release_server(cls, job_id, server_object_id, run_mode, server_provider):
        """Release server: standalone or cluster"""

    @abstractmethod
    def get_server_info(self):
        """ Access to machine information.
        # standalone data demo
        test_standalone_data = {
            "run_mode": "standalone",
            "server_provider": ""aligroup,
            "server_id": 1,
            "ip": "10.97.196.142",
            "sn": "xxx",
            "job_case_id": 1,
            "job_suite_id": 1,
            "ready": 1,
        }
        # cluster data demo
        test_cluster_data = {
            "run_mode": "cluster",
            "server_provider": ""aligroup,
            "cluster_id": 1,
            "servers": [
                {
                    "role": "local",
                    "ip": "10.97.196.142",
                    "sn": "xxx",
                    "server_id": 1,
                },
                {
                    "role": "remote",
                    "ip": "10.101.217.218",
                    "sn": "xxx",
                    "server_id": 2
                },
            ],
            "job_case_id": 2,
            "job_suite_id": 1,
            "ready": 1
        }
        # cloud data demo
        test_cloud_data = {
            "job_suite_id": 1,
            "job_case_id": 11,
            "server_provider": "aliyun",
            "test_type": "functional",
            "run_mode": "standalone",
            "ready": 1,
            "server_id": 11,
            "ip": "120.24.194.100",
            "sn": "xxx",
            "channel_type": "toneagent",
            "cloud_inst_meta": {
                "login_password": "xx...",
                "ak_id": 1,
                "provider": "aliyun_ecs",
                "image_id": "aliyun_1_1901_64_20G_alibase_2018310.vhd",
                "region": "cn-beijing",
                "zone_id": "cn-beijing-g",
                "is_instance": 1,
                "instance_id": "i-21e4j97255483r4ke",
                "instance_type": "ecs.g6.large",
                "name": "test-1d-tone-38114",
                "internet_max_bandwidth": "10",
                "data_disk_category": "cloud_ssd",
                "data_disk_count": "1",
                "data_disk_size": "40"
            }
        }
        # Machine data not prepared demo
        no_machine_data = {
            "run_mode": "standalone",
            "server_provider": ""aligroup,
            "ready": 0,
            "job_case_id": 1,
            "job_suite_id": 1,
        }"""
