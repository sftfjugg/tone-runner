import json
from lib import peewee
from .base import BaseModel
from constant import ExecState, ServerFlowFields, JobCfgFields


class Dag(BaseModel):
    job_id = peewee.IntegerField(unique=True)
    graph = peewee.BlobField(help_text="")
    is_update = peewee.BooleanField(default=False)
    block = peewee.BooleanField(default=True)

    class Meta:
        table_name = "dag"


class DagStepInstance(BaseModel):
    dag_id = peewee.IntegerField()
    step_data = peewee.TextField()
    stage = peewee.CharField()
    state = peewee.CharField(default=ExecState.PENDING)
    remark = peewee.TextField()

    class Meta:
        table_name = "dag_step_instance"

    @property
    def job_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.JOB_ID)

    @property
    def env_info(self):
        step_data = json.loads(self.step_data)
        return step_data.get(JobCfgFields.ENV_INFO, dict())

    @property
    def baseline_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.BASELINE_ID, 0)

    @property
    def cluster_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.CLUSTER_ID)

    @property
    def old_server_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.OLD_SERVER_ID)

    @old_server_id.setter
    def old_server_id(self, old_server_id):
        step_data = json.loads(self.step_data)
        step_data[ServerFlowFields.OLD_SERVER_ID] = old_server_id
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def server_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.SERVER_ID)

    @server_id.setter
    def server_id(self, new_server_id):
        step_data = json.loads(self.step_data)
        step_data[ServerFlowFields.SERVER_ID] = new_server_id
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def server_ip(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.SERVER_IP)

    @property
    def private_ip(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.PRIVATE_IP)

    @server_ip.setter
    def server_ip(self, server_ip):
        step_data = json.loads(self.step_data)
        step_data[ServerFlowFields.SERVER_IP] = server_ip
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def server_sn(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.SERVER_SN, "")

    @server_sn.setter
    def server_sn(self, server_sn):
        step_data = json.loads(self.step_data)
        step_data[ServerFlowFields.SERVER_SN] = server_sn
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def server_provider(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.SERVER_PROVIDER)

    @property
    def run_mode(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.RUN_MODE)

    @property
    def channel_type(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.CHANNEL_TYPE)

    @property
    def job_suite_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.JOB_SUITE_ID)

    @property
    def job_case_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.JOB_CASE_ID)

    @property
    def meta_data(self):
        return json.loads(self.step_data)

    @property
    def cloud_inst_mata(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.CLOUD_INST_META)

    @property
    def run_strategy(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.RUN_STRATEGY)

    @property
    def kernel_info(self):
        step_data = json.loads(self.step_data)
        return step_data.get(JobCfgFields.KERNEL_INFO)

    @kernel_info.setter
    def kernel_info(self, value):
        step_data = json.loads(self.step_data)
        step_data[JobCfgFields.KERNEL_INFO] = value
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def snapshot_server_id(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.SERVER_SNAPSHOT_ID)

    @property
    def kernel_version(self):
        step_data = json.loads(self.step_data)
        return step_data.get(JobCfgFields.KERNEL_VERSION)

    @kernel_version.setter
    def kernel_version(self, value):
        step_data = json.loads(self.step_data)
        step_data[JobCfgFields.KERNEL_VERSION] = value
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def reboot_time(self):
        step_data = json.loads(self.step_data)
        return step_data.get(JobCfgFields.REBOOT_TIME)

    @reboot_time.setter
    def reboot_time(self, value):
        step_data = json.loads(self.step_data)
        step_data[JobCfgFields.REBOOT_TIME] = value
        self.step_data = json.dumps(step_data)
        self.save()

    @property
    def in_pool(self):
        step_data = json.loads(self.step_data)
        return step_data.get(ServerFlowFields.IN_POOL)

    @classmethod
    def update_cloud_server_info(cls, dag_id, cloud_server):
        dag_steps = DagStepInstance.filter(dag_id=dag_id)
        for dag_step in dag_steps:
            step_data = json.loads(dag_step.step_data)
            if step_data.get('server_id') == cloud_server.parent_server_id:
                step_data.update({
                    ServerFlowFields.OLD_SERVER_ID: dag_step.server_id,
                    ServerFlowFields.SERVER_ID: cloud_server.id,
                    ServerFlowFields.SERVER_IP: cloud_server.server_ip,
                    ServerFlowFields.SERVER_SN: cloud_server.server_sn
                })
                dag_step.step_data = json.dumps(step_data)
                dag_step.save()
