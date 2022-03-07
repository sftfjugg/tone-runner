import unittest
from core.cache.remote_inst import RemoteProcessPlanInstSource as Rpl
from core.cache.clear_remote_inst import ClearProcessPlanInstSource as Clp
from scheduler.plan.processing_plan import ProcessingPlanInstContext
from scheduler.plan import process_plan
from core.cache.remote_source import RemoteFlowSource


class TestPlan(unittest.TestCase):

    @unittest.skip("ignore")
    def test_plan(self):
        call_times = 6
        plan_inst_id = 33500  # 只有test阶段
        process_plan_inst = ProcessingPlanInstContext(plan_inst_id)
        for _ in range(call_times):
            process_plan_inst.run()
            print(process_plan_inst.stage, process_plan_inst.state)

    @unittest.skip("ignore")
    def test_plan_with_remote_inst_context(self):
        call_times = 10
        plan_inst_id = 33608
        for _ in range(call_times):
            process_plan_inst = Rpl.get_process_plan_inst(plan_inst_id)
            is_complete = process_plan_inst.run()
            if is_complete:
                Clp.remove_process_plan_inst(plan_inst_id)
            else:
                Rpl.update_process_plan_inst(plan_inst_id, process_plan_inst)
            print(process_plan_inst.stage, process_plan_inst.state)

    @unittest.skip("ignore")
    def test_process_plan_with_async(self):
        real_plan_inst_id, fmt_plan_inst_id = RemoteFlowSource.pull_pending_plan()
        process_plan._processing_plan_inst(real_plan_inst_id, fmt_plan_inst_id)


if __name__ == "__main__":
    unittest.main()
