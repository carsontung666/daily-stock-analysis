# -*- coding: utf-8 -*-
"""Scheduler 多时间点与实例隔离测试（Issue #1581）。

刻意使用真实 schedule 库（不 mock），验证：job 注册在本实例上、不进全局表、
跨实例不串、重配不累积，从结构上证明 v1 的 2->2->4 泄漏不存在。
"""

import threading
import unittest

import schedule

from src.scheduler import Scheduler


def _make(times):
    s = Scheduler(schedule_times=times, install_signal_handlers=False)
    s.set_daily_task(lambda: None, run_immediately=False)
    return s


class SchedulerMultiTimeTestCase(unittest.TestCase):
    def setUp(self):
        schedule.clear()

    def tearDown(self):
        schedule.clear()

    def test_multi_times_register_on_instance_not_global(self):
        s = _make(["18:00", "09:20", "12:30"])
        self.assertEqual(s.scheduled_times, ["09:20", "12:30", "18:00"])  # 去重排序
        self.assertEqual(len(s._scheduler.get_jobs()), 3)
        self.assertEqual(len(schedule.jobs), 0)  # 全局表不受影响

    def test_dedup_and_drop_invalid_times(self):
        s = _make(["18:00", "18:00", "25:99", "bad", "09:20"])
        self.assertEqual(s.scheduled_times, ["09:20", "18:00"])
        self.assertEqual(len(s._scheduler.get_jobs()), 2)

    def test_reconfigure_full_rebuild_no_accumulation(self):
        s = _make(["09:00", "18:00"])
        self.assertEqual(len(s._scheduler.get_jobs()), 2)

        # 重配为不同的时间集合 -> 全量重建，不累积
        s._configure_daily_tasks(["10:00"])
        self.assertEqual(s.scheduled_times, ["10:00"])
        self.assertEqual(len(s._scheduler.get_jobs()), 1)  # 不是 3
        self.assertEqual(len(schedule.jobs), 0)

    def test_two_instances_isolated(self):
        s1 = _make(["09:00", "18:00"])
        s2 = _make(["09:00", "18:00"])
        self.assertIsNot(s1._scheduler, s2._scheduler)
        self.assertEqual(len(s1._scheduler.get_jobs()), 2)
        self.assertEqual(len(s2._scheduler.get_jobs()), 2)
        self.assertEqual(len(schedule.jobs), 0)

    def test_v1_leak_scenario_absent(self):
        # 复刻 v1 的 2->2->4：启用 -> 丢弃旧实例 -> 再启用
        s1 = _make(["09:00", "18:00"])
        self.assertEqual(len(s1._scheduler.get_jobs()), 2)
        del s1
        s2 = _make(["09:00", "18:00"])
        self.assertEqual(len(s2._scheduler.get_jobs()), 2)  # 是 2，不是 4
        self.assertEqual(len(schedule.jobs), 0)  # 全局表全程为 0

    def test_run_exit_clears_instance_jobs(self):
        s = _make(["09:00", "18:00"])
        self.assertEqual(len(s._scheduler.get_jobs()), 2)

        t = threading.Thread(target=s.run, daemon=True)
        t.start()
        s.stop()  # 通过事件立即唤醒主循环退出
        t.join(timeout=5)

        self.assertFalse(t.is_alive())
        self.assertEqual(len(s._scheduler.get_jobs()), 0)


if __name__ == "__main__":
    unittest.main()
