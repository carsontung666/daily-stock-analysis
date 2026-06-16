# -*- coding: utf-8 -*-
"""RuntimeSchedulerService 测试（Issue #1581）。

覆盖并发占位、定时重叠抑制、start/stop 不泄漏全局 job、reconcile 启停与热更新。
"""

import threading
import time
import unittest

import schedule

from src.services.runtime_scheduler_service import RuntimeSchedulerService


class RuntimeSchedulerServiceTestCase(unittest.TestCase):
    def setUp(self):
        schedule.clear()
        self._services = []

    def tearDown(self):
        for svc in self._services:
            svc.stop(timeout=5)
        schedule.clear()

    def _service(self, **kwargs):
        kwargs.setdefault("schedule_times_provider", lambda: ["18:00"])
        svc = RuntimeSchedulerService(**kwargs)
        self._services.append(svc)
        return svc

    def _wait(self, cond, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cond():
                return
            time.sleep(0.01)
        self.fail("条件在超时内未满足")

    def test_concurrent_run_now_rejects_second(self):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def task():
            calls.append(1)
            started.set()
            release.wait(timeout=5)

        svc = self._service(task=task)
        r1 = svc.run_now()
        self.assertTrue(started.wait(timeout=5))
        r2 = svc.run_now()
        release.set()

        self.assertTrue(r1)
        self.assertFalse(r2)
        self._wait(lambda: svc.status()["run_count"] == 1)
        self.assertEqual(calls, [1])
        self.assertEqual(svc.status()["skipped_count"], 1)

    def test_scheduled_trigger_skipped_when_task_running(self):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def task():
            calls.append(1)
            started.set()
            release.wait(timeout=5)

        svc = self._service(task=task)
        svc.run_now()
        self.assertTrue(started.wait(timeout=5))
        svc._wrapped_task()  # 模拟到点触发
        release.set()

        self._wait(lambda: svc.status()["run_count"] == 1)
        self.assertEqual(calls, [1])
        self.assertGreaterEqual(svc.status()["skipped_count"], 1)

    def test_start_does_not_auto_run_task(self):
        calls = []
        svc = self._service(task=lambda: calls.append(1), enabled_provider=lambda: True)
        self.assertTrue(svc.start())
        self.assertTrue(svc.is_running())
        self.assertEqual(calls, [])

    def test_reconcile_enable_does_not_auto_run(self):
        calls = []
        enabled = {"v": False}
        svc = self._service(
            task=lambda: calls.append(1),
            enabled_provider=lambda: enabled["v"],
        )
        enabled["v"] = True
        self.assertTrue(svc.reconcile_from_config())
        self.assertEqual(calls, [])

    def test_reconcile_follows_enabled_flag(self):
        enabled = {"v": False}
        svc = self._service(task=lambda: None, enabled_provider=lambda: enabled["v"])
        self.assertFalse(svc.reconcile_from_config())
        enabled["v"] = True
        self.assertTrue(svc.reconcile_from_config())
        enabled["v"] = False
        self.assertFalse(svc.reconcile_from_config())
        self.assertFalse(svc.is_running())

    def test_reconcile_while_running_refreshes_not_restart(self):
        svc = self._service(task=lambda: None, enabled_provider=lambda: True)
        self.assertTrue(svc.reconcile_from_config())
        first = svc._scheduler
        self.assertTrue(svc.reconcile_from_config())
        self.assertIs(svc._scheduler, first)  # 同一实例，未重启

    def test_service_restart_no_global_leak(self):
        svc = self._service(task=lambda: None, enabled_provider=lambda: True)
        svc.start()
        self.assertTrue(svc.is_running())
        self.assertEqual(len(schedule.jobs), 0)
        svc.stop()
        self.assertFalse(svc.is_running())
        svc.start()
        self.assertTrue(svc.is_running())
        self.assertEqual(len(schedule.jobs), 0)

    def test_start_returns_false_when_disabled(self):
        svc = self._service(task=lambda: None, enabled_provider=lambda: False)
        self.assertFalse(svc.start())
        self.assertFalse(svc.is_running())

    def test_start_returns_false_when_no_times(self):
        svc = self._service(
            task=lambda: None,
            enabled_provider=lambda: True,
            schedule_times_provider=lambda: [],
        )
        self.assertFalse(svc.start())

    def test_restart_replaces_old_instance(self):
        svc = self._service(task=lambda: None, enabled_provider=lambda: True)
        svc.start()
        first = svc._scheduler
        first_thread = svc._thread
        svc.stop()

        self.assertFalse(first_thread.is_alive())
        self.assertEqual(first._daily_jobs, {})

        svc.start()
        self.assertIsNot(svc._scheduler, first)
        self.assertEqual(len(schedule.jobs), 0)

    def test_status_contains_expected_keys(self):
        svc = self._service(task=lambda: None, enabled_provider=lambda: True)
        st = svc.status()
        for key in (
            "task_running", "enabled", "scheduler_running", "schedule_times",
            "next_run", "run_count", "skipped_count", "last_error",
        ):
            self.assertIn(key, st)


if __name__ == "__main__":
    unittest.main()
