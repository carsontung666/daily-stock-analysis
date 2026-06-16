# -*- coding: utf-8 -*-
"""运行时定时调度服务：在常驻进程内用守护线程驱动 Scheduler，支持运行期启停、
热更新执行时间、手动触发与状态查询。

设计要点：
- 每次 start() 新建独立 Scheduler 实例，job 不进全局表，stop() 丢弃，跨重启不累积。
- 手动与定时触发共用同一把状态锁和 task_running 标志，锁内占位，不会并发跑两次。
"""

import logging
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class RuntimeSchedulerService:
    """运行时定时调度服务（线程安全）。"""

    def __init__(
        self,
        *,
        task: Callable[[], None],
        schedule_times_provider: Callable[[], List[str]],
        enabled_provider: Optional[Callable[[], bool]] = None,
    ):
        self._task = task
        self._schedule_times_provider = schedule_times_provider
        self._enabled_provider = enabled_provider

        self._lock = threading.RLock()
        self._scheduler: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None

        self._status_lock = threading.Lock()
        self._status: Dict[str, Any] = {
            "task_running": False,
            "last_started_at": None,
            "last_finished_at": None,
            "last_success": None,
            "last_error": None,
            "run_count": 0,
            "skipped_count": 0,
            "last_skipped_reason": None,
        }

    def reconcile_from_config(self) -> bool:
        """按最新配置启停或热更新调度，返回当前是否在运行（配置保存后调用）。"""
        if not self._is_enabled():
            self.stop()
        elif self.is_running():
            with self._lock:
                scheduler = self._scheduler
            if scheduler is not None:
                scheduler.request_refresh()
        else:
            self.start()
        return self.is_running()

    def start(self) -> bool:
        """启动调度线程；已运行 / 未启用 / 无有效时间时返回 False。"""
        with self._lock:
            if self.is_running():
                return False
            if not self._is_enabled():
                logger.info("调度未启用（SCHEDULE_ENABLED=false），运行时调度服务不启动")
                return False
            times = self._current_times()
            if not times:
                logger.info("未配置有效的定时推送时间，运行时调度服务不启动")
                return False

            from src.scheduler import Scheduler

            scheduler = Scheduler(
                schedule_times=times,
                schedule_times_provider=self._schedule_times_provider,
                install_signal_handlers=False,
            )
            # 不在此处同步立即执行，避免阻塞启动线程；是否立即跑由调用方决定
            scheduler.set_daily_task(self._wrapped_task, run_immediately=False)
            thread = threading.Thread(target=scheduler.run, name="runtime-scheduler", daemon=True)
            self._scheduler = scheduler
            self._thread = thread
            thread.start()
            logger.info("运行时调度服务已启动，执行时间: %s", ", ".join(scheduler.scheduled_times))
            return True

    def stop(self, timeout: float = 35.0) -> None:
        """停止调度线程并等待退出（幂等）。"""
        with self._lock:
            scheduler = self._scheduler
            thread = self._thread
            self._scheduler = None
            self._thread = None
        if scheduler is not None:
            scheduler.stop()
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning("运行时调度线程在 %.0fs 内未能退出", timeout)

    def is_running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def run_now(self) -> bool:
        """手动触发一次执行（后台线程，不阻塞调用方）；已有任务在执行则跳过。"""
        if not self._claim("手动触发被跳过：已有任务在执行"):
            logger.info("已有调度任务在执行，跳过本次手动触发")
            return False
        threading.Thread(
            target=self._run_claimed_task, name="runtime-scheduler-run-now", daemon=True
        ).start()
        return True

    def status(self) -> Dict[str, Any]:
        """返回调度服务当前状态快照。"""
        scheduler = self._scheduler
        with self._status_lock:
            snapshot = dict(self._status)
        snapshot.update(
            {
                "enabled": self._is_enabled(),
                "scheduler_running": self.is_running(),
                "schedule_times": (
                    list(scheduler.scheduled_times) if scheduler is not None else self._current_times()
                ),
                "next_run": scheduler.get_next_run_time() if scheduler is not None else None,
            }
        )
        return snapshot

    def _wrapped_task(self) -> None:
        """到点回调：占位成功才跑，重叠则跳过。"""
        if not self._claim("定时触发被抑制：上一次分析仍在执行"):
            logger.warning("检测到调度任务重叠执行，已跳过本次定时执行")
            return
        self._run_claimed_task()

    def _claim(self, reason_if_busy: str) -> bool:
        """锁内原子占位：空闲则占位返回 True，否则记一次跳过返回 False。"""
        with self._status_lock:
            if self._status["task_running"]:
                self._status["skipped_count"] += 1
                self._status["last_skipped_reason"] = reason_if_busy
                return False
            self._status["task_running"] = True
            self._status["last_started_at"] = datetime.now().isoformat(timespec="seconds")
            return True

    def _run_claimed_task(self) -> None:
        """执行已占位的任务；单次失败只记录、不抛出，确保后续调度继续。"""
        error: Optional[str] = None
        try:
            self._task()
        except Exception as exc:  # noqa: BLE001 - 单次失败不应中断后续调度
            error = f"{type(exc).__name__}: {exc}"
            logger.exception("运行时调度任务执行失败: %s", exc)
        finally:
            with self._status_lock:
                self._status["task_running"] = False
                self._status["last_finished_at"] = datetime.now().isoformat(timespec="seconds")
                self._status["last_success"] = error is None
                self._status["last_error"] = error
                self._status["run_count"] += 1

    def _is_enabled(self) -> bool:
        return True if self._enabled_provider is None else bool(self._enabled_provider())

    def _current_times(self) -> List[str]:
        return list(self._schedule_times_provider() or [])
