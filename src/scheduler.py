# -*- coding: utf-8 -*-
"""
===================================
定时调度模块
===================================

职责：
1. 支持每日定时执行股票分析（单个或多个时间点）
2. 支持定时执行大盘复盘
3. 优雅处理信号，确保可靠退出

依赖：
- schedule: 轻量级定时任务库

设计要点：
- 每个 Scheduler 持有独立的 ``schedule.Scheduler()`` 实例，job 只注册到本实例，
  绝不进入全局 ``schedule`` 模块，避免多个调度器共享全局 job 表导致泄漏。
"""

import logging
import re
import signal
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """
    优雅退出处理器

    捕获 SIGTERM/SIGINT 信号，确保任务完成后再退出。

    ``signal.signal`` 只能在主线程注册。调度器运行在工作线程时
    （如 FastAPI lifespan 下的运行时调度服务），应将
    ``install_signal_handlers`` 设为 False，改由 stop() 显式控制退出。
    """

    def __init__(self, install_signal_handlers: bool = True):
        self.shutdown_requested = False
        self._lock = threading.Lock()
        if install_signal_handlers:
            self._install_signal_handlers()

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            logger.debug("非主线程，跳过信号处理器安装，由 stop() 控制退出")
            return
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.request_shutdown(f"收到退出信号 ({signum})")

    def request_shutdown(self, reason: Optional[str] = None) -> None:
        """线程安全地请求退出，供工作线程场景显式调用。"""
        with self._lock:
            if not self.shutdown_requested:
                if reason:
                    logger.info("%s，等待当前任务完成...", reason)
                self.shutdown_requested = True

    @property
    def should_shutdown(self) -> bool:
        """检查是否应该退出"""
        with self._lock:
            return self.shutdown_requested


class Scheduler:
    """
    定时任务调度器

    基于 schedule 库实现，支持：
    - 每日定时执行（单个或多个时间点）
    - 启动时立即执行
    - 运行期热重载执行时间
    - 优雅退出
    """

    def __init__(
        self,
        schedule_time: str = "18:00",
        schedule_time_provider: Optional[Callable[[], str]] = None,
        schedule_times: Optional[List[str]] = None,
        schedule_times_provider: Optional[Callable[[], List[str]]] = None,
        install_signal_handlers: bool = True,
    ):
        """
        初始化调度器

        Args:
            schedule_time: 单时间执行时间，格式 "HH:MM"（向后兼容/兜底）
            schedule_time_provider: 单时间热重载提供器，返回最新 "HH:MM"
            schedule_times: 多时间列表，提供后优先于 schedule_time
            schedule_times_provider: 多时间热重载提供器，返回最新时间列表
            install_signal_handlers: 是否安装 SIGINT/SIGTERM 处理器；
                工作线程场景应设为 False
        """
        try:
            import schedule
        except ImportError:
            logger.error("schedule 库未安装，请执行: pip install schedule")
            raise ImportError("请安装 schedule 库: pip install schedule")

        # 独立实例：job 仅注册到本实例，不进入全局 schedule 模块
        self._scheduler = schedule.Scheduler()

        self.schedule_time = schedule_time
        self._schedule_time_provider = schedule_time_provider
        self._explicit_schedule_times = list(schedule_times) if schedule_times else None
        self._schedule_times_provider = schedule_times_provider
        self.shutdown_handler = GracefulShutdown(
            install_signal_handlers=install_signal_handlers
        )
        self._task_callback: Optional[Callable] = None
        self._daily_jobs: Dict[str, Any] = {}  # 执行时间(HH:MM) -> job 句柄
        self.scheduled_times: List[str] = []    # 当前生效的执行时间（去重排序）
        self._background_tasks: List[Dict[str, Any]] = []
        self._running = False
        self._wake = threading.Event()

    def set_daily_task(self, task: Callable, run_immediately: bool = True):
        """
        设置每日定时任务（支持单个或多个执行时间）

        Args:
            task: 要执行的任务函数（无参数）
            run_immediately: 是否在设置后立即执行一次
        """
        self._task_callback = task
        target_times = self._initial_times()
        if not self._configure_daily_tasks(target_times):
            raise ValueError(f"无效的定时执行时间: {target_times!r}")

        if run_immediately:
            logger.info("立即执行一次任务...")
            self._safe_run_task()

    def _initial_times(self) -> List[str]:
        """初始目标执行时间：显式多时间优先，否则回退单时间。"""
        if self._explicit_schedule_times:
            return list(self._explicit_schedule_times)
        return [self.schedule_time]

    @staticmethod
    def _is_valid_schedule_time(schedule_time: str) -> bool:
        """Validate time string in HH:MM 24-hour format."""
        candidate = (schedule_time or "").strip()
        return bool(re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", candidate))

    @classmethod
    def _normalize_times(cls, times: Optional[List[str]]) -> List[str]:
        """校验、去重、排序时间列表，丢弃非法项（无副作用）。"""
        seen: set = set()
        out: List[str] = []
        for raw in times or []:
            candidate = (raw or "").strip()
            if not cls._is_valid_schedule_time(candidate) or candidate in seen:
                continue
            seen.add(candidate)
            out.append(candidate)
        return sorted(out)

    def _configure_daily_tasks(self, requested_times: List[str]) -> bool:
        """全量重建每日任务，使已注册时间与目标列表一致。

        先清空本实例所有 job，再按去重排序后的目标时间重新注册；无任何合法项时
        不做改动并返回 False（由调用方决定是初始报错还是热重载阶段沿用旧配置）。
        """
        invalid = [
            (t or "").strip()
            for t in (requested_times or [])
            if (t or "").strip() and not self._is_valid_schedule_time(t)
        ]
        if invalid:
            logger.warning(
                "忽略无效定时执行时间 %s，沿用 %s",
                ", ".join(invalid),
                self.scheduled_times or "(无)",
            )

        valid = self._normalize_times(requested_times)
        if not valid:
            return False

        previous = list(self.scheduled_times)
        self._scheduler.clear()
        self._daily_jobs = {}
        for candidate in valid:
            self._daily_jobs[candidate] = (
                self._scheduler.every().day.at(candidate).do(self._safe_run_task)
            )
        self.scheduled_times = valid
        self.schedule_time = valid[0]  # 单时间视图，向后兼容

        if previous == valid:
            logger.info("已设置每日定时任务，执行时间: %s", ", ".join(valid))
        else:
            logger.info(
                "定时执行时间已更新: %s -> %s",
                previous or "(无)",
                ", ".join(valid),
            )
        return True

    def _refresh_daily_schedule_if_needed(self) -> None:
        """根据最新运行时配置重建每日任务（如有变化）。"""
        if self._task_callback is None:
            return

        latest = self._poll_latest_times()
        if latest is None:
            return

        normalized = self._normalize_times(latest)
        if not normalized or normalized == self.scheduled_times:
            return

        if self._configure_daily_tasks(latest):
            logger.info("更新后的下次执行时间: %s", self._get_next_run_time())

    def _poll_latest_times(self) -> Optional[List[str]]:
        """读取最新执行时间：多时间提供器优先，否则单时间提供器；都没有则不热重载。"""
        if self._schedule_times_provider is not None:
            try:
                return list(self._schedule_times_provider() or [])
            except Exception as exc:  # pragma: no cover - defensive branch
                logger.warning("读取最新 SCHEDULE_TIMES 失败，沿用 %s: %s", self.scheduled_times, exc)
                return None
        if self._schedule_time_provider is not None:
            try:
                value = (self._schedule_time_provider() or "").strip()
            except Exception as exc:  # pragma: no cover - defensive branch
                logger.warning("读取最新 SCHEDULE_TIME 失败，沿用 %s: %s", self.schedule_time, exc)
                return None
            return [value] if value else []
        return None

    def _safe_run_task(self):
        """安全执行任务（带异常捕获）"""
        if self._task_callback is None:
            return

        try:
            logger.info("=" * 50)
            logger.info(f"定时任务开始执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 50)

            self._task_callback()

            logger.info(f"定时任务执行完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.exception(f"定时任务执行失败: {e}")

    def add_background_task(
        self,
        task: Callable,
        interval_seconds: int,
        run_immediately: bool = False,
        name: Optional[str] = None,
    ) -> None:
        """Register a periodic background task executed inside the scheduler loop.

        Note: The scheduler loop polls every 30 seconds, so *interval_seconds*
        below 30 will be clamped to 30 to avoid promising unreachable precision.
        """
        clamped_interval = max(30, int(interval_seconds))
        if int(interval_seconds) < 30:
            logger.warning(
                "后台任务 %s 请求间隔 %ds，但调度循环每 30s 轮询一次，已自动调整为 30s",
                name or getattr(task, "__name__", "background_task"),
                interval_seconds,
            )
        entry = {
            "task": task,
            "interval_seconds": clamped_interval,
            "last_run": 0.0,
            "name": name or getattr(task, "__name__", "background_task"),
            "thread": None,
            "running": False,
        }
        if not run_immediately:
            entry["last_run"] = time.time()
        self._background_tasks.append(entry)
        logger.info(
            "已注册后台任务: %s（间隔 %s 秒，立即执行=%s）",
            entry["name"],
            entry["interval_seconds"],
            run_immediately,
        )
        if run_immediately:
            self._start_background_task(entry)

    def _start_background_task(self, entry: Dict[str, Any]) -> bool:
        """Start one background task in a dedicated daemon thread."""
        worker = entry.get("thread")
        if worker is not None and worker.is_alive():
            return False

        def _runner() -> None:
            try:
                logger.info("后台任务开始执行: %s", entry["name"])
                entry["task"]()
            except Exception as exc:
                logger.exception("后台任务执行失败 [%s]: %s", entry["name"], exc)
            finally:
                entry["running"] = False
                entry["thread"] = None

        entry["last_run"] = time.time()
        entry["running"] = True
        worker = threading.Thread(
            target=_runner,
            daemon=True,
            name=f"scheduler-bg-{entry['name']}",
        )
        entry["thread"] = worker
        worker.start()
        return True

    def _run_background_tasks(self) -> None:
        """Execute any background tasks whose interval has elapsed."""
        if not self._background_tasks:
            return

        now = time.time()
        for entry in self._background_tasks:
            worker = entry.get("thread")
            if worker is not None and worker.is_alive():
                continue
            if entry.get("running"):
                entry["running"] = False
                entry["thread"] = None
            if now - entry["last_run"] < entry["interval_seconds"]:
                continue
            self._start_background_task(entry)

    def run(self):
        """
        运行调度器主循环

        阻塞运行，直到收到退出信号或调用 stop()
        """
        self._running = True
        logger.info("调度器开始运行...")
        logger.info(f"下次执行时间: {self._get_next_run_time()}")

        while self._running and not self.shutdown_handler.should_shutdown:
            self._refresh_daily_schedule_if_needed()
            self._scheduler.run_pending()
            self._run_background_tasks()

            # 每 30 秒检查一次；stop()/request_refresh() 通过事件立即唤醒
            self._wake.wait(timeout=30)
            self._wake.clear()

            # 每小时打印一次心跳
            if datetime.now().minute == 0 and datetime.now().second < 30:
                logger.info(f"调度器运行中... 下次执行: {self._get_next_run_time()}")

        # 退出时在本线程清掉本实例 job，确保无残留（与 run 同线程，无竞争）
        self._scheduler.clear()
        self._daily_jobs = {}
        logger.info("调度器已停止")

    def _get_next_run_time(self) -> str:
        """获取下次执行时间"""
        jobs = self._scheduler.get_jobs()
        if jobs:
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return "未设置"

    def get_next_run_time(self) -> str:
        """获取下次执行时间（公开接口，供调度服务读取状态）。"""
        return self._get_next_run_time()

    def request_refresh(self) -> None:
        """请求主循环立即重读调度配置（唤醒等待中的轮询）。"""
        self._wake.set()

    def stop(self):
        """停止调度器（立即唤醒等待中的主循环；job 在 run() 退出时清理）"""
        self._running = False
        self.shutdown_handler.request_shutdown()
        self._wake.set()


def run_with_schedule(
    task: Callable,
    schedule_time: str = "18:00",
    run_immediately: bool = True,
    background_tasks: Optional[List[Dict[str, Any]]] = None,
    schedule_time_provider: Optional[Callable[[], str]] = None,
    schedule_times: Optional[List[str]] = None,
    schedule_times_provider: Optional[Callable[[], List[str]]] = None,
):
    """
    便捷函数：使用定时调度运行任务

    Args:
        task: 要执行的任务函数
        schedule_time: 每日执行时间（单时间兜底）
        run_immediately: 是否立即执行一次
        background_tasks: 可选的后台任务定义列表。每项为一个字典，
            需包含 `task` 与 `interval_seconds`，可选包含 `name`
            和 `run_immediately`。`interval_seconds` 单位为秒。
        schedule_time_provider: 可选的单时间提供器；调度器每轮检查前会读取，
            当返回值变化时自动重建 daily job。
        schedule_times: 可选的多时间列表，提供后优先于 schedule_time。
        schedule_times_provider: 可选的多时间提供器，提供后优先于 schedule_time_provider。
    """
    scheduler = Scheduler(
        schedule_time=schedule_time,
        schedule_time_provider=schedule_time_provider,
        schedule_times=schedule_times,
        schedule_times_provider=schedule_times_provider,
    )
    for entry in background_tasks or []:
        scheduler.add_background_task(
            task=entry["task"],
            interval_seconds=entry["interval_seconds"],
            run_immediately=entry.get("run_immediately", False),
            name=entry.get("name"),
        )
    scheduler.set_daily_task(task, run_immediately=run_immediately)
    scheduler.run()


if __name__ == "__main__":
    # 测试定时调度
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )

    def test_task():
        print(f"任务执行中... {datetime.now()}")
        time.sleep(2)
        print("任务完成!")

    print("启动测试调度器（按 Ctrl+C 退出）")
    run_with_schedule(test_task, schedule_time="23:59", run_immediately=True)
