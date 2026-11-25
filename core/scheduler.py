"""Reglament submissions share the manual orchestrator and its locks."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta

from core.models import TaskRequest, TriggerType, from_utc, to_utc
from core.orchestrator import Orchestrator

_LOG = logging.getLogger(__name__)


class ReglamentScheduler:
    def __init__(self, orchestrator: Orchestrator, interval: float = 15.0) -> None:
        self._validate_interval(interval)
        self._orchestrator = orchestrator
        self._storage = orchestrator.storage
        self._interval = float(interval)
        self._enabled = False
        self._loop_task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._revision = 0

    @staticmethod
    def _validate_interval(seconds: float) -> None:
        if not math.isfinite(seconds) or seconds < 1:
            raise ValueError("Schedule interval must be finite and at least one second")

    def get_interval(self) -> float:
        return self._interval

    def set_interval(self, seconds: float) -> None:
        self._validate_interval(seconds)
        self._interval = float(seconds)
        if self._storage:
            with self._storage.transaction():
                for schedule in self._storage.list_schedules():
                    self._storage.update_schedule(schedule["schedule_id"], interval_sec=seconds)
        self._revision += 1
        self._wake.set()

    def is_enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        if enabled == self._enabled:
            return
        if enabled:
            asyncio.get_running_loop()  # Fail before mutating state outside an event loop.
        if self._storage:
            with self._storage.transaction():
                for schedule in self._storage.list_schedules():
                    self._storage.update_schedule(schedule["schedule_id"], is_enabled=int(enabled))
        self._enabled = enabled
        self._revision += 1
        self._wake.set()
        if enabled and (self._loop_task is None or self._loop_task.done()):
            self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self.set_enabled(False)
        task = self._loop_task
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._loop_task = None

    async def _loop(self) -> None:
        due: dict[int | str, float] = {}
        revision = self._revision
        while self._enabled:
            self._wake.clear()
            if revision != self._revision:
                due.clear()
                revision = self._revision
            specs = {s.process_id: s for s in self._orchestrator.get_process_specs() if s.is_active}
            schedules = self._storage.list_schedules() if self._storage else [
                {"schedule_id": pid, "process_id": pid, "is_enabled": 1,
                 "interval_sec": self._interval, "target_date_offset": 0}
                for pid in specs
            ]
            clock = asyncio.get_running_loop().time()
            delays = []
            for schedule in schedules:
                if not self._enabled:
                    break
                pid, key = schedule["process_id"], schedule["schedule_id"]
                if not schedule["is_enabled"] or pid not in specs:
                    continue
                interval = schedule["interval_sec"]
                planned = from_utc(schedule.get("next_trigger_at"))
                due.setdefault(key, clock + max(0, (planned - datetime.now()).total_seconds())
                               if planned else clock)
                if due[key] <= clock:
                    try:
                        await self._orchestrator.submit(TaskRequest(
                            pid, (date.today() + timedelta(days=schedule["target_date_offset"])).isoformat(),
                            TriggerType.AUTO, priority=1, initiated_by="scheduler_bot",
                        ))
                        if self._storage:
                            self._storage.update_schedule(
                                key, last_triggered_at=to_utc(datetime.now()),
                                next_trigger_at=to_utc(datetime.now() + timedelta(seconds=interval)),
                            )
                    except Exception:
                        _LOG.exception("Reglament submission failed for %s", pid)
                    due[key] = clock + interval
                delays.append(max(0.001, due[key] - clock))
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=min(delays) if delays else self._interval)
            except TimeoutError:
                continue
