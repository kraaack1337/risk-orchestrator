"""services/mocks.py — реалистичные заглушки FakeRiskService, MockOrchestrator и MockReglamentScheduler."""

from __future__ import annotations

import asyncio
import copy
import inspect
from datetime import date, datetime
from typing import Any

from core.models import (
    CrossDateCollisionError,
    DataCorruptionError,
    DeadlockError,
    OrchestratorEvent,
    ProcessSpec,
    ProgressCallback,
    StateChangeCallback,
    TaskRequest,
    TaskRun,
    TaskStatus,
    TriggerType,
)
from services.microservices import (
    CARRIER,
    DELAY,
    GLOBAL_SOURCES,
    PROCESS_SPECS,
    SLA,
    SPOILAGE,
    STOCKOUT,
    TRANSIT,
    WAREHOUSE,
)


class FakeRiskService:
    """Эмуляция расчёта ~3.2 с (5 шагов по 0.64 с) с прогрессом 20..100."""

    def __init__(self, spec: ProcessSpec) -> None:
        self.spec = spec

    async def run(
        self,
        task_id: int,
        target_date: str,
        trigger: TriggerType,
        on_progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        for pct in (20, 40, 60, 80, 100):
            await asyncio.sleep(0.64)
            if on_progress:
                await on_progress(task_id, pct)
        return {
            "process_id": self.spec.process_id,
            "target_date": target_date,
            "trigger": trigger.value,
            "risk_score": round(0.18 + (task_id * 17 % 73) / 100.0, 2),
            "records_checked": 40 + (task_id * 29) % 160,
        }


class MockOrchestrator:
    """Автономный in-memory оркестратор с полной поддержкой контракта Orchestrator (Раздел 5.4)."""

    def __init__(self, services: dict[str, Any] | None = None) -> None:
        self.storage = None
        self._specs: dict[str, ProcessSpec] = copy.deepcopy(PROCESS_SPECS)
        self._services = services or {pid: FakeRiskService(s) for pid, s in self._specs.items()}
        self._enabled = True
        self._paused = False
        self._started = False
        self._seq = 0
        self._tasks: dict[int, TaskRun] = {}
        self._events: list[OrchestratorEvent] = []
        self._subs: list[StateChangeCallback] = []
        self._runners: dict[int, asyncio.Task[Any]] = {}
        self._delayed: set[asyncio.Task[Any]] = set()
        self._doom: dict[int, tuple[int, Exception]] = {}
        self._max_parallelism = 0

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._log("INFO", None, "MockOrchestrator запущен и готов к диспетчеризации", "STARTUP")
        self._pump()
        self._notify()

    async def stop(self) -> None:
        self._started = False
        for dt in list(self._delayed):
            dt.cancel()
        if self._delayed:
            await asyncio.gather(*self._delayed, return_exceptions=True)
        self._delayed.clear()
        await self._cancel_runners()
        self._log("INFO", None, "MockOrchestrator остановлен", "SHUTDOWN")
        self._notify()

    def set_orchestration_enabled(self, enabled: bool) -> None:
        self._enabled = bool(enabled)
        mode_str = "TO-BE (С оркестратором)" if self._enabled else "AS-IS (Без оркестратора)"
        self._log("INFO" if self._enabled else "WARN", None, f"Переключён режим: {mode_str}", "MODE")
        self._pump()
        self._notify()

    def is_orchestration_enabled(self) -> bool:
        return self._enabled

    def is_paused(self) -> bool:
        return self._paused

    def pause(self) -> None:
        self._paused = True
        self._log("WARN", None, "Очередь диспетчера поставлена на паузу", "PAUSED")
        self._notify()

    def resume(self) -> None:
        self._paused = False
        self._log("INFO", None, "Диспетчеризация очереди возобновлена", "RESUMED")
        self._pump()
        self._notify()

    async def reset(self, *, clear_storage: bool = False) -> None:
        _ = clear_storage
        for dt in list(self._delayed):
            dt.cancel()
        self._delayed.clear()
        await self._cancel_runners()
        self._tasks.clear()
        self._events.clear()
        self._doom.clear()
        self._seq = 0
        self._paused = False
        self._max_parallelism = 0
        self._log("INFO", None, "Стенд сброшен: очередь, блокировки и журнал очищены", "RESET")
        self._notify()

    def subscribe(self, callback: StateChangeCallback) -> None:
        if callback not in self._subs:
            self._subs.append(callback)

    def unsubscribe(self, callback: StateChangeCallback) -> None:
        if callback in self._subs:
            self._subs.remove(callback)

    async def submit(self, request: TaskRequest) -> TaskRun:
        if not self._started:
            await self.start()
        spec = self._specs.get(request.process_id)
        if spec is None:
            raise ValueError(f"Unknown process: {request.process_id}")

        if request.delay_sec > 0:
            await asyncio.sleep(request.delay_sec)

        trigger = TriggerType(request.trigger)
        priority = 1 if trigger == TriggerType.AUTO else (
            int(request.priority) if request.priority is not None else spec.default_priority
        )
        initiated_by = request.initiated_by or (
            "scheduler_bot" if trigger == TriggerType.AUTO else "analyst_1"
        )
        duplicate = self._enabled and any(
            t.process_id == request.process_id
            and t.target_date == request.target_date
            and t.status in (TaskStatus.QUEUED, TaskStatus.RUNNING)
            for t in self._tasks.values()
        )
        self._seq += 1
        now = datetime.now()
        status = TaskStatus.DEDUPLICATED if duplicate else TaskStatus.QUEUED
        task = TaskRun(
            task_id=self._seq,
            process_id=request.process_id,
            target_date=request.target_date,
            trigger=trigger,
            priority=priority,
            status=status,
            progress=0,
            queued_at=now,
            finished_at=now if duplicate else None,
            initiated_by=initiated_by,
            session_id=request.session_id,
            effective_priority=float(priority),
            orchestration_enabled=self._enabled,
        )
        self._tasks[task.task_id] = task
        self._log(
            "INFO",
            task.task_id,
            f"{status.value}: {task.process_id} ({task.target_date}) от {initiated_by}",
            status.value,
        )
        self._pump()
        self._notify()
        return copy.deepcopy(task)

    async def submit_batch(self, requests: list[TaskRequest]) -> list[TaskRun]:
        if not self._started:
            await self.start()
        immediate: list[TaskRun] = []
        for req in requests:
            if req.delay_sec and req.delay_sec > 0:
                dt = asyncio.create_task(self.submit(req))
                self._delayed.add(dt)
                dt.add_done_callback(self._delayed.discard)
            else:
                immediate.append(await self.submit(req))
        return immediate

    async def cancel(self, task_id: int) -> bool:
        task = self._tasks.get(task_id)
        if task is None or task.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
            return False
        runner = self._runners.pop(task_id, None)
        if runner is not None:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        task.status = TaskStatus.CANCELLED
        task.finished_at = datetime.now()
        task.blocked_by = None
        task.error_type = "CancelledError"
        task.error_message = "CancelledError: Cancelled by user"
        self._log("WARN", task_id, f"CANCELLED: задача #{task_id} отменена пользователем", "CANCELLED")
        self._pump()
        self._notify()
        return True

    def get_process_specs(self) -> list[ProcessSpec]:
        return copy.deepcopy(list(self._specs.values()))

    def get_tasks(self, status: TaskStatus | None = None) -> list[TaskRun]:
        return copy.deepcopy(
            [t for t in self._tasks.values() if status is None or t.status == status]
        )

    def get_active_locks(self) -> dict[str, list[int]]:
        if not self._enabled:
            return {}
        locks: dict[str, list[int]] = {}
        for t in self._running():
            spec = self._specs[t.process_id]
            for src in spec.write_sources:
                part = "GLOBAL" if src in GLOBAL_SOURCES else t.target_date
                locks.setdefault(f"WRITE:SOURCE:{src}:{part}", []).append(t.task_id)
            for src in spec.read_sources:
                if src in spec.write_sources:
                    continue
                part = "GLOBAL" if src in GLOBAL_SOURCES else t.target_date
                locks.setdefault(f"READ:SOURCE:{src}:{part}", []).append(t.task_id)
            if not spec.allow_parallel_dates:
                locks.setdefault(f"WRITE:PROCESS:{t.process_id}:GLOBAL", []).append(t.task_id)
            if spec.uses_shared_staging and spec.staging_key:
                locks.setdefault(f"WRITE:STAGING:{spec.staging_key}:GLOBAL", []).append(t.task_id)
        return locks

    def get_events(self, limit: int = 100) -> list[OrchestratorEvent]:
        return copy.deepcopy(self._events[-limit:]) if limit > 0 else []

    def get_gantt_data(self) -> list[dict[str, Any]]:
        return copy.deepcopy(
            [
                {
                    "task_id": t.task_id,
                    "process_id": t.process_id,
                    "target_date": t.target_date,
                    "trigger": t.trigger.value,
                    "priority": t.priority,
                    "status": t.status.value,
                    "progress": t.progress,
                    "queued_at": t.queued_at,
                    "started_at": t.started_at,
                    "finished_at": t.finished_at,
                    "blocked_by": t.blocked_by,
                    "error_message": t.error_message,
                }
                for t in self._tasks.values()
            ]
        )

    def get_metrics(self) -> dict[str, Any]:
        tasks = list(self._tasks.values())
        counts = {s: sum(t.status == s for t in tasks) for s in TaskStatus}
        completed = counts[TaskStatus.COMPLETED]
        failed = counts[TaskStatus.FAILED]
        deduplicated = counts[TaskStatus.DEDUPLICATED]
        deadlocks = sum(t.error_type == "DeadlockError" for t in tasks)
        corruptions = sum(t.error_type == "DataCorruptionError" for t in tasks)
        cross_date = sum(t.error_type == "CrossDateCollisionError" for t in tasks)
        waits = [(t.started_at - t.queued_at).total_seconds() for t in tasks if t.started_at]
        runs = [
            (t.finished_at - t.started_at).total_seconds()
            for t in tasks
            if t.started_at and t.finished_at
        ]
        finished_total = completed + failed
        violations = max(failed, deadlocks + corruptions + cross_date)
        integrity_score = (
            round(100.0 * max(0, finished_total - violations) / finished_total, 1)
            if finished_total
            else 100.0
        )
        return {
            "total": len(tasks),
            "queued": counts[TaskStatus.QUEUED],
            "running": counts[TaskStatus.RUNNING],
            "completed": completed,
            "failed": failed,
            "cancelled": counts[TaskStatus.CANCELLED],
            "deduplicated": deduplicated,
            "deduplicated_count": deduplicated,
            "deadlocks": deadlocks,
            "corruptions": corruptions,
            "cross_date_collisions": cross_date,
            "avg_wait_sec": sum(waits) / len(waits) if waits else 0.0,
            "avg_run_sec": sum(runs) / len(runs) if runs else 0.0,
            "success_rate": 100.0 * completed / finished_total if finished_total else 100.0,
            "integrity_score": integrity_score,
            "active_locks": len(self.get_active_locks()),
            "max_parallelism": self._max_parallelism,
            "paused": self._paused,
            "orchestration_enabled": self._enabled,
        }

    def _running(self) -> list[TaskRun]:
        return [t for t in self._tasks.values() if t.status == TaskStatus.RUNNING]

    def _conflict(self, a: TaskRun, b: TaskRun) -> str | None:
        sa, sb = self._specs[a.process_id], self._specs[b.process_id]
        if a.process_id == b.process_id and not sa.allow_parallel_dates:
            return f"WRITE:STAGING:{sa.staging_key or a.process_id}:GLOBAL"
        if SLA in sa.write_sources and SLA in (*sb.read_sources, *sb.write_sources):
            return f"WRITE:SOURCE:{SLA}:GLOBAL"
        if SLA in sb.write_sources and SLA in (*sa.read_sources, *sa.write_sources):
            return f"WRITE:SOURCE:{SLA}:GLOBAL"
        if a.target_date != b.target_date:
            return None
        for src in (*sa.write_sources, *sb.write_sources):
            if src == SLA:
                continue
            if (src in sa.write_sources and src in (*sb.read_sources, *sb.write_sources)) or (
                src in sb.write_sources and src in (*sa.read_sources, *sa.write_sources)
            ):
                return f"WRITE:SOURCE:{src}:{a.target_date}"
        return None

    def _pump(self) -> None:
        if not self._started or self._paused:
            return
        now = datetime.now()
        running = self._running()
        queued = sorted(
            (t for t in self._tasks.values() if t.status == TaskStatus.QUEUED),
            key=lambda t: (t.effective_priority or float(t.priority), t.queued_at, t.task_id),
        )
        changed = False
        for t in queued:
            age = int((now - t.queued_at).total_seconds() / 15.0) if t.trigger == TriggerType.MANUAL else 0
            t.effective_priority = float(t.priority - age)
            hit = next(((o, r) for o in running if (r := self._conflict(t, o))), None)
            if hit and self._enabled:
                owner, res = hit
                why = f"Ожидает {res} (занят #{owner.task_id} {owner.process_id})"
                if t.blocked_by != why:
                    t.blocked_by = why
                    self._log("WARN", t.task_id, why, "WAIT_CONFLICT")
                    changed = True
                continue
            if hit and not self._enabled:
                self._doom[t.task_id] = (60, self._error_for(hit[1], t, hit[0]))
            t.status = TaskStatus.RUNNING
            t.started_at = now
            t.blocked_by = None
            running.append(t)
            self._max_parallelism = max(self._max_parallelism, len(running))
            self._runners[t.task_id] = asyncio.create_task(self._execute(t))
            self._log("INFO", t.task_id, f"RUNNING: #{t.task_id} {t.process_id} ({t.target_date})", "RUNNING")
            changed = True
        if changed:
            self._notify()

    @staticmethod
    def _error_for(reason: str, a: TaskRun, b: TaskRun) -> Exception:
        if "STAGING" in reason or (a.process_id == CARRIER and b.process_id == CARRIER):
            return CrossDateCollisionError(
                f"Коллизия staging-буфера {a.process_id} между {b.target_date} и {a.target_date}"
            )
        if {a.process_id, b.process_id} == {STOCKOUT, DELAY} and any(
            s in reason for s in (WAREHOUSE, TRANSIT)
        ):
            return DeadlockError(
                f"AB-BA Deadlock между #{a.task_id} {a.process_id} и #{b.task_id} {b.process_id}"
            )
        return DataCorruptionError(
            f"Lost Update / грязное чтение на {reason} (конфликт с #{b.task_id} {b.process_id})"
        )

    async def _execute(self, task: TaskRun) -> None:
        doom = self._doom.pop(task.task_id, None)

        async def on_progress(_tid: int, pct: int) -> None:
            task.progress = pct
            if doom and pct >= doom[0]:
                raise doom[1]
            self._notify()

        try:
            task.result = await self._services[task.process_id].run(
                task.task_id, task.target_date, task.trigger, on_progress
            )
            task.progress = 100
            task.status = TaskStatus.COMPLETED
            self._log("INFO", task.task_id, f"COMPLETED: #{task.task_id} {task.process_id}", "COMPLETED")
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            raise
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error_type = type(exc).__name__
            task.error_message = f"{task.error_type}: {exc}"
            self._log("ERROR", task.task_id, task.error_message, "FAILED")
        finally:
            task.finished_at = datetime.now()
            self._runners.pop(task.task_id, None)
            self._pump()
            self._notify()

    async def _cancel_runners(self) -> None:
        runners = list(self._runners.values())
        for r in runners:
            r.cancel()
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)
        self._runners.clear()

    def _log(self, level: str, task_id: int | None, message: str, code: str = "GENERAL") -> None:
        self._events.append(
            OrchestratorEvent(
                timestamp=datetime.now(),
                level=level,
                task_id=task_id,
                message=message,
                event_code=code,
            )
        )

    def _notify(self) -> None:
        for cb in tuple(self._subs):
            try:
                res = cb()
                if inspect.isawaitable(res):
                    asyncio.ensure_future(res)
            except Exception:
                continue


class MockReglamentScheduler:
    """Автономный планировщик регламента (совместимый с ReglamentScheduler)."""

    def __init__(self, orchestrator: Any, interval: float = 15.0) -> None:
        self._orchestrator = orchestrator
        self._interval = max(1.0, float(interval))
        self._enabled = False
        self._idx = 0
        self._task: asyncio.Task[None] | None = None

    def is_enabled(self) -> bool:
        return self._enabled

    def get_interval(self) -> float:
        return self._interval

    def set_interval(self, seconds: float) -> None:
        self._interval = max(1.0, float(seconds))

    def set_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        if enabled == self._enabled:
            return
        self._enabled = enabled
        if enabled:
            self._task = asyncio.create_task(self._loop())
        elif self._task is not None:
            self._task.cancel()
            self._task = None

    async def stop(self) -> None:
        self.set_enabled(False)

    async def _loop(self) -> None:
        cycle = (STOCKOUT, DELAY, SPOILAGE, CARRIER)
        target_day = date(2026, 11, 20).isoformat()
        try:
            while self._enabled:
                pid = cycle[self._idx % len(cycle)]
                self._idx += 1
                await self._orchestrator.submit(
                    TaskRequest(
                        process_id=pid,
                        target_date=target_day,
                        trigger=TriggerType.AUTO,
                        priority=1,
                        initiated_by="scheduler_bot",
                    )
                )
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            return
