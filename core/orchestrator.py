"""Single-process asyncio dispatcher; services own all risk calculations."""

from __future__ import annotations

import asyncio
import copy
import heapq
import inspect
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime
from typing import Any

from core.lock_manager import LockManager
from core.models import (
    AccessMode, LockScope, OrchestratorEvent, ProcessSpec, ResourceRule,
    StateChangeCallback, TaskRequest, TaskRun, TaskStatus, TriggerType, from_utc, to_utc,
)
from core.storage import Storage

_LOG = logging.getLogger(__name__)
_ACTIVE = (TaskStatus.QUEUED, TaskStatus.RUNNING)


class Orchestrator:
    def __init__(
        self, services: Mapping[str, Any], storage: Storage | None = None,
        process_specs: Sequence[ProcessSpec] | None = None, *,
        orchestration_enabled: bool = True, conservative_global_resources: bool = True,
        aging_interval_sec: float = 15.0,
    ) -> None:
        if not math.isfinite(aging_interval_sec) or aging_interval_sec <= 0:
            raise ValueError("aging_interval_sec must be positive and finite")
        if storage is None and process_specs is None:
            raise ValueError("Provide Storage or explicit process_specs")
        self.storage = storage
        self._services = dict(services)
        self._specs = {s.process_id: copy.deepcopy(s) for s in (
            storage.get_process_specs() if storage else process_specs
        )}
        self._global_resources = conservative_global_resources
        self._aging = aging_interval_sec
        self._enabled = orchestration_enabled
        self._paused = False
        self._running = False
        self._stopping = False
        self._recovered = False
        self._state_guard = asyncio.Lock()
        self._lifecycle = asyncio.Lock()
        self._wake = asyncio.Event()
        self._locks = LockManager(storage)
        self._tasks = {t.task_id: t for t in storage.load_tasks()} if storage else {}
        self._seq = max(self._tasks, default=0)
        self._events: list[OrchestratorEvent] = [OrchestratorEvent(
            from_utc(r["timestamp"]), r["level"], r["task_id"], r["message"],
            r["event_code"], r["user_id"], r["run_id"],
        ) for r in reversed(storage.list_events(limit=100))] if storage else []
        self._subs: list[StateChangeCallback] = []
        self._notifications: set[asyncio.Task] = set()
        self._workers: dict[int, asyncio.Task] = {}
        self._worker_started: dict[int, asyncio.Event] = {}
        self._dispatcher: asyncio.Task | None = None
        self._due: dict[int, float] = {}
        self._heap: list[tuple[float, int]] = []
        self._run_id: int | None = None
        self._max_parallelism = 0

    def get_process_specs(self) -> list[ProcessSpec]:
        if self.storage:
            self._specs = {s.process_id: s for s in self.storage.get_process_specs()}
        return copy.deepcopy(list(self._specs.values()))

    def _save(self, task: TaskRun) -> None:
        if self.storage:
            with self.storage.transaction():
                self.storage.save_task(task)
                if task.run_id is not None:
                    self.storage.refresh_run_metrics(task.run_id)

    def _event(self, level: str, task: TaskRun | None, message: str, code: str) -> None:
        event = OrchestratorEvent(datetime.now(), level, task.task_id if task else None,
                                  message, code, task.initiated_by if task else None,
                                  task.run_id if task else self._run_id)
        if self.storage:
            self.storage.create_event(
                message=message, level=level, task_id=event.task_id, event_code=code,
                run_id=event.run_id, user_id=event.user_id, timestamp=to_utc(event.timestamp),
            )
        self._events.append(event)

    def subscribe(self, callback: StateChangeCallback) -> None:
        if callback not in self._subs:
            self._subs.append(callback)

    def unsubscribe(self, callback: StateChangeCallback) -> None:
        if callback in self._subs:
            self._subs.remove(callback)

    def _notify(self) -> None:
        for callback in tuple(self._subs):
            try:
                result = callback()
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    self._notifications.add(task)
                    task.add_done_callback(self._notification_done)
            except (Exception, asyncio.CancelledError):
                _LOG.exception("State subscriber failed")

    def _notification_done(self, task: asyncio.Task) -> None:
        self._notifications.discard(task)
        if not task.cancelled() and task.exception():
            _LOG.error("Async state subscriber failed: %s", task.exception())

    async def start(self) -> None:
        async with self._lifecycle:
            if self._running:
                return
            if not self._recovered:
                interrupted_runs = set()
                for task in self._tasks.values():
                    if task.status in _ACTIVE:
                        task.status = TaskStatus.FAILED
                        task.finished_at = datetime.now()
                        task.blocked_by = None
                        task.error_type = "RuntimeError"
                        task.error_message = "RuntimeError: previous process interrupted"
                        self._save(task)
                        self._event("ERROR", task, task.error_message, "RECOVERED_INTERRUPTION")
                        if task.run_id is not None:
                            interrupted_runs.add(task.run_id)
                if self.storage:
                    for run_id in interrupted_runs:
                        self.storage.finalize_simulation_run(run_id)
                self._recovered = True
            self._running = True
            self._stopping = False
            self._wake.set()
            self._dispatcher = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        async with self._lifecycle:
            await self._stop()

    async def _stop(self) -> None:
        async with self._state_guard:
            self._stopping = True
            self._running = False
        if self._dispatcher:
            self._dispatcher.cancel()
            await asyncio.gather(self._dispatcher, return_exceptions=True)
            self._dispatcher = None
        workers = list(self._workers.values())
        await asyncio.gather(*(event.wait() for event in self._worker_started.values()))
        for worker in workers:
            if not worker.cancelling():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for task in self._tasks.values():
            if task.status == TaskStatus.QUEUED:
                self._finish(task, TaskStatus.CANCELLED, "CancelledError", "Orchestrator stopped")
        notifications = list(self._notifications)
        for notification in notifications:
            notification.cancel()
        await asyncio.gather(*notifications, return_exceptions=True)
        self._locks.reset()
        self._heap.clear()
        self._due.clear()
        if self.storage and self._run_id is not None:
            self.storage.refresh_run_metrics(self._run_id)
            self.storage.finalize_simulation_run(self._run_id)
        self._run_id = None
        self._stopping = False

    async def reset(self) -> None:
        async with self._lifecycle:
            restart = self._running
            await self._stop()
            self._tasks.clear()
            self._events.clear()
            self._paused = False
            self._max_parallelism = 0
            if restart:
                self._running = True
                self._wake.set()
                self._dispatcher = asyncio.create_task(self._dispatch_loop())
        self._notify()

    def pause(self) -> None:
        self._paused = True
        self._notify()

    def resume(self) -> None:
        self._paused = False
        self._wake.set()
        self._notify()

    def is_paused(self) -> bool:
        return self._paused

    def set_orchestration_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        if enabled == self._enabled:
            return
        if any(t.status in _ACTIVE for t in self._tasks.values()):
            raise RuntimeError("Finish or reset active tasks before switching orchestration mode")
        if self.storage and self._run_id is not None:
            self.storage.finalize_simulation_run(self._run_id)
        self._run_id = None
        self._enabled = enabled
        self._notify()

    def is_orchestration_enabled(self) -> bool:
        return self._enabled

    async def submit(self, request: TaskRequest) -> TaskRun:
        await self.start()
        async with self._state_guard:
            if self._stopping or not self._running:
                raise RuntimeError("Orchestrator is stopping")
            specs = {s.process_id: s for s in self.get_process_specs()}
            spec = specs.get(request.process_id)
            if spec is None or not spec.is_active or request.process_id not in self._services:
                raise ValueError(f"Unknown or inactive process: {request.process_id}")
            date.fromisoformat(request.target_date)
            trigger = TriggerType(request.trigger)
            if not math.isfinite(request.delay_sec) or request.delay_sec < 0:
                raise ValueError("delay_sec must be nonnegative and finite")
            if not math.isfinite(spec.timeout_sec) or spec.timeout_sec <= 0:
                raise ValueError("timeout_sec must be positive and finite")
            priority = 1 if trigger == TriggerType.AUTO else (
                request.priority if request.priority is not None else max(2, min(3, spec.default_priority))
            )
            if trigger == TriggerType.MANUAL and priority not in (2, 3):
                raise ValueError("MANUAL priority must be 2 or 3")
            duplicate = self._enabled and any(
                t.process_id == request.process_id and t.target_date == request.target_date
                and t.status in _ACTIVE for t in self._tasks.values()
            )
            status = TaskStatus.DEDUPLICATED if duplicate else TaskStatus.QUEUED
            initiated_by = request.initiated_by or (
                "scheduler_bot" if trigger == TriggerType.AUTO else "analyst_1"
            )
            now = datetime.now()
            if self.storage:
                if self._run_id is None:
                    self._run_id = self.storage.create_simulation_run(orchestration_enabled=self._enabled)
                task_id = self.storage.create_task(
                    process_id=request.process_id, target_date=request.target_date, trigger_type=trigger.value,
                    initiated_by=initiated_by, session_id=request.session_id, run_id=self._run_id,
                    idempotency_key=f"{request.process_id}:{request.target_date}", priority=priority,
                    status=status.value, queued_at=to_utc(now), orchestration_enabled=self._enabled,
                )
            else:
                self._seq += 1
                task_id = self._seq
            task = TaskRun(task_id, request.process_id, request.target_date, trigger, priority,
                           status, 0, now, initiated_by=initiated_by, session_id=request.session_id,
                           run_id=self._run_id, effective_priority=float(priority),
                           orchestration_enabled=self._enabled)
            self._tasks[task_id] = task
            if duplicate:
                task.finished_at = now
            else:
                clock = asyncio.get_running_loop().time()
                self._due[task_id] = clock + request.delay_sec
            self._save(task)
            self._event("INFO", task, f"{status.value}: {task.process_id} за {task.target_date}", status.value)
            self._wake.set()
        self._notify()
        return copy.deepcopy(task)

    async def submit_batch(self, requests: list[TaskRequest]) -> list[TaskRun]:
        return [await self.submit(request) for request in requests]

    async def cancel(self, task_id: int) -> bool:
        async with self._state_guard:
            task = self._tasks.get(task_id)
            if task is None or task.status not in _ACTIVE:
                return False
            worker = self._workers.get(task_id)
            if worker:
                started = self._worker_started[task_id]
            else:
                self._finish(task, TaskStatus.CANCELLED, "CancelledError", "Cancelled by user")
        if worker:
            await started.wait()
            if not worker.cancelling():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self._wake.set()
        self._notify()
        return True

    def _locking_spec(self, spec: ProcessSpec) -> ProcessSpec:
        if not self._global_resources:
            return spec
        rules = spec.resource_rules or tuple(
            ResourceRule(src, AccessMode.WRITE if src in spec.write_sources else AccessMode.READ)
            for src in dict.fromkeys((*spec.read_sources, *spec.write_sources))
        )
        return replace(spec, resource_rules=tuple(replace(r, lock_scope=LockScope.GLOBAL) for r in rules))

    async def _dispatch_loop(self) -> None:
        while self._running:
            self._wake.clear()
            try:
                delay = await self._dispatch()
            except Exception:
                _LOG.exception("Dispatcher failed; queue paused")
                self._paused = True
                delay = None
            try:
                if delay is None:
                    await self._wake.wait()
                else:
                    await asyncio.wait_for(self._wake.wait(), timeout=max(0.001, delay))
            except TimeoutError:
                continue

    async def _dispatch(self) -> float | None:
        if self._paused:
            return None
        async with self._state_guard:
            clock = asyncio.get_running_loop().time()
            next_wake: list[float] = []
            self._heap.clear()
            for task in self._tasks.values():
                if task.status != TaskStatus.QUEUED:
                    continue
                due = self._due[task.task_id]
                if due > clock:
                    next_wake.append(due - clock)
                    continue
                age = int((clock - due) / self._aging) if task.trigger == TriggerType.MANUAL else 0
                effective_priority = task.priority - age
                if effective_priority != task.effective_priority:
                    task.effective_priority = effective_priority
                    self._save(task)
                heapq.heappush(self._heap, (task.effective_priority, task.task_id))
                if task.trigger == TriggerType.MANUAL:
                    next_wake.append(self._aging - (clock - due) % self._aging)
            changed = False
            while self._heap and self._running and not self._paused:
                _, task_id = heapq.heappop(self._heap)
                task = self._tasks[task_id]
                spec = self._specs[task.process_id]
                acquired, reason = (True, None)
                if task.orchestration_enabled:
                    acquired, reason = await self._locks.try_acquire(task, self._locking_spec(spec))
                if not acquired:
                    if task.blocked_by != reason:
                        task.blocked_by = reason
                        self._save(task)
                        self._event("WARN", task, reason, "WAIT_CONFLICT")
                        changed = True
                    continue
                task.status = TaskStatus.RUNNING
                task.started_at = datetime.now()
                task.blocked_by = None
                self._worker_started[task_id] = asyncio.Event()
                self._workers[task_id] = asyncio.create_task(self._execute(task, spec.timeout_sec))
                self._max_parallelism = max(self._max_parallelism, len(self._workers))
                self._save(task)
                self._event("INFO", task, "RUNNING", "RUNNING")
                changed = True
            if changed:
                self._notify()
            return min(next_wake) if next_wake else None

    def _finish(self, task: TaskRun, status: TaskStatus, error_type: str | None = None,
                message: str | None = None) -> None:
        task.status = status
        task.finished_at = datetime.now()
        task.blocked_by = None
        task.error_type = error_type
        task.error_message = f"{error_type}: {message}" if error_type else None
        self._due.pop(task.task_id, None)
        self._save(task)
        self._event("ERROR" if status == TaskStatus.FAILED else "INFO", task,
                    task.error_message or status.value, status.value)

    async def _execute(self, task: TaskRun, timeout: float) -> None:
        self._worker_started[task.task_id].set()
        async def on_progress(task_id: int, percent: int) -> None:
            if task_id != task.task_id:
                raise ValueError("Progress task_id does not match owner")
            task.progress = max(task.progress, min(100, max(0, int(percent))))
            self._save(task)
            self._notify()

        service_task: asyncio.Task | None = None
        expired = False
        status, error_type, message = TaskStatus.COMPLETED, None, None
        try:
            service_task = asyncio.create_task(self._services[task.process_id].run(
                task.task_id, task.target_date, task.trigger, on_progress))
            done, _ = await asyncio.wait({service_task}, timeout=timeout)
            if not done:
                expired = True
                status, error_type, message = TaskStatus.FAILED, "TimeoutError", "Resource TTL exceeded"
            else:
                task.result = service_task.result()
                task.progress = 100
        except asyncio.CancelledError:
            status, error_type, message = TaskStatus.CANCELLED, "CancelledError", "Execution cancelled"
        except Exception as exc:
            name = type(exc).__name__
            error_type = name if name in {
                "DeadlockError", "DataCorruptionError", "CrossDateCollisionError", "TimeoutError",
            } else "RuntimeError"
            status, message = TaskStatus.FAILED, f"{name}: {exc}"
        finally:
            if service_task is not None:
                if not service_task.done():
                    service_task.cancel()
                while not service_task.done():
                    try:
                        await asyncio.shield(service_task)
                    except asyncio.CancelledError:
                        if not expired:
                            status, error_type, message = (
                                TaskStatus.CANCELLED, "CancelledError", "Execution cancelled")
                    except Exception:
                        break
                await asyncio.gather(service_task, return_exceptions=True)
            # The service, including cancellation cleanup, has actually stopped now.
            try:
                await self._locks.release(task.task_id, expired=expired)
            finally:
                self._workers.pop(task.task_id, None)
                self._worker_started.pop(task.task_id, None)
                self._finish(task, status, error_type, message)
                self._wake.set()
                if not self._stopping:
                    self._notify()

    def get_tasks(self, status: TaskStatus | None = None) -> list[TaskRun]:
        return copy.deepcopy([t for t in self._tasks.values() if status is None or t.status == status])

    def get_active_locks(self) -> dict[str, list[int]]:
        return self._locks.get_snapshot()

    def get_events(self, limit: int = 100) -> list[OrchestratorEvent]:
        return copy.deepcopy(self._events[-limit:]) if limit > 0 else []

    def get_gantt_data(self) -> list[dict[str, Any]]:
        if self.storage:
            rows = [r for r in self.storage.get_gantt_timeline() if r["task_id"] in self._tasks]
            # VIEW owns timeline selection; model adaptation supplies local UI datetimes.
            ids = [r["task_id"] for r in rows]
        else:
            ids = list(self._tasks)
        return copy.deepcopy([{
            "task_id": t.task_id, "process_id": t.process_id, "target_date": t.target_date,
            "trigger": t.trigger.value, "priority": t.priority, "status": t.status.value,
            "progress": t.progress, "queued_at": t.queued_at, "started_at": t.started_at,
            "finished_at": t.finished_at, "blocked_by": t.blocked_by, "error_message": t.error_message,
        } for task_id in ids for t in [self._tasks[task_id]]])

    def get_metrics(self) -> dict[str, Any]:
        tasks = list(self._tasks.values())
        counts = {s: sum(t.status == s for t in tasks) for s in TaskStatus}
        waits = [(t.started_at - t.queued_at).total_seconds() for t in tasks if t.started_at]
        runs = [(t.finished_at - t.started_at).total_seconds()
                for t in tasks if t.started_at and t.finished_at]
        completed, failed = counts[TaskStatus.COMPLETED], counts[TaskStatus.FAILED]
        return {
            "total": len(tasks), "queued": counts[TaskStatus.QUEUED],
            "running": counts[TaskStatus.RUNNING], "completed": completed, "failed": failed,
            "cancelled": counts[TaskStatus.CANCELLED], "deduplicated": counts[TaskStatus.DEDUPLICATED],
            "deadlocks": sum(t.error_type == "DeadlockError" for t in tasks),
            "corruptions": sum(t.error_type == "DataCorruptionError" for t in tasks),
            "cross_date_collisions": sum(t.error_type == "CrossDateCollisionError" for t in tasks),
            "avg_wait_sec": sum(waits) / len(waits) if waits else 0.0,
            "avg_run_sec": sum(runs) / len(runs) if runs else 0.0,
            "success_rate": 100 * completed / (completed + failed) if completed + failed else 100.0,
            "active_locks": len(self.get_active_locks()), "max_parallelism": self._max_parallelism,
            "paused": self._paused, "orchestration_enabled": self._enabled,
        }
