"""Deterministic asyncio tests, also runnable without pytest plugins."""

import asyncio
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from core.lock_manager import LockManager
from core.models import (
    AccessMode, LockScope, ProcessSpec, ResourceRule, TaskRequest, TaskRun,
    TaskStatus, TriggerType, from_utc, to_utc,
)
from core.orchestrator import Orchestrator
from core.scheduler import ReglamentScheduler
from core.storage import Storage


def spec(pid, reads=(), writes=(), parallel=True, rules=(), timeout=1.0):
    return ProcessSpec(pid, pid, reads, writes, parallel, 2, rules, timeout_sec=timeout)


def task(number, pid="p", day="2026-11-20"):
    return TaskRun(number, pid, day, TriggerType.MANUAL, 2, TaskStatus.QUEUED, 0, datetime.now())


def request(pid, *, trigger=TriggerType.MANUAL, priority=None, delay=0, day="2026-11-20"):
    return TaskRequest(pid, day, trigger, priority, delay)


class FakeRiskService:
    def __init__(self, gate=None, error=None, cleanup=None):
        self.gate = gate
        self.error = error
        self.cleanup = cleanup
        self.started = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.calls = []

    async def run(self, task_id, target_date, trigger, on_progress=None):
        self.calls.append(task_id)
        self.started.set()
        try:
            if on_progress:
                await on_progress(task_id, 20)
            if self.gate:
                await self.gate.wait()
            if self.error:
                raise self.error
            return {"id": task_id, "date": target_date, "trigger": trigger.value}
        finally:
            if self.cleanup:
                self.cleaning.set()
                await self.cleanup.wait()


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(wait(), 1.0)


def test_lock_compatibility_atomicity_and_scopes():
    async def check():
        locks = LockManager()
        read = spec("r", ("A",))
        write = spec("w", writes=("A", "B"))
        assert (await locks.try_acquire(task(1), read))[0]
        assert (await locks.try_acquire(task(2), read))[0]
        ok, why = await locks.try_acquire(task(3), write)
        assert not ok and "A" in why and "#1" in why
        assert all(3 not in owners for owners in locks.get_snapshot().values())
        assert not any(":B:" in key for key in locks.get_snapshot())
        assert (await locks.try_acquire(task(4, day="2026-11-21"), write))[0]
        assert not (await locks.try_acquire(task(5, day="2026-11-21"), write))[0]
        global_read = spec("g", rules=(ResourceRule("A", AccessMode.READ, LockScope.GLOBAL),))
        assert not (await locks.try_acquire(task(6), global_read))[0]
        for i in (1, 2, 4):
            await locks.release(i)
        assert (await locks.try_acquire(task(6), global_read))[0]
        assert (await locks.try_acquire(task(7), read))[0]
        assert not (await locks.try_acquire(task(8, day="2026-11-22"), write))[0]
        await locks.release(6)
        await locks.release(7)
        combined = spec("p", ("A",), ("A",), parallel=False)
        assert (await locks.try_acquire(task(9), combined))[0]
        assert not any(key.startswith("READ") for key in locks.get_snapshot())
        assert not (await locks.try_acquire(task(10, day="2026-11-21"), combined))[0]
        try:
            locks.reset()
        except RuntimeError:
            pass
        else:
            raise AssertionError("reset discarded a live owner")
        await locks.release(9)
        staged = spec("one")
        staged.uses_shared_staging, staged.staging_key = True, "buffer"
        assert (await locks.try_acquire(task(11), staged))[0]
        staged.process_id = "two"
        assert not (await locks.try_acquire(task(12), staged))[0]
        await locks.release(11)
        locks.reset()
        assert not locks.get_snapshot()
    asyncio.run(check())


def test_queue_priority_parallelism_and_fifo():
    async def check():
        gate = asyncio.Event()
        services = {p: FakeRiskService(gate if p == "hold" else None) for p in ("hold", "manual", "auto", "free")}
        specs = [spec("hold", writes=("A",)), spec("manual", writes=("A",)),
                 spec("auto", writes=("A",)), spec("free", writes=("B",))]
        orch = Orchestrator(services, process_specs=specs, conservative_global_resources=False)
        await orch.submit(request("hold"))
        await services["hold"].started.wait()
        orch.pause()
        manual = await orch.submit(request("manual", priority=3))
        auto = await orch.submit(request("auto", trigger=TriggerType.AUTO))
        free = await orch.submit(request("free"))
        orch.resume()
        await services["free"].started.wait()
        assert orch.get_tasks(TaskStatus.QUEUED)[0].blocked_by
        assert orch.get_metrics()["max_parallelism"] == 2
        assert not services["auto"].started.is_set()
        gate.set()
        await until(lambda: orch.get_metrics()["completed"] == 4)
        starts = [e.task_id for e in orch.get_events() if e.event_code == "RUNNING"]
        assert starts.index(auto.task_id) < starts.index(manual.task_id)
        assert free.task_id in starts
        snapshot = orch.get_tasks()
        snapshot[0].result["id"] = -1
        assert orch.get_tasks()[0].result["id"] != -1
        assert not orch.get_active_locks()
        await orch.stop()
        # Equal priorities preserve submission order.
        orch.pause()
        first = await orch.submit(request("manual", day="2026-11-21"))
        second = await orch.submit(request("auto", day="2026-11-21"))
        orch.resume()
        await until(lambda: orch.get_tasks()[-1].status == TaskStatus.COMPLETED)
        starts = [e.task_id for e in orch.get_events() if e.event_code == "RUNNING"]
        assert starts.index(first.task_id) < starts.index(second.task_id)
        await orch.stop()
    asyncio.run(check())


def test_deduplication_delay_and_aging():
    async def check():
        service = FakeRiskService()
        orch = Orchestrator({"p": service}, process_specs=[spec("p")], aging_interval_sec=0.01)
        orch.pause()
        tasks = await asyncio.gather(*(orch.submit(request("p", delay=0.02)) for _ in range(6)))
        assert sum(t.status == TaskStatus.DEDUPLICATED for t in tasks) == 5
        assert not orch.get_active_locks() and not service.calls
        orch.resume()
        await asyncio.sleep(0.005)
        assert not service.calls
        await until(lambda: orch.get_metrics()["completed"] == 1)
        assert len(service.calls) == 1
        orch.pause()
        old = await orch.submit(request("p", day="2026-11-21", priority=3))
        await asyncio.sleep(0.035)
        newer = await orch.submit(request("p", day="2026-11-22", trigger=TriggerType.AUTO))
        orch.resume()
        await until(lambda: orch.get_metrics()["completed"] == 3)
        starts = [e.task_id for e in orch.get_events() if e.event_code == "RUNNING"]
        assert starts.index(old.task_id) < starts.index(newer.task_id)
        await orch.stop()
    asyncio.run(check())


def test_exception_cancel_reset_notifications_and_modes():
    async def check():
        gate = asyncio.Event()
        services = {"bad": FakeRiskService(error=ValueError("failure")), "hold": FakeRiskService(gate)}
        orch = Orchestrator(services, process_specs=[spec("bad", writes=("A",)), spec("hold", writes=("A",))])

        def broken():
            raise RuntimeError("subscriber failure")

        notified = asyncio.Event()

        async def asynchronous():
            notified.set()

        orch.subscribe(broken)
        orch.subscribe(asynchronous)
        await orch.submit(request("bad"))
        await until(lambda: orch.get_metrics()["failed"] == 1)
        await notified.wait()
        assert not orch.get_active_locks()
        held = await orch.submit(request("hold"))
        await services["hold"].started.wait()
        try:
            orch.set_orchestration_enabled(False)
        except RuntimeError:
            pass
        else:
            raise AssertionError("mode changed while service was running")
        assert await orch.cancel(held.task_id)
        assert not orch.get_active_locks()
        await orch.submit(request("hold", delay=0.5))
        await orch.reset()
        assert not orch.get_tasks() and not orch.get_active_locks()
        assert not orch._workers and not orch._due
        orch.set_orchestration_enabled(False)
        failure = await orch.submit(request("bad"))
        await until(lambda: orch.get_tasks()[0].status == TaskStatus.FAILED)
        assert orch.get_tasks()[0].task_id == failure.task_id
        assert len(services["bad"].calls) == 2  # Same service executed in both modes.
        await orch.stop()
        await orch.stop()
        assert not orch._notifications
    asyncio.run(check())


def test_ttl_waits_for_real_cleanup_and_audits():
    async def check(root):
        store = Storage(root / "ttl.db")
        cleanup, gate = asyncio.Event(), asyncio.Event()
        slow = FakeRiskService(gate, cleanup=cleanup)
        fast = FakeRiskService()
        # Test metadata is changed through Storage, without modifying schema or seed files.
        with store.transaction() as conn:
            conn.execute("UPDATE processes SET timeout_sec = 0.01 WHERE process_id = 'StockoutRisk'")
        orch = Orchestrator({"StockoutRisk": slow, "SpoilageRisk": fast}, store)
        held = await orch.submit(request("StockoutRisk"))
        await slow.cleaning.wait()
        waiting = await orch.submit(request("SpoilageRisk"))
        await until(lambda: bool(orch.get_tasks()[-1].blocked_by))
        assert not fast.calls and orch.get_active_locks()
        cleanup.set()
        await until(lambda: orch.get_metrics()["completed"] == 1)
        saved = store.get_task(held.task_id)
        assert saved["status"] == "FAILED" and saved["error_type"] == "TimeoutError"
        assert "EXPIRED_TTL" in {e["action"] for e in store.list_lock_events(task_id=held.task_id)}
        assert store.get_task(waiting.task_id)["status"] == "COMPLETED"
        await orch.stop()
        # Reset during TTL cleanup must not cancel that cleanup a second time
        # or expose its resources to another service before it has stopped.
        cleanup2 = asyncio.Event()
        slow2 = FakeRiskService(asyncio.Event(), cleanup=cleanup2)
        second = Orchestrator({"StockoutRisk": slow2}, store)
        await second.submit(request("StockoutRisk"))
        await slow2.cleaning.wait()
        reset = asyncio.create_task(second.reset())
        await asyncio.sleep(0.005)
        assert not reset.done() and second.get_active_locks()
        cleanup2.set()
        await asyncio.wait_for(reset, 1)
        assert not second.get_active_locks() and not second.get_tasks()
        await second.stop()
        store.close()
    with TemporaryDirectory() as tmp:
        asyncio.run(check(Path(tmp)))


def test_sqlite_scheduler_restart_and_configuration():
    async def check(root):
        store = Storage(root / "integration.db")
        services = {s.process_id: FakeRiskService() for s in store.get_process_specs()}
        orch = Orchestrator(services, store)
        assert next(s for s in orch.get_process_specs() if s.process_id == "DelayRisk").allow_parallel_dates
        scheduler = ReglamentScheduler(orch)
        assert scheduler.get_interval() == 15
        try:
            scheduler.set_interval(0)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid schedule interval accepted")
        scheduler.set_interval(1)
        scheduler.set_enabled(True)
        scheduler.set_enabled(True)
        loop = scheduler._loop_task
        await until(lambda: orch.get_metrics()["completed"] == 4)
        assert scheduler._loop_task is loop
        assert all(t.trigger == TriggerType.AUTO and t.initiated_by == "scheduler_bot" for t in orch.get_tasks())
        await scheduler.stop()
        assert not scheduler.is_enabled() and scheduler._loop_task is None
        assert store.foreign_keys_enabled() == 1 and store.journal_mode() == "wal"
        assert store.conflict_matrix_count() == 24
        assert store.get_before_after_comparison()[0]["completed_tasks"] == 4
        assert len(orch.get_gantt_data()) == 4
        prior = max(t.task_id for t in orch.get_tasks())
        now = datetime.now()
        assert abs((from_utc(to_utc(now)) - now).total_seconds()) < 0.001
        await orch.reset()
        assert len(store.list_tasks()) == 4 and not orch.get_tasks()
        await orch.stop()
        # An interrupted persisted request fails on restart rather than being replayed.
        interrupted = store.create_task(process_id="DelayRisk", target_date="2026-11-19", trigger_type="AUTO")
        restarted = Orchestrator(services, store)
        await restarted.start()
        assert store.get_task(interrupted)["status"] == "FAILED"
        new = await restarted.submit(request("DelayRisk"))
        assert new.task_id > max(prior, interrupted)
        await until(lambda: store.get_task(new.task_id)["status"] == "COMPLETED")
        assert restarted.get_tasks()[-1].queued_at.tzinfo is None
        assert store.get_task(new.task_id)["queued_at"].endswith("Z")
        await restarted.stop()
        store.close()
    with TemporaryDirectory() as tmp:
        asyncio.run(check(Path(tmp)))
