"""Интеграционные тесты: синтетика (seed=42), 5 сценариев AS-IS/TO-BE, matplotlib Гант, монитор блокировок и Mock."""

from __future__ import annotations

import asyncio
import csv
import json
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from core.models import TaskRequest, TaskRun, TaskStatus, TriggerType
from core.orchestrator import Orchestrator
from core.storage import Storage
from main import bootstrap_environment
from services.microservices import SharedDataLayer, create_services
from services.mocks import MockOrchestrator
from services.scenarios import generate_random_batch, get_demo_scenarios
from ui.assets.theme import BG_PANEL, STATUS_COLORS
from ui.components.dashboard import summarize_kpi_metrics
from ui.components.event_log import compute_lock_monitor_rows, format_task_inspector_text
from ui.components.gantt_chart import build_gantt_figure
from ui.components.rules_view import analyze_pair_conflict


def test_synthetic_control_numbers_and_clean_bootstrap() -> None:
    """Проверка восстановления стенда с нуля и контрольных цифр seed=42."""
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        syn_dir = root / "synthetic_data"
        db_path = root / "data" / "orchestrator.db"

        store = bootstrap_environment(synthetic_dir=syn_dir, db_path=db_path, seed=42)
        try:
            assert db_path.exists()
            assert store.foreign_keys_enabled() == 1
            assert store.journal_mode() == "wal"
            assert store.conflict_matrix_count() == 24

            with (syn_dir / "warehouse_stocks.csv").open(encoding="utf-8") as f:
                stocks = list(csv.DictReader(f))
            with (syn_dir / "transit_telemetry.csv").open(encoding="utf-8") as f:
                transit = list(csv.DictReader(f))
            with (syn_dir / "weather_traffic.csv").open(encoding="utf-8") as f:
                weather = list(csv.DictReader(f))
            with (syn_dir / "supplier_sla.csv").open(encoding="utf-8") as f:
                sla = list(csv.DictReader(f))

            assert len(stocks) == 200
            assert len(transit) == 90
            assert len(weather) == 20
            assert len(sla) == 6
            assert len(stocks) + len(transit) + len(weather) + len(sla) == 316

            events = [
                json.loads(line)
                for line in (syn_dir / "legacy_system_logs.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            assert len(events) == 86
            levels = Counter(e["level"] for e in events)
            assert levels == {"INFO": 51, "WARN": 14, "ERROR": 21}
            err_codes = Counter(e["error_code"] for e in events if e.get("error_code"))
            assert err_codes == {
                "ERR_DEADLOCK_ABBA": 8,
                "ERR_CROSS_DATE_STAGING": 6,
                "ERR_LOST_UPDATE": 4,
                "ERR_DIRTY_READ_GLOBAL": 3,
            }

            scenarios = get_demo_scenarios(syn_dir / "scenarios.json")
            assert [s.scenario_id for s in scenarios] == [
                "s1_manual_vs_reglament",
                "s2_abba_deadlock",
                "s3_cross_date_collision",
                "s4_parallel_safe",
                "s5_multiuser_storm",
            ]
            assert len(scenarios[4].tasks) == 10
            assert len(generate_random_batch(15, seed=42)) == 15
        finally:
            store.close()


def test_five_canonical_scenarios_as_is_vs_to_be() -> None:
    """Проверка всех 5 сценариев: TO-BE (0 сбоев, integrity 100%, s4 параллельно) vs AS-IS (все классы сбоев)."""
    async def check() -> None:
        scenarios = get_demo_scenarios()

        # TO-BE (orchestration_enabled = True)
        for sc in scenarios:
            with TemporaryDirectory() as tmp:
                store = Storage(Path(tmp) / "on.db")
                db = SharedDataLayer(time_scale=0.02)
                orch = Orchestrator(create_services(db), store, orchestration_enabled=True)
                reqs = [replace(t, delay_sec=t.delay_sec * 0.02) for t in sc.tasks]
                await orch.submit_batch(reqs)
                while True:
                    m = orch.get_metrics()
                    if m["queued"] == 0 and m["running"] == 0:
                        break
                    await asyncio.sleep(0.002)
                m = orch.get_metrics()
                assert m["failed"] == 0, f"{sc.scenario_id} failed in TO-BE mode: {m}"
                assert m["deadlocks"] == 0
                assert m["integrity_score"] == 100.0
                if sc.scenario_id in ("s1_manual_vs_reglament", "s5_multiuser_storm"):
                    assert m["deduplicated_count"] >= 1
                if sc.scenario_id == "s4_parallel_safe":
                    assert m["max_parallelism"] == 3
                await orch.stop()
                store.close()

        # AS-IS (orchestration_enabled = False)
        total_deadlocks = 0
        total_corruptions = 0
        total_cross_date = 0
        saw_degraded_integrity = False

        for sc in scenarios:
            if sc.scenario_id == "s4_parallel_safe":
                continue
            with TemporaryDirectory() as tmp:
                store = Storage(Path(tmp) / "off.db")
                db = SharedDataLayer(time_scale=0.02)
                orch = Orchestrator(create_services(db), store, orchestration_enabled=False)
                reqs = [replace(t, delay_sec=t.delay_sec * 0.02) for t in sc.tasks]
                await orch.submit_batch(reqs)
                while True:
                    m = orch.get_metrics()
                    if m["queued"] == 0 and m["running"] == 0:
                        break
                    await asyncio.sleep(0.002)
                m = orch.get_metrics()
                total_deadlocks += m["deadlocks"]
                total_corruptions += m["corruptions"]
                total_cross_date += m["cross_date_collisions"]
                if m["integrity_score"] < 100.0:
                    saw_degraded_integrity = True
                await orch.stop()
                store.close()

        assert total_deadlocks >= 1
        assert total_corruptions >= 2
        assert total_cross_date >= 1
        assert saw_degraded_integrity

    asyncio.run(check())


def test_manual_priority_levels_and_full_stand_reset() -> None:
    """Проверка ручных приоритетов 1..3 (Высокий/Обычный/Фоновый) и очистки БД clear_storage=True."""
    async def check() -> None:
        with TemporaryDirectory() as tmp:
            store = Storage(Path(tmp) / "prio.db")
            db = SharedDataLayer(time_scale=0.01)
            orch = Orchestrator(create_services(db), store)
            for p, d in ((1, "2026-11-18"), (2, "2026-11-19"), (3, "2026-11-22")):
                t = await orch.submit(TaskRequest("SpoilageRisk", d, TriggerType.MANUAL, priority=p))
                assert t.priority == p
            while orch.get_metrics()["completed"] < 3:
                await asyncio.sleep(0.005)
            assert len(store.list_tasks()) == 3
            await orch.reset(clear_storage=True)
            assert len(store.list_tasks()) == 0
            assert not orch.get_tasks()
            await orch.stop()
            store.close()

    asyncio.run(check())


def test_matplotlib_gantt_lock_monitor_and_ui_viewmodels() -> None:
    """Проверка построения matplotlib Ганта (#161B22), монитора блокировок, KPI и standalone MockOrchestrator."""
    t0 = datetime(2026, 11, 20, 12, 0, 0)
    rows = [
        {
            "task_id": 1,
            "process_id": "StockoutRisk",
            "target_date": "2026-11-20",
            "trigger": "AUTO",
            "priority": 1,
            "status": "COMPLETED",
            "progress": 100,
            "queued_at": t0,
            "started_at": t0 + timedelta(seconds=0.1),
            "finished_at": t0 + timedelta(seconds=3.9),
        },
        {
            "task_id": 2,
            "process_id": "SpoilageRisk",
            "target_date": "2026-11-20",
            "trigger": "MANUAL",
            "priority": 2,
            "status": "RUNNING",
            "progress": 60,
            "queued_at": t0 + timedelta(seconds=0.2),
            "started_at": t0 + timedelta(seconds=3.9),
            "finished_at": None,
        },
        {
            "task_id": 3,
            "process_id": "StockoutRisk",
            "target_date": "2026-11-20",
            "trigger": "MANUAL",
            "priority": 2,
            "status": "DEDUPLICATED",
            "progress": 0,
            "queued_at": t0 + timedelta(seconds=0.4),
            "started_at": None,
            "finished_at": t0 + timedelta(seconds=0.4),
        },
    ]
    fig, vis_ids = build_gantt_figure(
        rows, now=t0 + timedelta(seconds=5.5), selected_task_id=2
    )
    assert vis_ids == [1, 2, 3]
    assert fig.get_facecolor()[:3] == tuple(int(BG_PANEL[i : i + 2], 16) / 255.0 for i in (1, 3, 5))
    assert set(STATUS_COLORS).issuperset(
        {"RUNNING", "QUEUED", "COMPLETED", "FAILED", "DEDUPLICATED", "CANCELLED"}
    )

    task1 = TaskRun(
        task_id=1,
        process_id="StockoutRisk",
        target_date="2026-11-20",
        trigger=TriggerType.AUTO,
        priority=1,
        status=TaskStatus.RUNNING,
        progress=60,
        queued_at=t0,
        started_at=t0,
        result={"stockout_count": 4},
    )
    active_locks = {
        "WRITE:SOURCE:WarehouseStocks:2026-11-20": [1],
        "READ:SOURCE:TransitTelemetry:2026-11-20": [1, 4],
        "WRITE:STAGING:staging_carrier_sla_rolling:GLOBAL": [5],
    }
    monitor = dict((name, (mode, badge)) for name, mode, badge in compute_lock_monitor_rows(active_locks, {1: task1}))
    assert monitor["WarehouseStocks"] == ("EXCLUSIVE", "[EXCLUSIVE: #1 StockoutRisk]")
    assert monitor["TransitTelemetry"] == ("SHARED", "[SHARED: #1, #4]")
    assert monitor["WeatherAndTraffic"] == ("FREE", "[FREE]")
    assert monitor["Staging-буфер"][0] == "EXCLUSIVE"

    insp_txt = format_task_inspector_text(task1, active_locks)
    assert "TASK #1" in insp_txt and "stockout_count" in insp_txt

    with TemporaryDirectory() as tmp:
        store = Storage(Path(tmp) / "matrix.db")
        try:
            specs = {s.process_id: s for s in store.get_process_specs()}
            assert analyze_pair_conflict(specs["StockoutRisk"], specs["DelayRisk"]) == ("AB-BA DEADLOCK", "FAILED")
            assert analyze_pair_conflict(specs["SpoilageRisk"], specs["SpoilageRisk"]) == ("DIFF DATES OK", "COMPLETED")

            t_start = time.perf_counter()
            for _ in range(40):
                tid = store.create_task(
                    process_id="DelayRisk", target_date="2026-11-20", trigger_type="MANUAL"
                )
                _ = store.get_task(tid)
            assert ((time.perf_counter() - t_start) * 1000.0 / 80.0) < 2.0
        finally:
            store.close()

    # Standalone MockOrchestrator sanity check
    async def check_mock() -> None:
        mock = MockOrchestrator()
        await mock.start()
        t = await mock.submit(TaskRequest("DelayRisk", "2026-11-20", TriggerType.MANUAL, priority=1))
        assert await mock.cancel(t.task_id)
        kpi = summarize_kpi_metrics(mock.get_tasks(), mock.get_metrics())
        assert kpi["integrity_score"] == 100.0
        await mock.stop()

    asyncio.run(check_mock())
