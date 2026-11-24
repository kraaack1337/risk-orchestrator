"""Проверка слоя БД: схема, PRAGMA, seed-метаданные и CRUD."""

from __future__ import annotations

import sqlite3

import pytest

from core.storage import Storage

EXPECTED_TABLES = {
    "processes",
    "data_sources",
    "process_source_rules",
    "users",
    "user_sessions",
    "reglament_schedules",
    "simulation_runs",
    "task_runs",
    "lock_audit",
    "orchestrator_events",
    "dom_warehouse_stocks",
    "dom_transit_telemetry",
    "dom_weather_traffic",
    "dom_supplier_sla",
    "dom_sla_staging_buffer",
}

EXPECTED_VIEWS = {"v_gantt_timeline", "v_before_after_comparison", "v_conflict_matrix"}

EXPECTED_INDEXES = {
    "idx_task_runs_status_prio",
    "idx_task_runs_run_id",
    "idx_task_runs_proc_date",
    "idx_task_runs_idemp",
    "idx_lock_audit_task",
    "idx_lock_audit_key",
    "idx_events_run_ts",
    "idx_events_task",
    "idx_dom_stocks_date_wh",
    "idx_dom_transit_date_carr",
}


@pytest.fixture()
def storage(tmp_path) -> Storage:
    store = Storage(tmp_path / "orchestrator.db")
    yield store
    store.close()


def _schema_items(store: Storage) -> dict[str, set[str]]:
    items = store._query(
        "SELECT name, type FROM sqlite_master "
        "WHERE type IN ('table', 'view', 'index') AND name NOT LIKE 'sqlite_%'"
    )
    return {
        kind: {row["name"] for row in items if row["type"] == kind}
        for kind in ("table", "view", "index")
    }


def test_pragmas_and_schema(storage: Storage) -> None:
    assert storage.foreign_keys_enabled() == 1
    assert storage.journal_mode() == "wal"

    items = _schema_items(storage)
    assert items["table"] == EXPECTED_TABLES
    assert items["view"] == EXPECTED_VIEWS
    assert items["index"] == EXPECTED_INDEXES


def test_conflict_matrix_returns_24(storage: Storage) -> None:
    assert storage.conflict_matrix_count() == 24


def test_seed_metadata_idempotent(storage: Storage) -> None:
    before_processes = storage._query("SELECT COUNT(*) AS c FROM processes")[0]["c"]
    before_rules = storage._query("SELECT COUNT(*) AS c FROM process_source_rules")[0]["c"]
    before_users = storage._query("SELECT COUNT(*) AS c FROM users")[0]["c"]
    before_schedules = storage._query("SELECT COUNT(*) AS c FROM reglament_schedules")[0]["c"]

    storage.seed_metadata()

    assert storage._query("SELECT COUNT(*) AS c FROM processes")[0]["c"] == before_processes
    assert storage._query("SELECT COUNT(*) AS c FROM process_source_rules")[0]["c"] == before_rules
    assert storage._query("SELECT COUNT(*) AS c FROM users")[0]["c"] == before_users
    assert storage._query("SELECT COUNT(*) AS c FROM reglament_schedules")[0]["c"] == before_schedules


def test_seed_metadata_content(storage: Storage) -> None:
    processes = {p["process_id"]: p for p in storage._query("SELECT * FROM processes")}
    assert set(processes) == {"StockoutRisk", "DelayRisk", "SpoilageRisk", "CarrierDefaultRisk"}
    assert processes["StockoutRisk"]["allow_parallel_dates"] == 0
    assert processes["DelayRisk"]["allow_parallel_dates"] == 1
    assert processes["SpoilageRisk"]["allow_parallel_dates"] == 1
    assert processes["CarrierDefaultRisk"]["allow_parallel_dates"] == 0
    assert processes["CarrierDefaultRisk"]["uses_shared_staging"] == 1
    assert processes["CarrierDefaultRisk"]["staging_key"] == "staging_carrier_sla_rolling"

    sources = {s["source_id"]: s for s in storage._query("SELECT * FROM data_sources")}
    assert sources["WarehouseStocks"]["granularity"] == "DATE_PARTITIONED"
    assert sources["TransitTelemetry"]["granularity"] == "DATE_PARTITIONED"
    assert sources["WeatherAndTraffic"]["granularity"] == "DATE_PARTITIONED"
    assert sources["SupplierSLA"]["granularity"] == "GLOBAL_CUMULATIVE"

    rules = storage._query(
        "SELECT * FROM process_source_rules WHERE process_id IN (?, ?) AND "
        "source_id IN (?, ?) ORDER BY process_id, acquire_order",
        ("StockoutRisk", "DelayRisk", "WarehouseStocks", "TransitTelemetry"),
    )
    stockout = [r for r in rules if r["process_id"] == "StockoutRisk"]
    delay = [r for r in rules if r["process_id"] == "DelayRisk"]
    # AB-BA: StockoutRisk берёт WarehouseStocks первым, DelayRisk — TransitTelemetry первым.
    assert stockout[0]["source_id"] == "WarehouseStocks"
    assert stockout[1]["source_id"] == "TransitTelemetry"
    assert delay[0]["source_id"] == "TransitTelemetry"
    assert delay[1]["source_id"] == "WarehouseStocks"

    users = {u["user_id"]: u["role"] for u in storage._query("SELECT user_id, role FROM users")}
    assert users == {
        "scheduler_bot": "SYSTEM_SCHEDULER",
        "analyst_1": "RISK_ANALYST",
        "analyst_2": "RISK_ANALYST",
        "manager_1": "RISK_MANAGER",
        "admin": "ADMIN",
    }

    schedules = storage.list_schedules()
    assert len(schedules) == 4
    assert all(s["is_enabled"] == 0 for s in schedules)


def test_simulation_run_crud(storage: Storage) -> None:
    run_id = storage.create_simulation_run(
        scenario_id="s1", scenario_title="Сценарий 1", orchestration_enabled=1
    )
    assert storage.get_simulation_run(run_id)["scenario_id"] == "s1"
    storage.update_simulation_run(run_id, total_tasks=3, completed_tasks=3)
    assert storage.get_simulation_run(run_id)["completed_tasks"] == 3
    storage.finalize_simulation_run(run_id, avg_wait_sec=0.5)
    assert storage.get_simulation_run(run_id)["finished_at"] is not None
    assert storage.list_simulation_runs()[0]["run_id"] == run_id
    storage.delete_simulation_run(run_id)
    assert storage.get_simulation_run(run_id) is None


def test_task_run_crud_and_cancel(storage: Storage) -> None:
    run_id = storage.create_simulation_run(orchestration_enabled=1)
    task_id = storage.create_task(
        process_id="StockoutRisk",
        target_date="2026-11-20",
        trigger_type="MANUAL",
        initiated_by="analyst_1",
        run_id=run_id,
        idempotency_key="stockout:2026-11-20",
        priority=2,
    )
    task = storage.get_task(task_id)
    assert task["status"] == "QUEUED"
    assert task["effective_priority"] == 2.0

    assert storage.cancel_task(task_id) is True
    assert storage.get_task(task_id)["status"] == "CANCELLED"
    assert storage.cancel_task(task_id) is False  # повторный cancel идемпотентен

    storage.update_task(task_id, blocked_by="[1]")
    assert storage.get_task(task_id)["blocked_by"] == "[1]"
    storage.delete_task(task_id)
    assert storage.get_task(task_id) is None


def test_lock_audit_and_events(storage: Storage) -> None:
    run_id = storage.create_simulation_run(orchestration_enabled=1)
    task_id = storage.create_task(
        process_id="StockoutRisk", target_date="2026-11-20", trigger_type="MANUAL", run_id=run_id
    )

    lock_id = storage.create_lock_event(
        task_id=task_id,
        run_id=run_id,
        lock_key="WarehouseStocks:2026-11-20",
        lock_mode="EXCLUSIVE",
        resource_type="SOURCE_DATE",
        action="ACQUIRED",
    )
    assert storage.get_lock_event(lock_id)["action"] == "ACQUIRED"
    assert len(storage.list_lock_events(task_id=task_id)) == 1

    event_id = storage.create_event(
        message="task queued", level="INFO", event_code="TASK_QUEUED", task_id=task_id, run_id=run_id
    )
    assert storage.get_event(event_id)["event_code"] == "TASK_QUEUED"
    assert len(storage.list_events(run_id=run_id)) == 1

    storage.delete_lock_events_for_task(task_id)
    assert storage.list_lock_events(task_id=task_id) == []


def test_user_sessions_crud(storage: Storage) -> None:
    storage.create_session(session_id="sess-1", user_id="analyst_1", client_id="client-1")
    assert storage.get_session("sess-1")["is_active"] == 1
    storage.mark_session_disconnected("sess-1")
    assert storage.get_session("sess-1")["is_active"] == 0
    assert storage.list_sessions(active_only=True) == []
    storage.delete_session("sess-1")
    assert storage.get_session("sess-1") is None


def test_schedules_crud(storage: Storage) -> None:
    schedule_id = storage.create_schedule(process_id="DelayRisk", interval_sec=20.0, is_enabled=1)
    assert storage.get_schedule(schedule_id)["is_enabled"] == 1
    storage.update_schedule(schedule_id, is_enabled=0)
    assert storage.get_schedule(schedule_id)["is_enabled"] == 0
    storage.delete_schedule(schedule_id)
    assert storage.get_schedule(schedule_id) is None


def test_foreign_keys_enforced(storage: Storage) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        storage.create_task(
            process_id="UnknownProcess", target_date="2026-11-20", trigger_type="MANUAL"
        )


def test_views_readable(storage: Storage) -> None:
    assert len(storage.get_conflict_matrix()) == 24
    assert isinstance(storage.get_before_after_comparison(), list)
    assert isinstance(storage.get_gantt_timeline(), list)
