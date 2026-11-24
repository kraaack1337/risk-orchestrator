"""Слой доступа к данным на стандартном sqlite3.

Единственное постоянное WAL-соединение, создаваемое с ``isolation_level=None``.
Все многооператорные записи управляются явно через ``BEGIN`` / ``COMMIT`` /
``ROLLBACK``, а ``PRAGMA foreign_keys = ON`` повторно подтверждается после
``executescript()`` для схемы.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

# ---------------------------------------------------------------------------
# Декларативные seed-контракты (источник истины для RWLockManager/планировщика).
# ---------------------------------------------------------------------------
_PROCESSES: tuple[dict[str, Any], ...] = (
    {
        "process_id": "StockoutRisk",
        "display_name": "Оценка риска дефицита (StockoutRisk)",
        "description": "Пересчёт stockout_risk_flag по витрине WarehouseStocks.",
        "allow_parallel_dates": 0,
        "uses_shared_staging": 0,
        "staging_key": None,
        "default_priority": 1,
        "timeout_sec": 30.0,
        "estimated_duration_s": 4.0,
        "is_active": 1,
    },
    {
        "process_id": "DelayRisk",
        "display_name": "Оценка риска задержки (DelayRisk)",
        "description": "Пересчёт delay_risk_score по витрине TransitTelemetry.",
        "allow_parallel_dates": 1,
        "uses_shared_staging": 0,
        "staging_key": None,
        "default_priority": 1,
        "timeout_sec": 30.0,
        "estimated_duration_s": 4.0,
        "is_active": 1,
    },
    {
        "process_id": "SpoilageRisk",
        "display_name": "Оценка риска порчи (SpoilageRisk)",
        "description": "Пересчёт spoilage_risk_flag по WarehouseStocks и WeatherAndTraffic.",
        "allow_parallel_dates": 1,
        "uses_shared_staging": 0,
        "staging_key": None,
        "default_priority": 2,
        "timeout_sec": 30.0,
        "estimated_duration_s": 4.0,
        "is_active": 1,
    },
    {
        "process_id": "CarrierDefaultRisk",
        "display_name": "Оценка риска дефолта перевозчика (CarrierDefaultRisk)",
        "description": "Пересчёт глобальной витрины SupplierSLA через shared staging.",
        "allow_parallel_dates": 0,
        "uses_shared_staging": 1,
        "staging_key": "staging_carrier_sla_rolling",
        "default_priority": 2,
        "timeout_sec": 30.0,
        "estimated_duration_s": 5.0,
        "is_active": 1,
    },
)

_DATA_SOURCES: tuple[dict[str, str], ...] = (
    {
        "source_id": "WarehouseStocks",
        "display_name": "Витрина остатков складов",
        "table_or_file_name": "warehouse_stocks.csv",
        "granularity": "DATE_PARTITIONED",
        "description": "Остатки, резервы и флаги рисков по складам/SKU/датам.",
    },
    {
        "source_id": "TransitTelemetry",
        "display_name": "Витрина телеметрии рейсов",
        "table_or_file_name": "transit_telemetry.csv",
        "granularity": "DATE_PARTITIONED",
        "description": "Телеметрия поставок и оценка риска задержки.",
    },
    {
        "source_id": "WeatherAndTraffic",
        "display_name": "Витрина погоды и трафика",
        "table_or_file_name": "weather_traffic.csv",
        "granularity": "DATE_PARTITIONED",
        "description": "Справочник погодных условий и загруженности коридоров.",
    },
    {
        "source_id": "SupplierSLA",
        "display_name": "Глобальная витрина SLA перевозчиков",
        "table_or_file_name": "supplier_sla.csv",
        "granularity": "GLOBAL_CUMULATIVE",
        "description": "Кумулятивные метрики SLA без партиционирования по датам.",
    },
)

# Матрица доступов: 12 правил → 24 ресурсных пересечения в v_conflict_matrix.
# У StockoutRisk и DelayRisk намеренно противоположный acquire_order для
# WarehouseStocks <-> TransitTelemetry, чтобы воспроизводить AB-BA deadlock.
_PROCESS_SOURCE_RULES: tuple[dict[str, Any], ...] = (
    # StockoutRisk
    {"process_id": "StockoutRisk", "source_id": "WarehouseStocks", "access_mode": "WRITE", "lock_scope": "BY_DATE", "acquire_order": 1},
    {"process_id": "StockoutRisk", "source_id": "TransitTelemetry", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 2},
    {"process_id": "StockoutRisk", "source_id": "SupplierSLA", "access_mode": "READ", "lock_scope": "GLOBAL", "acquire_order": 3},
    # DelayRisk (противоположный порядок захвата для AB-BA)
    {"process_id": "DelayRisk", "source_id": "TransitTelemetry", "access_mode": "WRITE", "lock_scope": "BY_DATE", "acquire_order": 1},
    {"process_id": "DelayRisk", "source_id": "WarehouseStocks", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 2},
    {"process_id": "DelayRisk", "source_id": "WeatherAndTraffic", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 3},
    {"process_id": "DelayRisk", "source_id": "SupplierSLA", "access_mode": "READ", "lock_scope": "GLOBAL", "acquire_order": 4},
    # SpoilageRisk
    {"process_id": "SpoilageRisk", "source_id": "WarehouseStocks", "access_mode": "WRITE", "lock_scope": "BY_DATE", "acquire_order": 1},
    {"process_id": "SpoilageRisk", "source_id": "WeatherAndTraffic", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 2},
    # CarrierDefaultRisk
    {"process_id": "CarrierDefaultRisk", "source_id": "SupplierSLA", "access_mode": "WRITE", "lock_scope": "GLOBAL", "acquire_order": 1},
    {"process_id": "CarrierDefaultRisk", "source_id": "TransitTelemetry", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 2},
    {"process_id": "CarrierDefaultRisk", "source_id": "WeatherAndTraffic", "access_mode": "READ", "lock_scope": "BY_DATE", "acquire_order": 3},
)

_USERS: tuple[dict[str, Any], ...] = (
    {"user_id": "scheduler_bot", "display_name": "Авторегламент (Scheduler Bot)", "role": "SYSTEM_SCHEDULER", "max_concurrent_tasks": 10},
    {"user_id": "analyst_1", "display_name": "Аналитик 1", "role": "RISK_ANALYST", "max_concurrent_tasks": 5},
    {"user_id": "analyst_2", "display_name": "Аналитик 2", "role": "RISK_ANALYST", "max_concurrent_tasks": 5},
    {"user_id": "manager_1", "display_name": "Менеджер 1", "role": "RISK_MANAGER", "max_concurrent_tasks": 5},
    {"user_id": "admin", "display_name": "Администратор", "role": "ADMIN", "max_concurrent_tasks": 5},
)

# Дефолтное регламентное расписание (одна запись на процесс, изначально выключено).
_SCHEDULES: tuple[dict[str, Any], ...] = (
    {"process_id": "StockoutRisk", "interval_sec": 15.0, "target_date_offset": 0, "priority": 1, "is_enabled": 0},
    {"process_id": "DelayRisk", "interval_sec": 15.0, "target_date_offset": 0, "priority": 1, "is_enabled": 0},
    {"process_id": "SpoilageRisk", "interval_sec": 15.0, "target_date_offset": 0, "priority": 1, "is_enabled": 0},
    {"process_id": "CarrierDefaultRisk", "interval_sec": 15.0, "target_date_offset": 0, "priority": 1, "is_enabled": 0},
)


class Storage:
    """Единое постоянное WAL-соединение к локальной SQLite-БД оркестратора."""

    def __init__(self, db_path: str | Path = "data/orchestrator.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # isolation_level=None отключает автозапуск транзакций Python,
        # благодаря чему PRAGMA foreign_keys = ON реально применяется.
        self._conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        self._conn.row_factory = sqlite3.Row

        self._apply_pragmas()
        self._conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
        # executescript() неявно коммитит и сбрасывает PRAGMA-контекст.
        self._conn.execute("PRAGMA foreign_keys = ON;")

        self.seed_metadata()

    # ------------------------------------------------------------------ #
    # Инициализация и диагностика
    # ------------------------------------------------------------------ #
    def _apply_pragmas(self) -> None:
        """PRAGMA строго в порядке, зафиксированном архитектурным инвариантом."""
        self._conn.execute("PRAGMA journal_mode = WAL;")
        self._conn.execute("PRAGMA synchronous = NORMAL;")
        self._conn.execute("PRAGMA busy_timeout = 5000;")
        self._conn.execute("PRAGMA foreign_keys = ON;")

    def now_iso(self) -> str:
        """Текущее время в формате SQLite-дефолта ('YYYY-MM-DDTHH:MM:SS.sssZ')."""
        row = self._conn.execute("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')").fetchone()
        return str(row[0])

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Явное управление многооператорной записью через BEGIN/COMMIT/ROLLBACK."""
        self._conn.execute("BEGIN")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def foreign_keys_enabled(self) -> int:
        return int(self._conn.execute("PRAGMA foreign_keys").fetchone()[0])

    def journal_mode(self) -> str:
        return str(self._conn.execute("PRAGMA journal_mode").fetchone()[0])

    def conflict_matrix_count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM v_conflict_matrix").fetchone()[0])

    # ------------------------------------------------------------------ #
    # Низкоуровневые помощники
    # ------------------------------------------------------------------ #
    @staticmethod
    def _json_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _insert(self, table: str, data: dict[str, Any], *, or_ignore: bool = False) -> int:
        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        prefix = "INSERT OR IGNORE INTO" if or_ignore else "INSERT INTO"
        sql = f"{prefix} {table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor = self._conn.execute(sql, list(data.values()))
        return int(cursor.lastrowid)

    def _update(self, table: str, data: dict[str, Any], where_col: str, where_val: Any) -> int:
        if not data:
            return 0
        assignments = ", ".join(f"{col} = ?" for col in data)
        sql = f"UPDATE {table} SET {assignments} WHERE {where_col} = ?"
        cursor = self._conn.execute(sql, [*data.values(), where_val])
        return int(cursor.rowcount)

    def _delete(self, table: str, where_col: str, where_val: Any) -> int:
        cursor = self._conn.execute(f"DELETE FROM {table} WHERE {where_col} = ?", (where_val,))
        return int(cursor.rowcount)

    def _query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(sql, params).fetchall()]

    def _query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = self._conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    # ------------------------------------------------------------------ #
    # Идемпотентное наполнение справочников
    # ------------------------------------------------------------------ #
    def seed_metadata(self) -> None:
        """Заполняет декларативные справочники; безопасен для повторного вызова."""
        with self.transaction():
            for process in _PROCESSES:
                self._insert("processes", process, or_ignore=True)
            for source in _DATA_SOURCES:
                self._insert("data_sources", source, or_ignore=True)
            for rule in _PROCESS_SOURCE_RULES:
                self._insert("process_source_rules", rule, or_ignore=True)
            for user in _USERS:
                self._insert("users", user, or_ignore=True)
            for schedule in _SCHEDULES:
                existing = self._query_one(
                    "SELECT 1 FROM reglament_schedules WHERE process_id = ?",
                    (schedule["process_id"],),
                )
                if existing is None:
                    self._insert("reglament_schedules", schedule)

    # ------------------------------------------------------------------ #
    # CRUD: simulation_runs
    # ------------------------------------------------------------------ #
    def create_simulation_run(
        self,
        *,
        scenario_id: str | None = None,
        scenario_title: str | None = None,
        orchestration_enabled: int = 1,
        notes: str | None = None,
    ) -> int:
        return self._insert(
            "simulation_runs",
            {
                "scenario_id": scenario_id,
                "scenario_title": scenario_title,
                "orchestration_enabled": int(orchestration_enabled),
                "notes": notes,
            },
        )

    def get_simulation_run(self, run_id: int) -> dict[str, Any] | None:
        return self._query_one("SELECT * FROM simulation_runs WHERE run_id = ?", (run_id,))

    def list_simulation_runs(self) -> list[dict[str, Any]]:
        return self._query("SELECT * FROM simulation_runs ORDER BY run_id DESC")

    def update_simulation_run(self, run_id: int, **fields: Any) -> int:
        return self._update("simulation_runs", fields, "run_id", run_id)

    def finalize_simulation_run(self, run_id: int, **fields: Any) -> int:
        """Проставляет finished_at и агрегированные метрики прогона."""
        if "finished_at" not in fields:
            fields["finished_at"] = self.now_iso()
        return self.update_simulation_run(run_id, **fields)

    def delete_simulation_run(self, run_id: int) -> int:
        return self._delete("simulation_runs", "run_id", run_id)

    # ------------------------------------------------------------------ #
    # CRUD: task_runs
    # ------------------------------------------------------------------ #
    def create_task(
        self,
        *,
        process_id: str,
        target_date: str,
        trigger_type: str,
        initiated_by: str = "analyst_1",
        session_id: str | None = None,
        run_id: int | None = None,
        idempotency_key: str | None = None,
        priority: int = 2,
        effective_priority: float | None = None,
        status: str = "QUEUED",
        progress: int = 0,
        orchestration_enabled: int = 1,
        queued_at: str | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        blocked_by: str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        result_json: Any = None,
        wait_duration_ms: int | None = None,
        exec_duration_ms: int | None = None,
    ) -> int:
        data: dict[str, Any] = {
            "run_id": run_id,
            "process_id": process_id,
            "target_date": target_date,
            "trigger_type": trigger_type,
            "initiated_by": initiated_by,
            "session_id": session_id,
            "idempotency_key": idempotency_key,
            "priority": int(priority),
            "effective_priority": float(effective_priority if effective_priority is not None else priority),
            "status": status,
            "progress": int(progress),
            "orchestration_enabled": int(orchestration_enabled),
            "queued_at": queued_at or self.now_iso(),
            "started_at": started_at,
            "finished_at": finished_at,
            "wait_duration_ms": wait_duration_ms,
            "exec_duration_ms": exec_duration_ms,
            "blocked_by": blocked_by,
            "error_type": error_type,
            "error_message": error_message,
            "result_json": self._json_text(result_json),
        }
        return self._insert("task_runs", data)

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        return self._query_one("SELECT * FROM task_runs WHERE task_id = ?", (task_id,))

    def list_tasks(
        self,
        *,
        run_id: int | None = None,
        status: str | None = None,
        process_id: str | None = None,
        target_date: str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if process_id is not None:
            clauses.append("process_id = ?")
            params.append(process_id)
        if target_date is not None:
            clauses.append("target_date = ?")
            params.append(target_date)

        sql = "SELECT * FROM task_runs"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        return self._query(sql, tuple(params))

    def update_task(self, task_id: int, **fields: Any) -> int:
        return self._update("task_runs", fields, "task_id", task_id)

    def update_task_status(self, task_id: int, status: str, **fields: Any) -> int:
        fields["status"] = status
        return self.update_task(task_id, **fields)

    def cancel_task(self, task_id: int) -> bool:
        """Переводит задачу из QUEUED в CANCELLED (идемпотентно)."""
        cursor = self._conn.execute(
            "UPDATE task_runs SET status = 'CANCELLED', finished_at = ? "
            "WHERE task_id = ? AND status = 'QUEUED'",
            (self.now_iso(), task_id),
        )
        return cursor.rowcount > 0

    def delete_task(self, task_id: int) -> int:
        return self._delete("task_runs", "task_id", task_id)

    # ------------------------------------------------------------------ #
    # CRUD: lock_audit
    # ------------------------------------------------------------------ #
    def create_lock_event(
        self,
        *,
        task_id: int,
        lock_key: str,
        lock_mode: str,
        resource_type: str,
        action: str,
        run_id: int | None = None,
        blocking_task_ids: Any = None,
        occurred_at: str | None = None,
        hold_duration_ms: int | None = None,
    ) -> int:
        return self._insert(
            "lock_audit",
            {
                "run_id": run_id,
                "task_id": task_id,
                "lock_key": lock_key,
                "lock_mode": lock_mode,
                "resource_type": resource_type,
                "action": action,
                "blocking_task_ids": self._json_text(blocking_task_ids),
                "occurred_at": occurred_at or self.now_iso(),
                "hold_duration_ms": hold_duration_ms,
            },
        )

    def get_lock_event(self, lock_event_id: int) -> dict[str, Any] | None:
        return self._query_one("SELECT * FROM lock_audit WHERE lock_event_id = ?", (lock_event_id,))

    def list_lock_events(
        self,
        *,
        task_id: int | None = None,
        run_id: int | None = None,
        lock_key: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if lock_key is not None:
            clauses.append("lock_key = ?")
            params.append(lock_key)
        sql = "SELECT * FROM lock_audit"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY lock_event_id ASC"
        return self._query(sql, tuple(params))

    def delete_lock_event(self, lock_event_id: int) -> int:
        return self._delete("lock_audit", "lock_event_id", lock_event_id)

    def delete_lock_events_for_task(self, task_id: int) -> int:
        return self._delete("lock_audit", "task_id", task_id)

    # ------------------------------------------------------------------ #
    # CRUD: orchestrator_events
    # ------------------------------------------------------------------ #
    def create_event(
        self,
        *,
        message: str,
        level: str = "INFO",
        event_code: str = "GENERAL",
        run_id: int | None = None,
        task_id: int | None = None,
        user_id: str | None = None,
        payload: Any = None,
        timestamp: str | None = None,
    ) -> int:
        return self._insert(
            "orchestrator_events",
            {
                "run_id": run_id,
                "timestamp": timestamp or self.now_iso(),
                "level": level,
                "event_code": event_code,
                "task_id": task_id,
                "user_id": user_id,
                "message": message,
                "payload_json": self._json_text(payload),
            },
        )

    def get_event(self, event_id: int) -> dict[str, Any] | None:
        return self._query_one("SELECT * FROM orchestrator_events WHERE event_id = ?", (event_id,))

    def list_events(
        self,
        *,
        run_id: int | None = None,
        task_id: int | None = None,
        level: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        if level is not None:
            clauses.append("level = ?")
            params.append(level)
        sql = "SELECT * FROM orchestrator_events"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY event_id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        return self._query(sql, tuple(params))

    def delete_event(self, event_id: int) -> int:
        return self._delete("orchestrator_events", "event_id", event_id)

    # ------------------------------------------------------------------ #
    # CRUD: user_sessions
    # ------------------------------------------------------------------ #
    def create_session(self, *, session_id: str, user_id: str, client_id: str) -> int:
        return self._insert(
            "user_sessions",
            {"session_id": session_id, "user_id": user_id, "client_id": client_id},
        )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self._query_one("SELECT * FROM user_sessions WHERE session_id = ?", (session_id,))

    def list_sessions(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        if active_only:
            return self._query(
                "SELECT * FROM user_sessions WHERE is_active = 1 ORDER BY connected_at ASC"
            )
        return self._query("SELECT * FROM user_sessions ORDER BY connected_at ASC")

    def mark_session_disconnected(self, session_id: str) -> int:
        return self._update(
            "user_sessions",
            {"is_active": 0, "disconnected_at": self.now_iso()},
            "session_id",
            session_id,
        )

    def update_session(self, session_id: str, **fields: Any) -> int:
        return self._update("user_sessions", fields, "session_id", session_id)

    def delete_session(self, session_id: str) -> int:
        return self._delete("user_sessions", "session_id", session_id)

    # ------------------------------------------------------------------ #
    # CRUD: reglament_schedules
    # ------------------------------------------------------------------ #
    def create_schedule(
        self,
        *,
        process_id: str,
        interval_sec: float = 15.0,
        target_date_offset: int = 0,
        priority: int = 1,
        is_enabled: int = 0,
        next_trigger_at: str | None = None,
    ) -> int:
        return self._insert(
            "reglament_schedules",
            {
                "process_id": process_id,
                "interval_sec": interval_sec,
                "target_date_offset": target_date_offset,
                "priority": priority,
                "is_enabled": int(is_enabled),
                "next_trigger_at": next_trigger_at,
            },
        )

    def get_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        return self._query_one(
            "SELECT * FROM reglament_schedules WHERE schedule_id = ?", (schedule_id,)
        )

    def list_schedules(self) -> list[dict[str, Any]]:
        return self._query(
            "SELECT s.*, p.display_name AS process_name "
            "FROM reglament_schedules s JOIN processes p ON p.process_id = s.process_id "
            "ORDER BY s.schedule_id ASC"
        )

    def update_schedule(self, schedule_id: int, **fields: Any) -> int:
        return self._update("reglament_schedules", fields, "schedule_id", schedule_id)

    def delete_schedule(self, schedule_id: int) -> int:
        return self._delete("reglament_schedules", "schedule_id", schedule_id)

    # ------------------------------------------------------------------ #
    # Чтение аналитических VIEW
    # ------------------------------------------------------------------ #
    def get_gantt_timeline(self, run_id: int | None = None) -> list[dict[str, Any]]:
        if run_id is not None:
            return self._query(
                "SELECT * FROM v_gantt_timeline WHERE run_id = ?", (run_id,)
            )
        return self._query("SELECT * FROM v_gantt_timeline")

    def get_before_after_comparison(self) -> list[dict[str, Any]]:
        return self._query("SELECT * FROM v_before_after_comparison")

    def get_conflict_matrix(self) -> list[dict[str, Any]]:
        return self._query("SELECT * FROM v_conflict_matrix")
