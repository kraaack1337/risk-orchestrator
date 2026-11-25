PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

-- ============================================================================
-- Оркестратор оценки логистических рисков — нормализованная схема SQLite.
-- 4 логических слоя · 15 таблиц · 10 индексов · 3 аналитических VIEW.
-- Ссылочная целостность: PRAGMA foreign_keys = ON.
-- Конкурентное чтение при записи: PRAGMA journal_mode = WAL, busy_timeout = 5000.
-- ============================================================================

-- 1. СЛОЙ МЕТАДАННЫХ И РЕСУРСНЫХ КОНТРАКТОВ (Metadata Layer)
CREATE TABLE IF NOT EXISTS processes (
    process_id           TEXT PRIMARY KEY,
    display_name         TEXT NOT NULL,
    description          TEXT NOT NULL DEFAULT '',
    allow_parallel_dates INTEGER NOT NULL DEFAULT 0 CHECK (allow_parallel_dates IN (0, 1)),
    uses_shared_staging  INTEGER NOT NULL DEFAULT 0 CHECK (uses_shared_staging IN (0, 1)),
    staging_key          TEXT,
    default_priority     INTEGER NOT NULL DEFAULT 2 CHECK (default_priority BETWEEN 1 AND 5),
    timeout_sec          REAL NOT NULL DEFAULT 30.0 CHECK (timeout_sec > 0),
    estimated_duration_s REAL NOT NULL DEFAULT 4.0 CHECK (estimated_duration_s > 0),
    is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS data_sources (
    source_id            TEXT PRIMARY KEY,
    display_name         TEXT NOT NULL,
    table_or_file_name   TEXT NOT NULL,
    granularity          TEXT NOT NULL CHECK (granularity IN ('DATE_PARTITIONED', 'GLOBAL_CUMULATIVE')),
    description          TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS process_source_rules (
    rule_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id           TEXT NOT NULL REFERENCES processes(process_id) ON DELETE CASCADE,
    source_id            TEXT NOT NULL REFERENCES data_sources(source_id) ON DELETE CASCADE,
    access_mode          TEXT NOT NULL CHECK (access_mode IN ('READ', 'WRITE', 'READ_WRITE')),
    lock_scope           TEXT NOT NULL CHECK (lock_scope IN ('BY_DATE', 'GLOBAL')),
    acquire_order        INTEGER NOT NULL DEFAULT 1 CHECK (acquire_order >= 1),
    UNIQUE (process_id, source_id)
);

-- 2. СЛОЙ ПОЛЬЗОВАТЕЛЕЙ, СЕССИЙ И РЕГЛАМЕНТА (Users & Schedules Layer)
CREATE TABLE IF NOT EXISTS users (
    user_id              TEXT PRIMARY KEY,
    display_name         TEXT NOT NULL,
    role                 TEXT NOT NULL CHECK (role IN ('RISK_ANALYST', 'RISK_MANAGER', 'ADMIN', 'SYSTEM_SCHEDULER')),
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 5 CHECK (max_concurrent_tasks >= 1),
    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id           TEXT PRIMARY KEY,
    user_id              TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    client_id            TEXT NOT NULL,
    connected_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    disconnected_at      TEXT,
    is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS reglament_schedules (
    schedule_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id           TEXT NOT NULL REFERENCES processes(process_id) ON DELETE CASCADE,
    interval_sec         REAL NOT NULL DEFAULT 15.0 CHECK (interval_sec >= 1.0),
    target_date_offset   INTEGER NOT NULL DEFAULT 0,
    priority             INTEGER NOT NULL DEFAULT 1 CHECK (priority BETWEEN 1 AND 5),
    is_enabled           INTEGER NOT NULL DEFAULT 0 CHECK (is_enabled IN (0, 1)),
    last_triggered_at    TEXT,
    next_trigger_at      TEXT
);

-- 3. ОПЕРАЦИОННЫЙ СЛОЙ ОРКЕСТРАЦИИ (Runtime & Audit Layer)
CREATE TABLE IF NOT EXISTS simulation_runs (
    run_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id           TEXT,
    scenario_title        TEXT,
    orchestration_enabled INTEGER NOT NULL CHECK (orchestration_enabled IN (0, 1)),
    started_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    finished_at           TEXT,
    total_tasks           INTEGER NOT NULL DEFAULT 0,
    completed_tasks       INTEGER NOT NULL DEFAULT 0,
    failed_tasks          INTEGER NOT NULL DEFAULT 0,
    cancelled_tasks       INTEGER NOT NULL DEFAULT 0,
    deadlock_count        INTEGER NOT NULL DEFAULT 0,
    data_corruption_count INTEGER NOT NULL DEFAULT 0,
    cross_date_collisions INTEGER NOT NULL DEFAULT 0,
    avg_wait_sec          REAL NOT NULL DEFAULT 0.0,
    max_wait_sec          REAL NOT NULL DEFAULT 0.0,
    total_makespan_sec    REAL NOT NULL DEFAULT 0.0,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    task_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                INTEGER REFERENCES simulation_runs(run_id) ON DELETE SET NULL,
    process_id            TEXT NOT NULL REFERENCES processes(process_id),
    target_date           TEXT NOT NULL,
    trigger_type          TEXT NOT NULL CHECK (trigger_type IN ('AUTO', 'MANUAL')),
    initiated_by          TEXT NOT NULL DEFAULT 'analyst_1' REFERENCES users(user_id),
    session_id            TEXT REFERENCES user_sessions(session_id) ON DELETE SET NULL,
    idempotency_key       TEXT,
    priority              INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 5),
    effective_priority    REAL NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'DEDUPLICATED')),
    progress              INTEGER NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    orchestration_enabled INTEGER NOT NULL DEFAULT 1 CHECK (orchestration_enabled IN (0, 1)),
    queued_at             TEXT NOT NULL,
    started_at            TEXT,
    finished_at           TEXT,
    wait_duration_ms      INTEGER,
    exec_duration_ms      INTEGER,
    blocked_by            TEXT,
    error_type            TEXT CHECK (error_type IN (NULL, 'DeadlockError', 'DataCorruptionError', 'CrossDateCollisionError', 'TimeoutError', 'CancelledError', 'RuntimeError')),
    error_message         TEXT,
    result_json           TEXT
);

CREATE TABLE IF NOT EXISTS lock_audit (
    lock_event_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                INTEGER REFERENCES simulation_runs(run_id) ON DELETE SET NULL,
    task_id               INTEGER NOT NULL REFERENCES task_runs(task_id) ON DELETE CASCADE,
    lock_key              TEXT NOT NULL,
    lock_mode             TEXT NOT NULL CHECK (lock_mode IN ('SHARED', 'EXCLUSIVE')),
    resource_type         TEXT NOT NULL CHECK (resource_type IN ('SOURCE_DATE', 'SOURCE_GLOBAL', 'PROCESS_STAGING')),
    action                TEXT NOT NULL CHECK (action IN ('ACQUIRED', 'WAIT_CONFLICT', 'RELEASED', 'EXPIRED_TTL')),
    blocking_task_ids     TEXT,
    occurred_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    hold_duration_ms      INTEGER
);

CREATE TABLE IF NOT EXISTS orchestrator_events (
    event_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                INTEGER REFERENCES simulation_runs(run_id) ON DELETE SET NULL,
    timestamp             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    level                 TEXT NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARN', 'ERROR')),
    event_code            TEXT NOT NULL DEFAULT 'GENERAL',
    task_id               INTEGER REFERENCES task_runs(task_id) ON DELETE SET NULL,
    user_id               TEXT REFERENCES users(user_id) ON DELETE SET NULL,
    message               TEXT NOT NULL,
    payload_json          TEXT
);

-- 4. ДОМЕННЫЙ СЛОЙ (ВИТРИНЫ И STAGING-БУФЕР)
CREATE TABLE IF NOT EXISTS dom_warehouse_stocks (
    record_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    business_date         TEXT NOT NULL,
    warehouse_id          TEXT NOT NULL,
    sku_id                TEXT NOT NULL,
    sku_category          TEXT NOT NULL,
    qty_on_hand           INTEGER NOT NULL CHECK (qty_on_hand >= 0),
    qty_reserved          INTEGER NOT NULL DEFAULT 0,
    reorder_point         INTEGER NOT NULL,
    avg_daily_demand      REAL NOT NULL,
    expiry_date           TEXT,
    temp_celsius          REAL,
    stockout_risk_flag    INTEGER NOT NULL DEFAULT 0,
    spoilage_risk_flag    INTEGER NOT NULL DEFAULT 0,
    row_version           INTEGER NOT NULL DEFAULT 1,
    updated_by_task_id    INTEGER,
    UNIQUE (business_date, warehouse_id, sku_id)
);

CREATE TABLE IF NOT EXISTS dom_transit_telemetry (
    shipment_id           TEXT NOT NULL,
    business_date         TEXT NOT NULL,
    carrier_id            TEXT NOT NULL,
    warehouse_id          TEXT NOT NULL,
    sku_id                TEXT NOT NULL,
    corridor_id           TEXT NOT NULL,
    planned_eta_hours     REAL NOT NULL,
    actual_eta_hours      REAL NOT NULL,
    cargo_qty             INTEGER NOT NULL,
    cargo_temp_celsius    REAL,
    gps_status            TEXT NOT NULL CHECK (gps_status IN ('ON_TIME', 'DELAYED', 'STOPPED', 'REROUTED')),
    delay_risk_score      REAL NOT NULL DEFAULT 0.0,
    row_version           INTEGER NOT NULL DEFAULT 1,
    updated_by_task_id    INTEGER,
    PRIMARY KEY (business_date, shipment_id)
);

CREATE TABLE IF NOT EXISTS dom_weather_traffic (
    business_date         TEXT NOT NULL,
    corridor_id           TEXT NOT NULL,
    region_name           TEXT NOT NULL,
    weather_severity      REAL NOT NULL CHECK (weather_severity BETWEEN 0.0 AND 1.0),
    temp_ambient_celsius  REAL NOT NULL,
    traffic_congestion    REAL NOT NULL CHECK (traffic_congestion BETWEEN 0.0 AND 1.0),
    road_alert_code       TEXT NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (business_date, corridor_id)
);

CREATE TABLE IF NOT EXISTS dom_supplier_sla (
    carrier_id            TEXT PRIMARY KEY,
    carrier_name          TEXT NOT NULL,
    as_of_date            TEXT NOT NULL,
    rolling_window_days   INTEGER NOT NULL DEFAULT 30,
    on_time_rate          REAL NOT NULL CHECK (on_time_rate BETWEEN 0.0 AND 1.0),
    claim_rate            REAL NOT NULL CHECK (claim_rate BETWEEN 0.0 AND 1.0),
    active_shipments      INTEGER NOT NULL DEFAULT 0,
    default_risk_score    REAL NOT NULL DEFAULT 0.0,
    sla_tier              TEXT NOT NULL CHECK (sla_tier IN ('A', 'B', 'C', 'CRITICAL')),
    row_version           INTEGER NOT NULL DEFAULT 1,
    updated_by_task_id    INTEGER
);

CREATE TABLE IF NOT EXISTS dom_sla_staging_buffer (
    staging_row_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_task_id         INTEGER NOT NULL,
    business_date         TEXT NOT NULL,
    carrier_id            TEXT NOT NULL,
    metric_value          REAL NOT NULL,
    loaded_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 5. ИНДЕКСЫ
CREATE INDEX IF NOT EXISTS idx_task_runs_status_prio ON task_runs(status, effective_priority, queued_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_run_id      ON task_runs(run_id, queued_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_proc_date   ON task_runs(process_id, target_date, status);
CREATE INDEX IF NOT EXISTS idx_task_runs_idemp       ON task_runs(idempotency_key) WHERE status IN ('QUEUED', 'RUNNING');
CREATE INDEX IF NOT EXISTS idx_lock_audit_task       ON lock_audit(task_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_lock_audit_key        ON lock_audit(lock_key, action, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_run_ts         ON orchestrator_events(run_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_task           ON orchestrator_events(task_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dom_stocks_date_wh    ON dom_warehouse_stocks(business_date, warehouse_id);
CREATE INDEX IF NOT EXISTS idx_dom_transit_date_carr ON dom_transit_telemetry(business_date, carrier_id);

-- 6. АНАЛИТИЧЕСКИЕ ПРЕДСТАВЛЕНИЯ (VIEWS)
CREATE VIEW IF NOT EXISTS v_gantt_timeline AS
SELECT
    t.task_id,
    t.run_id,
    t.process_id,
    p.display_name AS process_name,
    t.target_date,
    t.trigger_type,
    t.initiated_by,
    t.status,
    t.priority,
    t.queued_at,
    t.started_at,
    t.finished_at,
    t.wait_duration_ms,
    t.exec_duration_ms,
    t.blocked_by,
    t.error_type
FROM task_runs t
JOIN processes p ON p.process_id = t.process_id
ORDER BY t.queued_at ASC, t.task_id ASC;

CREATE VIEW IF NOT EXISTS v_before_after_comparison AS
SELECT
    run_id,
    scenario_id,
    CASE orchestration_enabled WHEN 1 THEN 'WITH_ORCHESTRATOR' ELSE 'WITHOUT_ORCHESTRATOR' END AS mode,
    total_tasks,
    completed_tasks,
    failed_tasks,
    ROUND(100.0 * failed_tasks / NULLIF(total_tasks, 0), 1) AS error_rate_pct,
    deadlock_count,
    data_corruption_count,
    cross_date_collisions,
    ROUND(avg_wait_sec, 2) AS avg_wait_sec,
    ROUND(total_makespan_sec, 2) AS total_makespan_sec
FROM simulation_runs
ORDER BY run_id DESC;

CREATE VIEW IF NOT EXISTS v_conflict_matrix AS
SELECT
    r1.process_id AS process_a,
    r2.process_id AS process_b,
    r1.source_id  AS shared_source,
    r1.access_mode AS mode_a,
    r2.access_mode AS mode_b,
    CASE
        WHEN r1.access_mode = 'READ' AND r2.access_mode = 'READ' THEN 'COMPATIBLE_PARALLEL'
        WHEN r1.lock_scope = 'GLOBAL' OR r2.lock_scope = 'GLOBAL' THEN 'CONFLICT_GLOBAL_ALL_DATES'
        ELSE 'CONFLICT_SAME_DATE'
    END AS conflict_type
FROM process_source_rules r1
JOIN process_source_rules r2
  ON r1.source_id = r2.source_id
 AND r1.process_id <= r2.process_id;
