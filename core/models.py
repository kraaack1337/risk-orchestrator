"""Shared contracts. UI timestamps are local naive; persistence uses UTC."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

STOCKOUT_RISK = "StockoutRisk"
DELAY_RISK = "DelayRisk"
SPOILAGE_RISK = "SpoilageRisk"
CARRIER_DEFAULT_RISK = "CarrierDefaultRisk"
PROCESS_IDS: tuple[str, ...] = (
    STOCKOUT_RISK,
    DELAY_RISK,
    SPOILAGE_RISK,
    CARRIER_DEFAULT_RISK,
)

WAREHOUSE_STOCKS = "WarehouseStocks"
TRANSIT_TELEMETRY = "TransitTelemetry"
WEATHER_AND_TRAFFIC = "WeatherAndTraffic"
SUPPLIER_SLA = "SupplierSLA"
SOURCE_IDS: tuple[str, ...] = (
    WAREHOUSE_STOCKS,
    TRANSIT_TELEMETRY,
    WEATHER_AND_TRAFFIC,
    SUPPLIER_SLA,
)


class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEDUPLICATED = "DEDUPLICATED"


class TriggerType(str, Enum):
    AUTO = "AUTO"
    MANUAL = "MANUAL"


class AccessMode(str, Enum):
    READ = "READ"
    WRITE = "WRITE"
    READ_WRITE = "READ_WRITE"


class LockMode(str, Enum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class LockScope(str, Enum):
    BY_DATE = "BY_DATE"
    GLOBAL = "GLOBAL"


@dataclass(frozen=True)
class ResourceRule:
    source_id: str
    access_mode: AccessMode
    lock_scope: LockScope = LockScope.BY_DATE
    acquire_order: int = 1


@dataclass
class ProcessSpec:
    process_id: str
    display_name: str
    read_sources: tuple[str, ...]
    write_sources: tuple[str, ...]
    allow_parallel_dates: bool = True
    default_priority: int = 2
    resource_rules: tuple[ResourceRule, ...] = ()
    uses_shared_staging: bool = False
    staging_key: str | None = None
    timeout_sec: float = 30.0
    estimated_duration_s: float = 4.0
    is_active: bool = True
    global_sources: tuple[str, ...] = (SUPPLIER_SLA,)


@dataclass
class TaskRequest:
    process_id: str
    target_date: str
    trigger: TriggerType
    priority: int | None = None
    delay_sec: float = 0.0
    initiated_by: str | None = None
    session_id: str | None = None
    idempotency_key: str | None = None


@dataclass
class TaskRun:
    task_id: int
    process_id: str
    target_date: str
    trigger: TriggerType
    priority: int
    status: TaskStatus
    progress: int
    queued_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    blocked_by: str | None = None
    error_message: str | None = None
    result: dict[str, Any] | None = None
    initiated_by: str = "analyst_1"
    session_id: str | None = None
    run_id: int | None = None
    effective_priority: float | None = None
    error_type: str | None = None
    orchestration_enabled: bool = True

    def __post_init__(self) -> None:
        if self.effective_priority is None:
            self.effective_priority = float(self.priority)


@dataclass
class OrchestratorEvent:
    timestamp: datetime
    level: str
    task_id: int | None
    message: str
    event_code: str = "GENERAL"
    user_id: str | None = None
    run_id: int | None = None


@dataclass
class ScenarioSpec:
    scenario_id: str
    title: str
    description: str
    tasks: list[TaskRequest]


ProgressCallback = Callable[[int, int], Awaitable[None]]
StateChangeCallback = Callable[[], Awaitable[None] | None]


class DeadlockError(RuntimeError):
    """Cycle in legacy resource acquisition."""


class DataCorruptionError(RuntimeError):
    """Lost update or dirty read."""


class CrossDateCollisionError(RuntimeError):
    """Shared staging overwritten by another date."""


def to_utc(value: datetime | None) -> str | None:
    """Interpret naive UI time in the host's local zone, then serialize UTC."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def from_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone().replace(tzinfo=None)
