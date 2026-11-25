"""Contract bridge between Desktop UI and Core/Services modules."""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from core.models import (
    CARRIER_DEFAULT_RISK,
    DELAY_RISK,
    PROCESS_IDS,
    SOURCE_IDS,
    SPOILAGE_RISK,
    STOCKOUT_RISK,
    SUPPLIER_SLA,
    TRANSIT_TELEMETRY,
    WAREHOUSE_STOCKS,
    WEATHER_AND_TRAFFIC,
    CrossDateCollisionError,
    DataCorruptionError,
    DeadlockError,
    OrchestratorEvent,
    ProcessSpec,
    ProgressCallback,
    ScenarioSpec,
    StateChangeCallback,
    TaskRequest,
    TaskRun,
    TaskStatus,
    TriggerType,
)

SOURCE_DISPLAY_NAMES: dict[str, str] = {
    WAREHOUSE_STOCKS: "Остатки складов",
    TRANSIT_TELEMETRY: "Телеметрия рейсов",
    WEATHER_AND_TRAFFIC: "Погода и трафик",
    SUPPLIER_SLA: "Глобальный SLA",
}


@runtime_checkable
class OrchestratorProtocol(Protocol):
    """Public UI <-> Core Orchestrator contract (Section 5.4)."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def set_orchestration_enabled(self, enabled: bool) -> None: ...
    def is_orchestration_enabled(self) -> bool: ...
    async def submit(self, request: TaskRequest) -> TaskRun: ...
    async def submit_batch(self, requests: list[TaskRequest]) -> list[TaskRun]: ...
    async def cancel(self, task_id: int) -> bool: ...
    def pause(self) -> None: ...
    def resume(self) -> None: ...
    async def reset(self) -> None: ...
    def subscribe(self, callback: StateChangeCallback) -> None: ...
    def unsubscribe(self, callback: StateChangeCallback) -> None: ...

    def get_process_specs(self) -> list[ProcessSpec]: ...
    def get_tasks(self, status: TaskStatus | None = None) -> list[TaskRun]: ...
    def get_active_locks(self) -> dict[str, list[int]]: ...
    def get_events(self, limit: int = 100) -> list[OrchestratorEvent]: ...
    def get_gantt_data(self) -> list[dict[str, Any]]: ...
    def get_metrics(self) -> dict[str, Any]: ...


@runtime_checkable
class SchedulerProtocol(Protocol):
    """Public UI <-> ReglamentScheduler contract (Section 5.4)."""

    def set_enabled(self, enabled: bool) -> None: ...
    def set_interval(self, seconds: float) -> None: ...
    def is_enabled(self) -> bool: ...


RandomBatchGenerator = Callable[[int, int | None], list[TaskRequest]]


def status_key(status: TaskStatus | str) -> str:
    if isinstance(status, Enum):
        return status.name.upper()
    return str(status).upper()


def trigger_key(trigger: TriggerType | str) -> str:
    if isinstance(trigger, Enum):
        return trigger.name.upper()
    return str(trigger).upper()


def extract_error_type(task: TaskRun) -> str:
    """Extract concise exception class name for error badges."""
    err_type = getattr(task, "error_type", None)
    if err_type:
        return str(err_type)
    msg = task.error_message or ""
    if ":" in msg:
        head = msg.split(":", 1)[0].strip()
        if head.endswith("Error"):
            return head
    low = msg.lower()
    if "deadlock" in low or "ab-ba" in low:
        return "DeadlockError"
    if "staging" in low or "cross" in low:
        return "CrossDateCollisionError"
    if "lost update" in low or "грязн" in low or "corrupt" in low:
        return "DataCorruptionError"
    return "RuntimeError"


__all__ = [
    "CARRIER_DEFAULT_RISK",
    "CrossDateCollisionError",
    "DELAY_RISK",
    "DataCorruptionError",
    "DeadlockError",
    "OrchestratorEvent",
    "OrchestratorProtocol",
    "PROCESS_IDS",
    "ProcessSpec",
    "ProgressCallback",
    "RandomBatchGenerator",
    "SOURCE_DISPLAY_NAMES",
    "SOURCE_IDS",
    "SPOILAGE_RISK",
    "STOCKOUT_RISK",
    "SUPPLIER_SLA",
    "ScenarioSpec",
    "SchedulerProtocol",
    "StateChangeCallback",
    "TRANSIT_TELEMETRY",
    "TaskRequest",
    "TaskRun",
    "TaskStatus",
    "TriggerType",
    "WAREHOUSE_STOCKS",
    "WEATHER_AND_TRAFFIC",
    "extract_error_type",
    "status_key",
    "trigger_key",
]
