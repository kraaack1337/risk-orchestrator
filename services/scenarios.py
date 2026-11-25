"""services/scenarios.py — загрузка 5 канонических демо-сценариев и генератор стресс-батчей."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from core.models import ScenarioSpec, TaskRequest, TriggerType
from services.microservices import CARRIER, DELAY, SPOILAGE, STOCKOUT

_PROCESSES: tuple[str, ...] = (STOCKOUT, DELAY, SPOILAGE, CARRIER)
DEMO_DATES: tuple[str, ...] = (
    "2026-11-18",
    "2026-11-19",
    "2026-11-20",
    "2026-11-21",
    "2026-11-22",
)
_SCENARIOS_PATH = Path(__file__).resolve().parent.parent / "synthetic_data" / "scenarios.json"


def _task_from_dict(raw: dict[str, Any]) -> TaskRequest:
    trigger = TriggerType(raw["trigger"])
    priority = 1 if trigger == TriggerType.AUTO else int(raw.get("priority") or 2)
    if trigger == TriggerType.MANUAL and priority not in (2, 3):
        priority = 2
    return TaskRequest(
        process_id=str(raw["process_id"]),
        target_date=str(raw["target_date"]),
        trigger=trigger,
        priority=priority,
        delay_sec=float(raw.get("delay_sec", 0.0)),
        initiated_by=str(raw.get("initiated_by") or ("scheduler_bot" if trigger == TriggerType.AUTO else "analyst_1")),
    )


def _fallback_scenarios() -> list[ScenarioSpec]:
    return [
        ScenarioSpec(
            scenario_id="s1_manual_vs_reglament",
            title="Сценарий 1: Ручной vs Регламент (Lost Update + Дубль)",
            description="Ночной регламент StockoutRisk и одновременный ручной запуск SpoilageRisk + дубль StockoutRisk за 2026-11-20.",
            tasks=[
                TaskRequest(STOCKOUT, "2026-11-20", TriggerType.AUTO, 1, 0.0, "scheduler_bot"),
                TaskRequest(SPOILAGE, "2026-11-20", TriggerType.MANUAL, 2, 0.05, "analyst_1"),
                TaskRequest(STOCKOUT, "2026-11-20", TriggerType.MANUAL, 2, 0.08, "analyst_2"),
            ],
        ),
        ScenarioSpec(
            scenario_id="s2_abba_deadlock",
            title="Сценарий 2: Взаимная блокировка AB-BA (Deadlock)",
            description="Одновременный запуск StockoutRisk и DelayRisk за 2026-11-20 с противоположным порядком захвата WarehouseStocks <-> TransitTelemetry.",
            tasks=[
                TaskRequest(STOCKOUT, "2026-11-20", TriggerType.AUTO, 1, 0.0, "scheduler_bot"),
                TaskRequest(DELAY, "2026-11-20", TriggerType.MANUAL, 2, 0.02, "analyst_2"),
            ],
        ),
        ScenarioSpec(
            scenario_id="s3_cross_date_collision",
            title="Сценарий 3: Перекрёстные даты и глобальный SLA",
            description="Два пользователя запускают CarrierDefaultRisk за разные даты (общий staging) + DelayRisk читает глобальную витрину SupplierSLA.",
            tasks=[
                TaskRequest(CARRIER, "2026-11-19", TriggerType.MANUAL, 2, 0.0, "analyst_1"),
                TaskRequest(CARRIER, "2026-11-20", TriggerType.MANUAL, 2, 0.04, "manager_1"),
                TaskRequest(DELAY, "2026-11-21", TriggerType.MANUAL, 2, 0.06, "analyst_2"),
            ],
        ),
        ScenarioSpec(
            scenario_id="s4_parallel_safe",
            title="Сценарий 4: Безопасный параллелизм (разные даты и READ+READ)",
            description="Независимые процессы и партиционированные даты (SpoilageRisk 11-18, 11-19 + DelayRisk 11-22) стартуют строго одновременно.",
            tasks=[
                TaskRequest(SPOILAGE, "2026-11-18", TriggerType.MANUAL, 2, 0.0, "analyst_1"),
                TaskRequest(SPOILAGE, "2026-11-19", TriggerType.MANUAL, 2, 0.0, "analyst_2"),
                TaskRequest(DELAY, "2026-11-22", TriggerType.MANUAL, 2, 0.0, "manager_1"),
            ],
        ),
        ScenarioSpec(
            scenario_id="s5_multiuser_storm",
            title="Сценарий 5: Стресс-тест «Шторм пользователей» (10 задач)",
            description="Регламент и 3 аналитика одновременно вбрасывают 10 задач: AB-BA, Lost Update, глобальный SLA, дубль и независимые даты.",
            tasks=[
                TaskRequest(STOCKOUT, "2026-11-20", TriggerType.AUTO, 1, 0.0, "scheduler_bot"),
                TaskRequest(DELAY, "2026-11-20", TriggerType.MANUAL, 2, 0.02, "analyst_1"),
                TaskRequest(SPOILAGE, "2026-11-20", TriggerType.MANUAL, 2, 0.03, "analyst_2"),
                TaskRequest(CARRIER, "2026-11-20", TriggerType.AUTO, 1, 0.05, "scheduler_bot"),
                TaskRequest(CARRIER, "2026-11-19", TriggerType.MANUAL, 3, 0.06, "manager_1"),
                TaskRequest(SPOILAGE, "2026-11-18", TriggerType.MANUAL, 2, 0.07, "analyst_1"),
                TaskRequest(DELAY, "2026-11-21", TriggerType.MANUAL, 3, 0.08, "analyst_2"),
                TaskRequest(STOCKOUT, "2026-11-20", TriggerType.MANUAL, 2, 0.09, "analyst_1"),
                TaskRequest(SPOILAGE, "2026-11-22", TriggerType.MANUAL, 3, 0.10, "manager_1"),
                TaskRequest(STOCKOUT, "2026-11-21", TriggerType.MANUAL, 2, 0.12, "analyst_2"),
            ],
        ),
    ]


def get_demo_scenarios(scenarios_path: str | Path | None = None) -> list[ScenarioSpec]:
    """Загружает 5 канонических сценариев из synthetic_data/scenarios.json."""
    path = Path(scenarios_path) if scenarios_path else _SCENARIOS_PATH
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            raw_list = payload.get("scenarios", [])
            if isinstance(raw_list, list) and raw_list:
                return [
                    ScenarioSpec(
                        scenario_id=str(item["scenario_id"]),
                        title=str(item["title"]),
                        description=str(item["description"]),
                        tasks=[_task_from_dict(t) for t in item.get("tasks", [])],
                    )
                    for item in raw_list
                ]
        except (OSError, ValueError, KeyError):
            pass
    return _fallback_scenarios()


def generate_random_batch(
    count: int = 15,
    seed: int | None = None,
    conflict_bias: float = 0.65,
) -> list[TaskRequest]:
    """Генерирует случайную пачку задач с пересечением по датам и источникам."""
    if seed == 42 and count == 15 and _SCENARIOS_PATH.exists():
        try:
            payload = json.loads(_SCENARIOS_PATH.read_text(encoding="utf-8"))
            raw_batch = payload.get("random_stress_batch", [])
            if isinstance(raw_batch, list) and len(raw_batch) == 15:
                return [_task_from_dict(t) for t in raw_batch]
        except (OSError, ValueError, KeyError):
            pass

    rng = random.Random(seed)
    users = ("analyst_1", "analyst_2", "manager_1")
    hot_date = DEMO_DATES[len(DEMO_DATES) // 2]  # 2026-11-20
    batch: list[TaskRequest] = []
    for _ in range(max(0, count)):
        proc = rng.choice(_PROCESSES)
        target_date = hot_date if rng.random() < conflict_bias else rng.choice(DEMO_DATES)
        is_auto = rng.random() < 0.25
        trigger = TriggerType.AUTO if is_auto else TriggerType.MANUAL
        user = "scheduler_bot" if is_auto else rng.choice(users)
        prio = 1 if is_auto else rng.choice((2, 2, 3))
        delay = round(rng.uniform(0.0, 0.25), 3)
        batch.append(
            TaskRequest(
                process_id=proc,
                target_date=target_date,
                trigger=trigger,
                priority=prio,
                delay_sec=delay,
                initiated_by=user,
            )
        )
    return batch
