#!/usr/bin/env python3
"""
Генератор 5 канонических демо-сценариев и параметрических стресс-батчей (JSON).
Запуск:
    python scripts/generate_scenarios.py --out synthetic_data/scenarios.json --random-count 15 --seed 42
"""
from __future__ import annotations

import argparse
import json
import random
from dataclasses import asdict, dataclass
from pathlib import Path

PROCESS_IDS: tuple[str, ...] = (
    "StockoutRisk",
    "DelayRisk",
    "SpoilageRisk",
    "CarrierDefaultRisk",
)

DEMO_DATES: tuple[str, ...] = (
    "2026-11-18",
    "2026-11-19",
    "2026-11-20",
    "2026-11-21",
    "2026-11-22",
)


@dataclass
class ScenarioTask:
    process_id: str
    target_date: str
    trigger: str  # "AUTO" | "MANUAL"
    priority: int
    delay_sec: float = 0.0
    initiated_by: str = "analyst_1"


@dataclass
class ScenarioBundle:
    scenario_id: str
    title: str
    description: str
    tasks: list[ScenarioTask]


def build_demo_scenarios() -> list[ScenarioBundle]:
    return [
        ScenarioBundle(
            scenario_id="s1_manual_vs_reglament",
            title="Сценарий 1: Ручной vs Регламент (Lost Update + Дубль)",
            description="Ночной регламент StockoutRisk и одновременный ручной запуск SpoilageRisk + дубль StockoutRisk за 2026-11-20.",
            tasks=[
                ScenarioTask("StockoutRisk", "2026-11-20", "AUTO", 1, 0.0, "scheduler_bot"),
                ScenarioTask("SpoilageRisk", "2026-11-20", "MANUAL", 2, 0.05, "analyst_1"),
                ScenarioTask("StockoutRisk", "2026-11-20", "MANUAL", 2, 0.08, "analyst_2"),
            ],
        ),
        ScenarioBundle(
            scenario_id="s2_abba_deadlock",
            title="Сценарий 2: Взаимная блокировка AB-BA (Deadlock)",
            description="Одновременный запуск StockoutRisk и DelayRisk за 2026-11-20 с противоположным порядком захвата WarehouseStocks <-> TransitTelemetry.",
            tasks=[
                ScenarioTask("StockoutRisk", "2026-11-20", "AUTO", 1, 0.0, "scheduler_bot"),
                ScenarioTask("DelayRisk", "2026-11-20", "MANUAL", 2, 0.02, "analyst_2"),
            ],
        ),
        ScenarioBundle(
            scenario_id="s3_cross_date_collision",
            title="Сценарий 3: Перекрёстные даты и глобальный SLA",
            description="Два пользователя запускают CarrierDefaultRisk за разные даты (общий staging) + DelayRisk читает глобальную витрину SupplierSLA.",
            tasks=[
                ScenarioTask("CarrierDefaultRisk", "2026-11-19", "MANUAL", 2, 0.0, "analyst_1"),
                ScenarioTask("CarrierDefaultRisk", "2026-11-20", "MANUAL", 2, 0.04, "manager_1"),
                ScenarioTask("DelayRisk", "2026-11-21", "MANUAL", 2, 0.06, "analyst_2"),
            ],
        ),
        ScenarioBundle(
            scenario_id="s4_parallel_safe",
            title="Сценарий 4: Безопасный параллелизм (разные даты и READ+READ)",
            description="Независимые процессы и партиционированные даты (SpoilageRisk 11-18, 11-19 + DelayRisk 11-22) стартуют строго одновременно.",
            tasks=[
                ScenarioTask("SpoilageRisk", "2026-11-18", "MANUAL", 2, 0.0, "analyst_1"),
                ScenarioTask("SpoilageRisk", "2026-11-19", "MANUAL", 2, 0.0, "analyst_2"),
                ScenarioTask("DelayRisk", "2026-11-22", "MANUAL", 2, 0.0, "manager_1"),
            ],
        ),
        ScenarioBundle(
            scenario_id="s5_multiuser_storm",
            title="Сценарий 5: Стресс-тест «Шторм пользователей» (10 задач)",
            description="Регламент и 3 аналитика одновременно вбрасывают 10 задач: AB-BA, Lost Update, глобальный SLA, дубль и независимые даты.",
            tasks=[
                ScenarioTask("StockoutRisk", "2026-11-20", "AUTO", 1, 0.0, "scheduler_bot"),
                ScenarioTask("DelayRisk", "2026-11-20", "MANUAL", 2, 0.02, "analyst_1"),
                ScenarioTask("SpoilageRisk", "2026-11-20", "MANUAL", 2, 0.03, "analyst_2"),
                ScenarioTask("CarrierDefaultRisk", "2026-11-20", "AUTO", 1, 0.05, "scheduler_bot"),
                ScenarioTask("CarrierDefaultRisk", "2026-11-19", "MANUAL", 3, 0.06, "manager_1"),
                ScenarioTask("SpoilageRisk", "2026-11-18", "MANUAL", 2, 0.07, "analyst_1"),
                ScenarioTask("DelayRisk", "2026-11-21", "MANUAL", 3, 0.08, "analyst_2"),
                ScenarioTask("StockoutRisk", "2026-11-20", "MANUAL", 2, 0.09, "analyst_1"),
                ScenarioTask("SpoilageRisk", "2026-11-22", "MANUAL", 3, 0.10, "manager_1"),
                ScenarioTask("StockoutRisk", "2026-11-21", "MANUAL", 2, 0.12, "analyst_2"),
            ],
        ),
    ]


def generate_random_batch(
    count: int = 15,
    seed: int = 42,
    conflict_bias: float = 0.65,
) -> list[ScenarioTask]:
    rng = random.Random(seed)
    users = ("analyst_1", "analyst_2", "manager_1")
    hot_date = DEMO_DATES[len(DEMO_DATES) // 2]  # 2026-11-20 — пиковая дата коллизий
    batch: list[ScenarioTask] = []
    for _ in range(count):
        proc = rng.choice(PROCESS_IDS)
        target_date = hot_date if rng.random() < conflict_bias else rng.choice(DEMO_DATES)
        is_auto = rng.random() < 0.25
        trigger = "AUTO" if is_auto else "MANUAL"
        user = "scheduler_bot" if is_auto else rng.choice(users)
        prio = 1 if is_auto else rng.choice((2, 2, 3))
        delay = round(rng.uniform(0.0, 0.25), 3)
        batch.append(ScenarioTask(proc, target_date, trigger, prio, delay, user))
    return batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Генерация демо-сценариев и стресс-батча.")
    parser.add_argument("--out", type=Path, default=Path("synthetic_data/scenarios.json"))
    parser.add_argument("--random-count", type=int, default=15)
    parser.add_argument("--conflict-bias", type=float, default=0.65)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    payload = {
        "scenarios": [asdict(s) for s in build_demo_scenarios()],
        "random_stress_batch": [asdict(t) for t in generate_random_batch(args.random_count, args.seed, args.conflict_bias)],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Сценарии сохранены в {args.out} ({args.out.stat().st_size} байт)")


if __name__ == "__main__":
    main()
