#!/usr/bin/env python3
"""Точка входа нативного десктоп-приложения «RiskFlow Orchestrator» (CustomTkinter + matplotlib + sqlite3 WAL).

При первом старте автоматически создаёт синтетические CSV-витрины, логи и сценарии (seed=42),
инициализирует БД data/orchestrator.db в режиме WAL и открывает главное окно диспетчера.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.orchestrator import Orchestrator
from core.scheduler import ReglamentScheduler
from core.storage import Storage
from scripts.generate_domain_data import generate_domain_datasets, load_csvs_to_sqlite
from scripts.generate_legacy_logs import generate_legacy_logs
from scripts.generate_scenarios import (
    build_demo_scenarios,
)
from scripts.generate_scenarios import (
    generate_random_batch as generate_scenario_tasks,
)
from services.microservices import SharedDataLayer, create_services
from services.scenarios import get_demo_scenarios

DEFAULT_SYNTHETIC_DIR = ROOT_DIR / "synthetic_data"
DEFAULT_DB_PATH = ROOT_DIR / "data" / "orchestrator.db"

REQUIRED_CSVS: tuple[str, ...] = (
    "warehouse_stocks.csv",
    "transit_telemetry.csv",
    "weather_traffic.csv",
    "supplier_sla.csv",
)
REQUIRED_LOGS: tuple[str, ...] = (
    "legacy_system_logs.jsonl",
    "legacy_system_logs.log",
)


def ensure_synthetic_data(synthetic_dir: Path = DEFAULT_SYNTHETIC_DIR, seed: int = 42) -> None:
    """Проверяет и при необходимости генерирует синтетические витрины, логи и сценарии (seed=42)."""
    synthetic_dir.mkdir(parents=True, exist_ok=True)

    if any(not (synthetic_dir / name).exists() for name in REQUIRED_CSVS):
        generate_domain_datasets(out_dir=synthetic_dir, seed=seed)

    if any(not (synthetic_dir / name).exists() for name in REQUIRED_LOGS):
        generate_legacy_logs(out_dir=synthetic_dir, seed=seed)

    scenarios_file = synthetic_dir / "scenarios.json"
    if not scenarios_file.exists():
        payload = {
            "scenarios": [asdict(s) for s in build_demo_scenarios()],
            "random_stress_batch": [
                asdict(t) for t in generate_scenario_tasks(count=15, seed=seed, conflict_bias=0.65)
            ],
        }
        scenarios_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def bootstrap_environment(
    synthetic_dir: Path = DEFAULT_SYNTHETIC_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    seed: int = 42,
) -> Storage:
    """Гарантирует наличие всех файлов synthetic_data/ и инициализированной БД data/orchestrator.db."""
    ensure_synthetic_data(synthetic_dir=synthetic_dir, seed=seed)
    db_existed = db_path.exists()
    storage = Storage(db_path)

    row = storage._query_one("SELECT COUNT(*) AS cnt FROM dom_warehouse_stocks")
    if not db_existed or not row or int(row["cnt"]) == 0:
        load_csvs_to_sqlite(synthetic_dir, db_path)
    return storage


def build_runtime(
    synthetic_dir: Path = DEFAULT_SYNTHETIC_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    seed: int = 42,
    time_scale: float = 1.0,
) -> tuple[Storage, SharedDataLayer, Orchestrator, ReglamentScheduler]:
    """Собирает Storage, SharedDataLayer, Orchestrator и ReglamentScheduler."""
    storage = bootstrap_environment(synthetic_dir=synthetic_dir, db_path=db_path, seed=seed)
    data_layer = SharedDataLayer(data_dir=synthetic_dir, time_scale=time_scale)
    services = create_services(data_layer)
    orchestrator = Orchestrator(services=services, storage=storage)
    scheduler = ReglamentScheduler(orchestrator=orchestrator, interval=15.0)
    return storage, data_layer, orchestrator, scheduler


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RiskFlow Orchestrator — Нативное десктоп-приложение (CustomTkinter + matplotlib + sqlite3 WAL)"
    )
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Только проверить/сгенерировать синтетику и инициализировать БД без открытия окна",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    storage, _data_layer, orchestrator, scheduler = build_runtime()
    if args.bootstrap_only:
        storage.close()
        return

    from ui.app import run_ui

    scenarios = get_demo_scenarios(DEFAULT_SYNTHETIC_DIR / "scenarios.json")
    try:
        run_ui(
            orchestrator=orchestrator,
            scheduler=scheduler,
            scenarios=scenarios,
        )
    finally:
        storage.close()


if __name__ == "__main__":
    main()
