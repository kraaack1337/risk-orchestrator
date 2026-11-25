#!/usr/bin/env python3
"""
Генератор связанных доменных витрин логистики (CSV + опционально SQLite).
Стандартная библиотека Python 3.11+, детерминированный seed.
Примеры запуска:
    python scripts/generate_domain_data.py
    python scripts/generate_domain_data.py --out-dir synthetic_data --to-sqlite data/orchestrator.db --seed 42
"""
from __future__ import annotations

import argparse
import csv
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

WAREHOUSES: list[tuple[str, str, str]] = [
    ("WH-MSK-01", "Москва-Север", "COR-MSK-SPB"),
    ("WH-SPB-02", "Санкт-Петербург", "COR-MSK-SPB"),
    ("WH-KZN-03", "Казань-Хаб", "COR-MSK-KZN"),
    ("WH-EKB-04", "Екатеринбург", "COR-KZN-EKB"),
    ("WH-KRR-05", "Краснодар-Юг", "COR-MSK-KRR"),
]

CORRIDORS: list[tuple[str, str]] = [
    ("COR-MSK-SPB", "М-11 Москва — СПб"),
    ("COR-MSK-KZN", "М-12 Москва — Казань"),
    ("COR-KZN-EKB", "Р-242 Казань — Екатеринбург"),
    ("COR-MSK-KRR", "М-4 Дон Москва — Краснодар"),
]

# (sku_id, category, is_perishable, norm_temp_c, reorder_point, avg_daily_demand)
SKUS: list[tuple[str, str, bool, float, int, float]] = [
    ("SKU-101", "DAIRY_FRESH", True, 4.0, 220, 45.0),
    ("SKU-102", "MEAT_CHILLED", True, 2.0, 180, 35.0),
    ("SKU-103", "PRODUCE_FRESH", True, 6.0, 300, 60.0),
    ("SKU-201", "PHARMA_COLD", True, 5.0, 90, 18.0),
    ("SKU-301", "BEVERAGES", False, 18.0, 500, 95.0),
    ("SKU-302", "GROCERY_DRY", False, 20.0, 600, 110.0),
    ("SKU-401", "ELECTRONICS", False, 20.0, 80, 12.0),
    ("SKU-402", "HOUSEHOLD", False, 20.0, 350, 65.0),
]

# (carrier_id, carrier_name, base_on_time_rate, base_claim_rate)
CARRIERS: list[tuple[str, str, float, float]] = [
    ("CAR-01", "СеверТранс Логистик", 0.96, 0.012),
    ("CAR-02", "ВолгаЭкспресс", 0.91, 0.025),
    ("CAR-03", "УралФрахт", 0.85, 0.045),
    ("CAR-04", "ЮгКарго Плюс", 0.72, 0.088),
    ("CAR-05", "Магистраль-24", 0.94, 0.018),
    ("CAR-06", "ЭкспрессРейл", 0.64, 0.115),
]

DDL_DOMAIN = """
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
"""

def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_csvs_to_sqlite(csv_dir: Path, db_path: Path) -> dict[str, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL_DOMAIN)
    conn.execute("PRAGMA foreign_keys = ON;")
    mapping = [
        ("weather_traffic.csv", "dom_weather_traffic"),
        ("warehouse_stocks.csv", "dom_warehouse_stocks"),
        ("transit_telemetry.csv", "dom_transit_telemetry"),
        ("supplier_sla.csv", "dom_supplier_sla"),
    ]
    loaded: dict[str, int] = {}
    conn.execute("BEGIN;")
    try:
        for fname, table in mapping:
            with (csv_dir / fname).open("r", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            if not rows:
                loaded[table] = 0
                continue
            cols = list(rows[0].keys())
            placeholders = ", ".join("?" for _ in cols)
            sql = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
            conn.executemany(sql, [[r[c] for c in cols] for r in rows])
            loaded[table] = len(rows)
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()
    return loaded


def generate_domain_datasets(
    out_dir: Path,
    start_date: date = date(2026, 11, 18),
    num_days: int = 5,
    shipments_per_day: int = 18,
    seed: int = 42,
    sqlite_path: Path | None = None,
) -> dict[str, int]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    weather_rows: list[dict[str, object]] = []
    stock_rows: list[dict[str, object]] = []
    transit_rows: list[dict[str, object]] = []
    sla_rows: list[dict[str, object]] = []
    weather_index: dict[tuple[str, str], dict[str, object]] = {}
    shipment_seq = 1000

    # 1. Витрина WeatherAndTraffic (партиционирована по датам и коридорам)
    for d in dates:
        d_str = d.isoformat()
        for cor_id, region in CORRIDORS:
            if d == date(2026, 11, 20) and cor_id in ("COR-MSK-SPB", "COR-MSK-KZN"):
                severity = round(rng.uniform(0.82, 0.96), 2)
                congestion = round(rng.uniform(0.78, 0.95), 2)
                temp_amb = round(rng.uniform(-14.0, -7.0), 1)
                alert = "BLIZZARD_SEVERE"
            elif d == date(2026, 11, 21) and cor_id == "COR-MSK-KRR":
                severity = round(rng.uniform(0.55, 0.75), 2)
                congestion = round(rng.uniform(0.72, 0.88), 2)
                temp_amb = round(rng.uniform(22.0, 27.0), 1)
                alert = "ROADWORK_CONGESTION"
            else:
                severity = round(rng.uniform(0.05, 0.42), 2)
                congestion = round(rng.uniform(0.15, 0.55), 2)
                temp_amb = round(rng.uniform(-4.0, 9.0), 1)
                alert = "NONE"
            row: dict[str, object] = {
                "business_date": d_str,
                "corridor_id": cor_id,
                "region_name": region,
                "weather_severity": severity,
                "temp_ambient_celsius": temp_amb,
                "traffic_congestion": congestion,
                "road_alert_code": alert,
            }
            weather_rows.append(row)
            weather_index[(d_str, cor_id)] = row

    # 2. Витрина WarehouseStocks (партиционирована по датам, складам и SKU)
    for d in dates:
        d_str = d.isoformat()
        for wh_id, _wh_name, _cor_id in WAREHOUSES:
            for sku_id, cat, is_perishable, norm_temp, reorder_pt, avg_demand in SKUS:
                if (
                    d == date(2026, 11, 20)
                    and wh_id in ("WH-MSK-01", "WH-KZN-03")
                    and sku_id in ("SKU-101", "SKU-201", "SKU-401")
                ):
                    qty_on_hand = rng.randint(5, max(10, int(reorder_pt * 0.35)))
                    qty_reserved = min(qty_on_hand, rng.randint(5, max(6, int(qty_on_hand * 0.8))))
                else:
                    qty_on_hand = rng.randint(int(reorder_pt * 0.85), int(reorder_pt * 3.2))
                    qty_reserved = rng.randint(0, int(qty_on_hand * 0.3))
                if is_perishable:
                    if (
                        d in (date(2026, 11, 20), date(2026, 11, 21))
                        and wh_id == "WH-KRR-05"
                        and sku_id in ("SKU-101", "SKU-102", "SKU-201")
                    ):
                        expiry = (d + timedelta(days=rng.randint(1, 2))).isoformat()
                        temp_c = round(norm_temp + rng.uniform(5.5, 9.0), 1)
                    else:
                        expiry = (d + timedelta(days=rng.randint(4, 21))).isoformat()
                        temp_c = round(norm_temp + rng.uniform(-1.0, 1.2), 1)
                else:
                    expiry = (d + timedelta(days=rng.randint(180, 540))).isoformat()
                    temp_c = round(norm_temp + rng.uniform(-2.0, 2.0), 1)
                stock_rows.append({
                    "business_date": d_str,
                    "warehouse_id": wh_id,
                    "sku_id": sku_id,
                    "sku_category": cat,
                    "qty_on_hand": qty_on_hand,
                    "qty_reserved": qty_reserved,
                    "reorder_point": reorder_pt,
                    "avg_daily_demand": round(avg_demand * rng.uniform(0.85, 1.2), 1),
                    "expiry_date": expiry,
                    "temp_celsius": temp_c,
                    "stockout_risk_flag": 0,
                    "spoilage_risk_flag": 0,
                    "row_version": 1,
                })

    # 3. Витрина TransitTelemetry (связывает перевозчика, склад, SKU и коридор)
    carrier_stats: dict[str, list[bool]] = {c[0]: [] for c in CARRIERS}
    for d in dates:
        d_str = d.isoformat()
        for _ in range(shipments_per_day):
            shipment_seq += 1
            wh_id, _wh_name, cor_id = rng.choice(WAREHOUSES)
            sku_id, _cat, is_perishable, norm_temp, _rp, _dem = rng.choice(SKUS)
            car_id, _car_name, base_ot, _base_claim = rng.choice(CARRIERS)
            w = weather_index[(d_str, cor_id)]
            w_sev = float(w["weather_severity"])
            t_cong = float(w["traffic_congestion"])
            planned_eta = round(rng.uniform(8.0, 36.0), 1)
            delay_prob = (1.0 - base_ot) + 0.45 * w_sev + 0.25 * t_cong
            is_delayed = rng.random() < min(0.95, delay_prob)
            if is_delayed:
                extra_hours = round(rng.uniform(2.5, 18.0) * (1.0 + w_sev), 1)
                actual_eta = round(planned_eta + extra_hours, 1)
                gps_status = (
                    "STOPPED"
                    if w_sev > 0.8
                    else ("REROUTED" if rng.random() < 0.3 else "DELAYED")
                )
            else:
                actual_eta = round(planned_eta + rng.uniform(-1.0, 0.8), 1)
                gps_status = "ON_TIME"
            carrier_stats[car_id].append(not is_delayed)
            temp_shift = rng.uniform(3.0, 7.0) if (is_perishable and is_delayed) else rng.uniform(-0.8, 0.8)
            transit_rows.append({
                "shipment_id": f"SHP-{d.strftime('%m%d')}-{shipment_seq}",
                "business_date": d_str,
                "carrier_id": car_id,
                "warehouse_id": wh_id,
                "sku_id": sku_id,
                "corridor_id": cor_id,
                "planned_eta_hours": planned_eta,
                "actual_eta_hours": actual_eta,
                "cargo_qty": rng.randint(40, 450),
                "cargo_temp_celsius": round(norm_temp + temp_shift, 1),
                "gps_status": gps_status,
                "delay_risk_score": 0.0,
                "row_version": 1,
            })

    # 4. Витрина SupplierSLA (глобальная кумулятивная витрина за 30 дней)
    latest_date_str = dates[-1].isoformat()
    for car_id, car_name, base_ot, base_claim in CARRIERS:
        outcomes = carrier_stats[car_id]
        obs_ot = sum(outcomes) / len(outcomes) if outcomes else base_ot
        rolling_ot = round(0.65 * base_ot + 0.35 * obs_ot, 3)
        claim_rate = round(base_claim * rng.uniform(0.9, 1.15), 3)
        if rolling_ot >= 0.92:
            tier = "A"
        elif rolling_ot >= 0.82:
            tier = "B"
        elif rolling_ot >= 0.70:
            tier = "C"
        else:
            tier = "CRITICAL"
        sla_rows.append({
            "carrier_id": car_id,
            "carrier_name": car_name,
            "as_of_date": latest_date_str,
            "rolling_window_days": 30,
            "on_time_rate": rolling_ot,
            "claim_rate": claim_rate,
            "active_shipments": len(outcomes),
            "default_risk_score": 0.0,
            "sla_tier": tier,
            "row_version": 1,
        })

    write_csv(out_dir / "weather_traffic.csv", weather_rows)
    write_csv(out_dir / "warehouse_stocks.csv", stock_rows)
    write_csv(out_dir / "transit_telemetry.csv", transit_rows)
    write_csv(out_dir / "supplier_sla.csv", sla_rows)
    summary = {
        "weather_traffic": len(weather_rows),
        "warehouse_stocks": len(stock_rows),
        "transit_telemetry": len(transit_rows),
        "supplier_sla": len(sla_rows),
        "anomalies_stockout": sum(
            1
            for r in stock_rows
            if (int(r["qty_on_hand"]) - int(r["qty_reserved"])) < int(r["reorder_point"]) * 0.5
        ),
        "anomalies_spoilage": sum(
            1
            for r in stock_rows
            if str(r["sku_category"]) in ("DAIRY_FRESH", "MEAT_CHILLED", "PRODUCE_FRESH", "PHARMA_COLD")
            and float(r["temp_celsius"]) > 8.5
        ),
        "anomalies_delay": sum(1 for r in transit_rows if str(r["gps_status"]) in ("DELAYED", "STOPPED")),
        "anomalies_carrier_sla": sum(1 for r in sla_rows if str(r["sla_tier"]) in ("C", "CRITICAL")),
    }
    if sqlite_path is not None:
        load_csvs_to_sqlite(out_dir, sqlite_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Генерация синтетических логистических витрин.")
    parser.add_argument("--out-dir", type=Path, default=Path("synthetic_data"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 11, 18))
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--to-sqlite", type=Path, default=None)
    args = parser.parse_args()
    stats = generate_domain_datasets(
        out_dir=args.out_dir,
        start_date=args.start_date,
        num_days=args.days,
        seed=args.seed,
        sqlite_path=args.to_sqlite,
    )
    print("Доменные витрины сгенерированы:", stats)


if __name__ == "__main__":
    main()

