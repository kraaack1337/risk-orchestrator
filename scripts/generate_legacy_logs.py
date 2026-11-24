#!/usr/bin/env python3
"""
Генератор синтетических сбойных логов «старой системы» (86 событий)
для воспроизводимого ИИ-анализа узких мест (TXT + JSONL).
Запуск:
    python scripts/generate_legacy_logs.py --out-dir synthetic_data --seed 42
"""
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_legacy_logs(
    out_dir: Path,
    seed: int = 42,
    start_ts: datetime = datetime(2026, 11, 20, 6, 0, 0, tzinfo=timezone.utc),
) -> dict[str, object]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, object]] = []
    ts = start_ts
    job_id = 100

    def emit(
        level: str,
        service: str,
        trigger: str,
        user_id: str,
        target_date: str,
        action: str,
        message: str,
        resources: list[str],
        error_code: str | None = None,
        incident_type: str | None = None,
        jid: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        nonlocal ts
        ts += timedelta(milliseconds=rng.randint(350, 2400))
        events.append({
            "timestamp": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": level,
            "job_id": jid,
            "service": service,
            "trigger": trigger,
            "user_id": user_id,
            "target_date": target_date,
            "action": action,
            "resources": resources,
            "error_code": error_code,
            "incident_type": incident_type,
            "duration_ms": duration_ms,
            "message": message,
        })

    def add_normal_run(service: str, trigger: str, user_id: str, target_date: str, res: list[str]) -> None:
        nonlocal job_id
        job_id += 1
        jid = job_id
        emit("INFO", service, trigger, user_id, target_date, "START",
             f"Job #{jid} started ({trigger} by {user_id}) for {target_date}", res, jid=jid)
        emit("INFO", service, trigger, user_id, target_date, "COMPLETE",
             f"Job #{jid} finished successfully; rows_updated={rng.randint(18, 95)}", res,
             jid=jid, duration_ms=rng.randint(2800, 4900))

    def add_abba_deadlock(target_date: str, user_a: str, user_b: str, trig_a: str = "AUTO", trig_b: str = "MANUAL") -> None:
        nonlocal job_id
        job_id += 1
        ja = job_id
        job_id += 1
        jb = job_id
        emit("INFO", "StockoutRisk", trig_a, user_a, target_date, "LOCK_ACQUIRE",
             f"Job #{ja} acquired EXCLUSIVE lock on WarehouseStocks({target_date})",
             ["WarehouseStocks"], jid=ja)
        emit("INFO", "DelayRisk", trig_b, user_b, target_date, "LOCK_ACQUIRE",
             f"Job #{jb} acquired EXCLUSIVE lock on TransitTelemetry({target_date})",
             ["TransitTelemetry"], jid=jb)
        emit("WARN", "StockoutRisk", trig_a, user_a, target_date, "LOCK_WAIT",
             f"Job #{ja} waiting for READ lock on TransitTelemetry({target_date}) held by Job #{jb}",
             ["WarehouseStocks", "TransitTelemetry"], jid=ja)
        emit("WARN", "DelayRisk", trig_b, user_b, target_date, "LOCK_WAIT",
             f"Job #{jb} waiting for READ lock on WarehouseStocks({target_date}) held by Job #{ja}",
             ["TransitTelemetry", "WarehouseStocks"], jid=jb)
        emit("ERROR", "StockoutRisk", trig_a, user_a, target_date, "ABORT_DEADLOCK",
             f"DeadlockError: cyclic wait detected between Job #{ja} (StockoutRisk) and Job #{jb} (DelayRisk) on WarehouseStocks<->TransitTelemetry ({target_date})",
             ["WarehouseStocks", "TransitTelemetry"], error_code="ERR_DEADLOCK_ABBA",
             incident_type="AB_BA_DEADLOCK", jid=ja, duration_ms=rng.randint(8000, 12000))
        emit("ERROR", "DelayRisk", trig_b, user_b, target_date, "ABORT_DEADLOCK",
             f"DeadlockError: transaction rolled back for Job #{jb} after wait timeout 10s",
             ["TransitTelemetry", "WarehouseStocks"], error_code="ERR_DEADLOCK_ABBA",
             incident_type="AB_BA_DEADLOCK", jid=jb, duration_ms=rng.randint(8000, 12000))

    def add_lost_update_race(target_date: str, user_manual: str) -> None:
        nonlocal job_id
        job_id += 1
        j_auto = job_id
        job_id += 1
        j_man = job_id
        emit("INFO", "StockoutRisk", "AUTO", "scheduler_bot", target_date, "SNAPSHOT_READ",
             f"Job #{j_auto} (AUTO reglament) read WarehouseStocks({target_date}) row_version=1",
             ["WarehouseStocks", "TransitTelemetry", "SupplierSLA"], jid=j_auto)
        emit("INFO", "SpoilageRisk", "MANUAL", user_manual, target_date, "SNAPSHOT_READ",
             f"Job #{j_man} (MANUAL by {user_manual}) overlapped reglament and read stale WarehouseStocks({target_date}) row_version=1",
             ["WarehouseStocks", "WeatherAndTraffic"], jid=j_man)
        emit("INFO", "StockoutRisk", "AUTO", "scheduler_bot", target_date, "COMMIT",
             f"Job #{j_auto} committed stockout_risk_flag to WarehouseStocks({target_date}), row_version=2",
             ["WarehouseStocks"], jid=j_auto, duration_ms=rng.randint(3200, 4500))
        emit("ERROR", "SpoilageRisk", "MANUAL", user_manual, target_date, "OVERWRITE_CONFLICT",
             f"DataCorruptionError: Lost Update detected! Job #{j_man} overwrote WarehouseStocks({target_date}) from version=1->3, erasing stockout_risk_flag written by Job #{j_auto}",
             ["WarehouseStocks"], error_code="ERR_LOST_UPDATE",
             incident_type="MANUAL_VS_REGLAMENT_LOST_UPDATE", jid=j_man, duration_ms=rng.randint(3600, 5100))

    def add_cross_date_staging_collision(date_a: str, date_b: str, user_a: str, user_b: str) -> None:
        nonlocal job_id
        job_id += 1
        ja = job_id
        job_id += 1
        jb = job_id
        emit("INFO", "CarrierDefaultRisk", "MANUAL", user_a, date_a, "STAGING_TRUNCATE",
             f"Job #{ja} cleared staging_carrier_sla_rolling and loaded 30d window for {date_a}",
             ["SupplierSLA", "staging_carrier_sla_rolling", "TransitTelemetry"], jid=ja)
        emit("WARN", "CarrierDefaultRisk", "MANUAL", user_b, date_b, "STAGING_TRUNCATE",
             f"Job #{jb} concurrently truncated staging_carrier_sla_rolling for {date_b} while Job #{ja} still aggregating!",
             ["SupplierSLA", "staging_carrier_sla_rolling", "TransitTelemetry"], jid=jb)
        emit("ERROR", "CarrierDefaultRisk", "MANUAL", user_a, date_a, "CORRUPT_AGGREGATE",
             f"CrossDateCollisionError: mixed business_dates ({date_a} & {date_b}) in staging_carrier_sla_rolling; SupplierSLA rolling on_time_rate corrupted",
             ["SupplierSLA", "staging_carrier_sla_rolling"], error_code="ERR_CROSS_DATE_STAGING",
             incident_type="CROSS_DATE_COLLISION", jid=ja, duration_ms=rng.randint(4100, 6000))
        emit("ERROR", "CarrierDefaultRisk", "MANUAL", user_b, date_b, "CORRUPT_AGGREGATE",
             f"CrossDateCollisionError: Job #{jb} aborted due to staging ownership mismatch with Job #{ja}",
             ["SupplierSLA", "staging_carrier_sla_rolling"], error_code="ERR_CROSS_DATE_STAGING",
             incident_type="CROSS_DATE_COLLISION", jid=jb, duration_ms=rng.randint(4100, 6000))

    def add_cross_date_dirty_read(date_writer: str, date_reader: str, user_w: str, user_r: str) -> None:
        nonlocal job_id
        job_id += 1
        jw = job_id
        job_id += 1
        jr = job_id
        emit("INFO", "CarrierDefaultRisk", "AUTO", user_w, date_writer, "WRITE_BEGIN",
             f"Job #{jw} started rewriting global mart SupplierSLA for {date_writer} (partial rows updated)",
             ["SupplierSLA"], jid=jw)
        emit("WARN", "DelayRisk", "MANUAL", user_r, date_reader, "DIRTY_READ",
             f"Job #{jr} read partially updated global mart SupplierSLA while Job #{jw} writing ({date_writer} vs {date_reader})",
             ["SupplierSLA", "TransitTelemetry"], jid=jr)
        emit("ERROR", "DelayRisk", "MANUAL", user_r, date_reader, "DATA_QUALITY_FAIL",
             f"DataCorruptionError: inconsistent SupplierSLA snapshot (checksum mismatch between CAR-04 and CAR-06) during DelayRisk({date_reader})",
             ["SupplierSLA", "TransitTelemetry"], error_code="ERR_DIRTY_READ_GLOBAL",
             incident_type="CROSS_DATE_DIRTY_READ", jid=jr, duration_ms=rng.randint(2900, 4200))
        emit("INFO", "CarrierDefaultRisk", "AUTO", user_w, date_writer, "COMMIT",
             f"Job #{jw} finished writing SupplierSLA (downstream DelayRisk #{jr} already corrupted)",
             ["SupplierSLA"], jid=jw, duration_ms=rng.randint(4500, 5800))

    # Сборка суточного цикла (86 событий, 38 задач)
    add_normal_run("SpoilageRisk", "AUTO", "scheduler_bot", "2026-11-19", ["WeatherAndTraffic", "WarehouseStocks"])
    add_normal_run("CarrierDefaultRisk", "AUTO", "scheduler_bot", "2026-11-19", ["TransitTelemetry", "WeatherAndTraffic", "SupplierSLA"])
    add_normal_run("DelayRisk", "MANUAL", "analyst_1", "2026-11-19", ["WeatherAndTraffic", "SupplierSLA", "WarehouseStocks", "TransitTelemetry"])
    add_lost_update_race("2026-11-20", "analyst_1")
    add_normal_run("SpoilageRisk", "MANUAL", "analyst_2", "2026-11-18", ["WeatherAndTraffic", "WarehouseStocks"])
    add_abba_deadlock("2026-11-20", "scheduler_bot", "analyst_2", "AUTO", "MANUAL")
    add_cross_date_staging_collision("2026-11-19", "2026-11-20", "analyst_1", "manager_1")
    add_normal_run("StockoutRisk", "MANUAL", "analyst_1", "2026-11-18", ["SupplierSLA", "TransitTelemetry", "WarehouseStocks"])
    add_abba_deadlock("2026-11-20", "analyst_1", "manager_1", "MANUAL", "MANUAL")
    add_cross_date_dirty_read("2026-11-20", "2026-11-19", "scheduler_bot", "analyst_2")
    add_lost_update_race("2026-11-20", "analyst_2")
    add_normal_run("DelayRisk", "AUTO", "scheduler_bot", "2026-11-18", ["WeatherAndTraffic", "SupplierSLA", "WarehouseStocks", "TransitTelemetry"])
    add_cross_date_staging_collision("2026-11-20", "2026-11-21", "analyst_2", "analyst_1")
    add_abba_deadlock("2026-11-21", "scheduler_bot", "analyst_1", "AUTO", "MANUAL")
    add_cross_date_dirty_read("2026-11-21", "2026-11-20", "scheduler_bot", "manager_1")
    add_lost_update_race("2026-11-21", "manager_1")
    add_cross_date_staging_collision("2026-11-18", "2026-11-21", "manager_1", "analyst_2")
    add_normal_run("SpoilageRisk", "AUTO", "scheduler_bot", "2026-11-21", ["WeatherAndTraffic", "WarehouseStocks"])
    add_abba_deadlock("2026-11-21", "analyst_2", "analyst_1", "MANUAL", "MANUAL")
    add_normal_run("StockoutRisk", "AUTO", "scheduler_bot", "2026-11-22", ["SupplierSLA", "TransitTelemetry", "WarehouseStocks"])
    add_lost_update_race("2026-11-22", "analyst_1")
    add_cross_date_dirty_read("2026-11-22", "2026-11-21", "scheduler_bot", "analyst_1")
    add_normal_run("DelayRisk", "MANUAL", "manager_1", "2026-11-22", ["WeatherAndTraffic", "SupplierSLA", "WarehouseStocks", "TransitTelemetry"])
    add_normal_run("CarrierDefaultRisk", "AUTO", "scheduler_bot", "2026-11-22", ["TransitTelemetry", "WeatherAndTraffic", "SupplierSLA"])
    add_normal_run("SpoilageRisk", "MANUAL", "analyst_2", "2026-11-22", ["WeatherAndTraffic", "WarehouseStocks"])

    jsonl_path = out_dir / "legacy_system_logs.jsonl"
    txt_path = out_dir / "legacy_system_logs.log"
    with jsonl_path.open("w", encoding="utf-8") as fj, txt_path.open("w", encoding="utf-8") as ft:
        for ev in events:
            fj.write(json.dumps(ev, ensure_ascii=False) + "\n")
            err = f" code={ev['error_code']}" if ev["error_code"] else ""
            ft.write(
                f"{ev['timestamp']} [{str(ev['level']):5s}] job=#{ev['job_id']} "
                f"svc={str(ev['service']):18s} trig={str(ev['trigger']):6s} user={str(ev['user_id']):13s} "
                f"date={ev['target_date']} action={ev['action']}{err} | {ev['message']}\n"
            )
    return {
        "total_events": len(events),
        "levels": dict(Counter(str(e["level"]) for e in events)),
        "incidents": dict(Counter(str(e["incident_type"]) for e in events if e["incident_type"])),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Генерация синтетических сбойных логов старой системы.")
    parser.add_argument("--out-dir", type=Path, default=Path("synthetic_data"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    summary = generate_legacy_logs(args.out_dir, seed=args.seed)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
