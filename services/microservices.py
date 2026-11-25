"""services/microservices.py — SharedDataLayer и 4 эмулятора логистических микросервисов.

Микросервис не знает, идёт ли он через LockManager. Без оркестратора коллизии
выявляются самим SharedDataLayer и приводят к честным исключениям:
  * DeadlockError           — цикл ожидания AB-BA (StockoutRisk <-> DelayRisk);
  * DataCorruptionError     — Lost Update (row_version) / грязное чтение глобальной SupplierSLA;
  * CrossDateCollisionError — коллизия owner_task_id в общем staging-буфере (CarrierDefaultRisk).
"""

from __future__ import annotations

import asyncio
import copy
import csv
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from core.models import (
    AccessMode,
    CrossDateCollisionError,
    DataCorruptionError,
    DeadlockError,
    LockScope,
    ProcessSpec,
    ProgressCallback,
    ResourceRule,
    TriggerType,
)

STOCKOUT, DELAY, SPOILAGE, CARRIER = (
    "StockoutRisk",
    "DelayRisk",
    "SpoilageRisk",
    "CarrierDefaultRisk",
)
WAREHOUSE, TRANSIT, WEATHER, SLA = (
    "WarehouseStocks",
    "TransitTelemetry",
    "WeatherAndTraffic",
    "SupplierSLA",
)
SOURCES = (WAREHOUSE, TRANSIT, WEATHER, SLA)
GLOBAL_SOURCES = frozenset({SLA})

_FILES = {
    WAREHOUSE: "warehouse_stocks",
    TRANSIT: "transit_telemetry",
    WEATHER: "weather_traffic",
    SLA: "supplier_sla",
}

# Компактные данные по умолчанию, если synthetic_data/ пуста.
_DEFAULTS: dict[str, list[dict[str, Any]]] = {
    WAREHOUSE: [
        {"sku": "A1", "warehouse": "W1", "qty": 40, "min_qty": 50, "days_to_expiry": 2},
        {"sku": "B2", "warehouse": "W1", "qty": 200, "min_qty": 80, "days_to_expiry": 30},
        {"sku": "C3", "warehouse": "W2", "qty": 15, "min_qty": 30, "days_to_expiry": 5},
        {"sku": "D4", "warehouse": "W2", "qty": 90, "min_qty": 60, "days_to_expiry": 1},
    ],
    TRANSIT: [
        {"shipment": "S1", "sku": "A1", "carrier": "FastTrans", "qty": 30, "eta_delay_h": 2, "status": "moving"},
        {"shipment": "S2", "sku": "C3", "carrier": "SlowLog", "qty": 40, "eta_delay_h": 18, "status": "delayed"},
        {"shipment": "S3", "sku": "B2", "carrier": "FastTrans", "qty": 100, "eta_delay_h": 0, "status": "moving"},
    ],
    WEATHER: [{"route": "R1", "severity": 0.2}, {"route": "R2", "severity": 0.7}],
    SLA: [{"carrier": "FastTrans", "on_time_rate": 0.95}, {"carrier": "SlowLog", "on_time_rate": 0.6}],
}


def _num(v: str) -> Any:
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in v else f
    except (TypeError, ValueError):
        return v


def _load(data_dir: Path, source: str) -> list[dict[str, Any]]:
    """CSV/JSON из synthetic_data/ при наличии, иначе компактный дефолт."""
    try:
        csv_path, json_path = (data_dir / f"{_FILES[source]}{e}" for e in (".csv", ".json"))
        if csv_path.exists():
            with csv_path.open(newline="", encoding="utf-8") as f:
                return [{k: _num(v) for k, v in row.items()} for row in csv.DictReader(f)]
        if json_path.exists():
            data = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
    except (OSError, ValueError):
        pass
    return copy.deepcopy(_DEFAULTS[source])


@dataclass
class _Table:
    rows: list[dict[str, Any]]
    version: int = 1
    partition_versions: dict[str, int] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)


class SharedDataLayer:
    """4 общие витрины + общий staging-буфер + детектор коллизий (без блокировок оркестратора)."""

    def __init__(self, data_dir: str | Path | None = None, time_scale: float = 1.0) -> None:
        self.data_dir = (
            Path(data_dir)
            if data_dir
            else Path(__file__).resolve().parent.parent / "synthetic_data"
        )
        self.time_scale = time_scale  # 1.0 = штатные ~3.8 с; <1 ускоряет тесты
        self._initial = {s: _load(self.data_dir, s) for s in SOURCES}
        self.reset()

    def reset(self) -> None:
        self.tables = {s: _Table(copy.deepcopy(r)) for s, r in self._initial.items()}
        # resource_key -> {task_id: (mode, process_id, target_date)}
        self.holders: dict[str, dict[int, tuple[str, str, str]]] = {}
        # task_id -> {resource_key: mode}
        self.held: dict[int, dict[str, str]] = {}
        # task_id -> (resource_key, mode)
        self.waiting: dict[int, tuple[str, str]] = {}
        # task_id -> {resource_key: version}
        self.snapshots: dict[int, dict[str, int]] = {}
        # dom_sla_staging_buffer
        self.staging: dict[str, Any] = {"owner_task_id": None, "date": None, "values": []}
        self._displaced: dict[int, tuple[int, str]] = {}

    @staticmethod
    def _res_key(source: str, target_date: str | None) -> str:
        if source in GLOBAL_SOURCES or not target_date:
            return f"{source}:GLOBAL"
        return f"{source}:{target_date}"

    def _get_version(self, source: str, res_key: str) -> int:
        tbl = self.tables[source]
        if source in GLOBAL_SOURCES:
            return tbl.version
        return tbl.partition_versions.get(res_key, 1)

    # ---- чтение / версии ----
    def rows(self, source: str, target_date: str | None = None) -> list[dict[str, Any]]:
        all_rows = self.tables[source].rows
        if target_date and source not in GLOBAL_SOURCES and all_rows and "business_date" in all_rows[0]:
            filtered = [r for r in all_rows if str(r.get("business_date")) == target_date]
            if filtered:
                return copy.deepcopy(filtered)
        return copy.deepcopy(all_rows)

    def versions(self) -> dict[str, int]:
        return {s: t.version for s, t in self.tables.items()}

    # ---- доступ к витринам с детекцией Deadlock / Lost Update / Dirty Read ----
    def _blockers(self, task_id: int, res_key: str, mode: str) -> set[int]:
        holders = self.holders.get(res_key, {})
        return {t for t, (m, _, _) in holders.items() if t != task_id and "W" in (m, mode)}

    def _has_cycle(self, me: int) -> bool:
        seen: set[int] = set()
        stack = [me]
        while stack:
            t = stack.pop()
            w = self.waiting.get(t)
            if not w:
                continue
            for b in self._blockers(t, *w):
                if b == me:
                    return True
                if b not in seen:
                    seen.add(b)
                    stack.append(b)
        return False

    async def access(
        self,
        task_id: int,
        process_id: str,
        source: str,
        mode: str,
        target_date: str | None = None,
    ) -> None:
        """Захватывает ресурс ('R'/'W') с учётом гранулярности (BY_DATE vs GLOBAL)."""
        res_key = self._res_key(source, target_date)
        cur = self.held.get(task_id, {}).get(res_key)
        if cur == "W" or (cur and mode == "R"):
            return

        # Записываем снимок row_version в момент первого обращения задачи к ресурсу
        self.snapshots.setdefault(task_id, {}).setdefault(
            res_key, self._get_version(source, res_key)
        )

        self.waiting[task_id] = (res_key, mode)
        ticks = 0
        try:
            while blockers := self._blockers(task_id, res_key, mode):
                holders_map = self.holders.get(res_key, {})
                names = ", ".join(
                    f"#{t} {holders_map[t][1]} ({holders_map[t][2]})"
                    for t in sorted(blockers)
                    if t in holders_map
                )
                if self._has_cycle(task_id):
                    raise DeadlockError(
                        f"AB-BA Deadlock: задача #{task_id} ({process_id}) ждёт {res_key}, "
                        f"удерживаемый {names}, в цикле взаимной блокировки"
                    )
                if task_id in self._displaced:
                    self._staging_check(task_id, target_date or "")
                if source in GLOBAL_SOURCES and mode == "R":
                    raise DataCorruptionError(
                        f"Грязное чтение глобальной витрины {source}: задача #{task_id} ({process_id}, "
                        f"{target_date}) читает несогласованный срез во время записи {names}"
                    )
                if ticks >= 4:
                    is_ww = mode == "W" and any(
                        holders_map.get(t, ("R", "", ""))[0] == "W" for t in blockers
                    )
                    if is_ww:
                        raise DataCorruptionError(
                            f"Lost Update (row_version): задача #{task_id} ({process_id}) "
                            f"пересеклась по записи {res_key} с {names}"
                        )
                    raise DataCorruptionError(
                        f"Грязное чтение витрины {res_key}: задача #{task_id} ({process_id}) "
                        f"пересеклась с записью {names}"
                    )
                await asyncio.sleep(0.05 * self.time_scale)
                ticks += 1
        finally:
            self.waiting.pop(task_id, None)

        self.holders.setdefault(res_key, {})[task_id] = (mode, process_id, target_date or "GLOBAL")
        self.held.setdefault(task_id, {})[res_key] = mode

    def commit(
        self,
        task_id: int,
        writes: dict[str, dict[str, Any]],
        target_date: str | None = None,
    ) -> None:
        """Проверка row_version (защита от Lost Update и грязного чтения) и запись в витрины."""
        write_res_keys = {self._res_key(src, target_date) for src in writes}
        for res_key, ver in self.snapshots.get(task_id, {}).items():
            src = res_key.split(":", 1)[0]
            cur = self._get_version(src, res_key)
            if cur != ver:
                kind = (
                    "Lost Update (конфликт row_version)"
                    if res_key in write_res_keys
                    else f"Грязное чтение витрины {src}"
                )
                raise DataCorruptionError(
                    f"{kind}: {res_key} изменена другой задачей (row_version v{ver} -> v{cur}) "
                    f"во время выполнения #{task_id}"
                )
        for src, payload in writes.items():
            res_key = self._res_key(src, target_date)
            t = self.tables[src]
            t.meta.update(payload)
            t.version += 1
            t.partition_versions[res_key] = self._get_version(src, res_key) + 1

    def release(self, task_id: int) -> None:
        for res_key in self.held.pop(task_id, {}):
            holders = self.holders.get(res_key)
            if holders:
                holders.pop(task_id, None)
                if not holders:
                    self.holders.pop(res_key, None)
        self.waiting.pop(task_id, None)
        self.snapshots.pop(task_id, None)
        self._displaced.pop(task_id, None)
        if self.staging["owner_task_id"] == task_id:
            self.staging.update(owner_task_id=None, date=None, values=[])

    # ---- общий staging-буфер скользящих средних (dom_sla_staging_buffer) ----
    def staging_reset(self, task_id: int, target_date: str) -> None:
        """Очищает dom_sla_staging_buffer и фиксирует owner_task_id."""
        prev = self.staging["owner_task_id"]
        prev_date = self.staging["date"]
        if prev is not None and prev != task_id:
            self._displaced[prev] = (task_id, target_date)
            if prev_date and prev_date != target_date:
                # Второй процесс за другую дату тоже помечается как участник коллизии буфера,
                # но ошибку выбросит на этапе агрегации (шаг 4), успев инициировать запись SLA.
                self._displaced[task_id] = (prev, str(prev_date))
        self.staging.update(owner_task_id=task_id, date=target_date, values=[])

    def _staging_check(self, task_id: int, target_date: str) -> None:
        if task_id in self._displaced:
            by, d = self._displaced[task_id]
            exc = CrossDateCollisionError if d != target_date else DataCorruptionError
            raise exc(
                f"Коллизия owner_task_id в dom_sla_staging_buffer (staging_carrier_sla_rolling): "
                f"буфер задачи #{task_id} ({target_date}) перезаписан задачей #{by} ({d})"
            )

    def staging_push(self, task_id: int, target_date: str, value: Any, *, check: bool = True) -> None:
        if check:
            self._staging_check(task_id, target_date)
        self.staging["values"].append(value)

    def staging_values(self, task_id: int, target_date: str) -> list[Any]:
        self._staging_check(task_id, target_date)
        return list(self.staging["values"])


@dataclass
class _Ctx:
    task_id: int
    target_date: str
    trigger: TriggerType
    data: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)


def _mean(vals: list[float], default: float = 0.0) -> float:
    return sum(vals) / len(vals) if vals else default


class BaseRiskService(ABC):
    """Общий каркас: 5 шагов по 20% (~3.75 с суммарно), прогресс после каждого шага."""

    spec: ProcessSpec

    def __init__(self, db: SharedDataLayer) -> None:
        self.db = db

    @abstractmethod
    async def _step(self, i: int, c: _Ctx) -> None:
        """Шаг i=1..5: обращение к витринам / расчёт; на шаге 5 — commit и c.result."""

    async def _acc(self, c: _Ctx, source: str, mode: str) -> list[dict[str, Any]]:
        await self.db.access(c.task_id, self.spec.process_id, source, mode, c.target_date)
        return self.db.rows(source, c.target_date)

    async def run(
        self,
        task_id: int,
        target_date: str,
        trigger: TriggerType,
        on_progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        c = _Ctx(task_id, target_date, trigger)
        pause = 0.75 * self.db.time_scale
        try:
            for i in range(1, 6):
                await self._step(i, c)
                await asyncio.sleep(pause)
                if on_progress:
                    await on_progress(task_id, i * 20)
            return {
                "process_id": self.spec.process_id,
                "target_date": target_date,
                "trigger": trigger.name,
                **c.result,
                "versions": self.db.versions(),
            }
        finally:
            self.db.release(task_id)


class StockoutRiskService(BaseRiskService):
    spec = ProcessSpec(
        process_id=STOCKOUT,
        display_name="Оценка риска дефицита (StockoutRisk)",
        read_sources=(TRANSIT, SLA),
        write_sources=(WAREHOUSE,),
        allow_parallel_dates=False,
        default_priority=1,
        resource_rules=(
            ResourceRule(WAREHOUSE, AccessMode.WRITE, LockScope.BY_DATE, 1),
            ResourceRule(TRANSIT, AccessMode.READ, LockScope.BY_DATE, 2),
            ResourceRule(SLA, AccessMode.READ, LockScope.GLOBAL, 3),
        ),
        uses_shared_staging=False,
        staging_key=None,
        timeout_sec=30.0,
        estimated_duration_s=4.0,
    )

    async def _step(self, i: int, c: _Ctx) -> None:
        d = c.data
        if i == 1:
            # acquire_order = 1: WRITE WarehouseStocks (BY_DATE)
            d["wh"] = await self._acc(c, WAREHOUSE, "W")
        elif i == 2:
            # acquire_order = 2: READ TransitTelemetry (BY_DATE) — встречный захват к DelayRisk (AB-BA)
            d["tt"] = await self._acc(c, TRANSIT, "R")
        elif i == 3:
            # acquire_order = 3: READ SupplierSLA (GLOBAL)
            d["sla"] = await self._acc(c, SLA, "R")
        elif i == 4:
            reliable = {
                r.get("carrier_id") or r.get("carrier")
                for r in d["sla"]
                if float(r.get("on_time_rate", 1.0)) >= 0.8
            }
            inbound: dict[tuple[str, str], float] = {}
            for r in d["tt"]:
                carrier = r.get("carrier_id") or r.get("carrier")
                status = str(r.get("gps_status") or r.get("status") or "ON_TIME").upper()
                if carrier in reliable and status != "STOPPED":
                    wh = str(r.get("warehouse_id") or r.get("warehouse") or "")
                    sku = str(r.get("sku_id") or r.get("sku") or "")
                    qty = float(r.get("cargo_qty") if "cargo_qty" in r else r.get("qty", 0))
                    inbound[(wh, sku)] = inbound.get((wh, sku), 0.0) + qty

            flags: list[str] = []
            for r in d["wh"]:
                wh = str(r.get("warehouse_id") or r.get("warehouse") or "")
                sku = str(r.get("sku_id") or r.get("sku") or "")
                qty_on_hand = float(r.get("qty_on_hand") if "qty_on_hand" in r else r.get("qty", 0))
                qty_reserved = float(r.get("qty_reserved", 0))
                reorder_pt = float(r.get("reorder_point") if "reorder_point" in r else r.get("min_qty", 0))
                avail = qty_on_hand - qty_reserved
                if avail < reorder_pt * 0.5 or (avail + inbound.get((wh, sku), 0.0) < reorder_pt):
                    label = f"{wh}:{sku}" if wh else sku
                    flags.append(label)
            d["flags"] = sorted(set(flags))
        else:
            self.db.commit(
                c.task_id,
                {WAREHOUSE: {f"stockout_flags:{c.target_date}": d["flags"]}},
                c.target_date,
            )
            c.result = {"stockout_skus": d["flags"], "stockout_count": len(d["flags"])}


class DelayRiskService(BaseRiskService):
    spec = ProcessSpec(
        process_id=DELAY,
        display_name="Оценка риска задержки (DelayRisk)",
        read_sources=(WAREHOUSE, WEATHER, SLA),
        write_sources=(TRANSIT,),
        allow_parallel_dates=True,
        default_priority=1,
        resource_rules=(
            ResourceRule(TRANSIT, AccessMode.WRITE, LockScope.BY_DATE, 1),
            ResourceRule(WAREHOUSE, AccessMode.READ, LockScope.BY_DATE, 2),
            ResourceRule(WEATHER, AccessMode.READ, LockScope.BY_DATE, 3),
            ResourceRule(SLA, AccessMode.READ, LockScope.GLOBAL, 4),
        ),
        uses_shared_staging=False,
        staging_key=None,
        timeout_sec=30.0,
        estimated_duration_s=4.0,
    )

    async def _step(self, i: int, c: _Ctx) -> None:
        d = c.data
        if i == 1:
            # acquire_order = 1: WRITE TransitTelemetry (BY_DATE)
            d["tt"] = await self._acc(c, TRANSIT, "W")
        elif i == 2:
            # acquire_order = 2: READ WarehouseStocks (BY_DATE) — встречный захват к StockoutRisk (AB-BA)
            d["wh"] = await self._acc(c, WAREHOUSE, "R")
        elif i == 3:
            # acquire_order = 3: READ WeatherAndTraffic (BY_DATE), acquire_order = 4: READ SupplierSLA (GLOBAL)
            d["wx"] = await self._acc(c, WEATHER, "R")
            d["sla"] = await self._acc(c, SLA, "R")
        elif i == 4:
            sev = _mean([
                float(r.get("weather_severity") if "weather_severity" in r else r.get("severity", 0.0))
                for r in d["wx"]
            ])
            rate = {
                (r.get("carrier_id") or r.get("carrier")): float(r.get("on_time_rate", 1.0))
                for r in d["sla"]
            }
            low_skus = {
                (r.get("sku_id") or r.get("sku"))
                for r in d["wh"]
                if (float(r.get("qty_on_hand") if "qty_on_hand" in r else r.get("qty", 0)) - float(r.get("qty_reserved", 0)))
                < float(r.get("reorder_point") if "reorder_point" in r else r.get("min_qty", 0))
            }
            scores: dict[str, float] = {}
            critical: list[str] = []
            for r in d["tt"]:
                shp = str(r.get("shipment_id") or r.get("shipment") or "")
                carrier = r.get("carrier_id") or r.get("carrier")
                if "actual_eta_hours" in r and "planned_eta_hours" in r:
                    delay_h = max(0.0, float(r["actual_eta_hours"]) - float(r["planned_eta_hours"]))
                else:
                    delay_h = float(r.get("eta_delay_h", 0.0))
                score = round(
                    min(1.0, delay_h / 24.0 + 0.4 * sev + 0.4 * (1.0 - rate.get(carrier, 1.0))),
                    2,
                )
                scores[shp] = score
                sku = r.get("sku_id") or r.get("sku")
                status = str(r.get("gps_status") or r.get("status") or "").upper()
                if score >= 0.5 or status in ("DELAYED", "STOPPED") or (sku in low_skus and score >= 0.35):
                    critical.append(shp)
            d["scores"] = scores
            d["critical"] = sorted(set(critical))
        else:
            self.db.commit(
                c.task_id,
                {TRANSIT: {f"delay_scores:{c.target_date}": d["scores"]}},
                c.target_date,
            )
            c.result = {
                "delay_scores": d["scores"],
                "critical_shipments": d["critical"],
                "delayed_count": len(d["critical"]),
            }


class SpoilageRiskService(BaseRiskService):
    spec = ProcessSpec(
        process_id=SPOILAGE,
        display_name="Оценка риска порчи (SpoilageRisk)",
        read_sources=(WEATHER, WAREHOUSE),
        write_sources=(WAREHOUSE,),
        allow_parallel_dates=True,
        default_priority=2,
        resource_rules=(
            ResourceRule(WAREHOUSE, AccessMode.WRITE, LockScope.BY_DATE, 1),
            ResourceRule(WEATHER, AccessMode.READ, LockScope.BY_DATE, 2),
        ),
        uses_shared_staging=False,
        staging_key=None,
        timeout_sec=30.0,
        estimated_duration_s=4.0,
    )

    async def _step(self, i: int, c: _Ctx) -> None:
        d = c.data
        if i == 1:
            d["wh"] = await self._acc(c, WAREHOUSE, "W")
        elif i == 2:
            d["wx"] = await self._acc(c, WEATHER, "R")
        elif i == 4:
            sev = _mean([
                float(r.get("weather_severity") if "weather_severity" in r else r.get("severity", 0.0))
                for r in d["wx"]
            ])
            limit = 3 + (2 if sev > 0.5 else 0)
            writeoff: list[str] = []
            try:
                cur_date = date.fromisoformat(c.target_date)
            except ValueError:
                cur_date = date(2026, 11, 20)

            for r in d["wh"]:
                wh = str(r.get("warehouse_id") or r.get("warehouse") or "")
                sku = str(r.get("sku_id") or r.get("sku") or "")
                if "days_to_expiry" in r:
                    days_left = int(r["days_to_expiry"])
                elif r.get("expiry_date"):
                    try:
                        days_left = (date.fromisoformat(str(r["expiry_date"])) - cur_date).days
                    except ValueError:
                        days_left = 99
                else:
                    days_left = 99

                temp_c = float(r.get("temp_celsius", 4.0))
                cat = str(r.get("sku_category", ""))
                is_perishable = cat in ("DAIRY_FRESH", "MEAT_CHILLED", "PRODUCE_FRESH", "PHARMA_COLD")
                if days_left <= limit or (is_perishable and temp_c > 8.5):
                    writeoff.append(f"{wh}:{sku}" if wh else sku)
            d["writeoff"] = sorted(set(writeoff))
        elif i == 5:
            self.db.commit(
                c.task_id,
                {WAREHOUSE: {f"writeoff_flags:{c.target_date}": d["writeoff"]}},
                c.target_date,
            )
            c.result = {"writeoff_skus": d["writeoff"], "spoilage_count": len(d["writeoff"])}


class CarrierDefaultRiskService(BaseRiskService):
    spec = ProcessSpec(
        process_id=CARRIER,
        display_name="Оценка риска дефолта перевозчика (CarrierDefaultRisk)",
        read_sources=(TRANSIT, WEATHER),
        write_sources=(SLA,),
        allow_parallel_dates=False,
        default_priority=2,
        resource_rules=(
            ResourceRule(SLA, AccessMode.WRITE, LockScope.GLOBAL, 1),
            ResourceRule(TRANSIT, AccessMode.READ, LockScope.BY_DATE, 2),
            ResourceRule(WEATHER, AccessMode.READ, LockScope.BY_DATE, 3),
        ),
        uses_shared_staging=True,
        staging_key="staging_carrier_sla_rolling",
        timeout_sec=30.0,
        estimated_duration_s=5.0,
    )

    async def _step(self, i: int, c: _Ctx) -> None:
        db, d = self.db, c.data
        if i == 1:
            # Очищаем общий staging-буфер и открываем запись в глобальную витрину SupplierSLA
            db.staging_reset(c.task_id, c.target_date)
            d["sla"] = await self._acc(c, SLA, "W")
        elif i == 2:
            # Читаем партиционированную телеметрию за target_date
            d["tt"] = await self._acc(c, TRANSIT, "R")
        elif i == 3:
            wx = await self._acc(c, WEATHER, "R")
            d["sev"] = _mean([
                float(r.get("weather_severity") if "weather_severity" in r else r.get("severity", 0.0))
                for r in wx
            ])
            by_carrier: dict[str, list[bool]] = {}
            for r in d["tt"]:
                carrier = str(r.get("carrier_id") or r.get("carrier") or "")
                if "actual_eta_hours" in r and "planned_eta_hours" in r:
                    delay_h = max(0.0, float(r["actual_eta_hours"]) - float(r["planned_eta_hours"]))
                else:
                    delay_h = float(r.get("eta_delay_h", 0.0))
                status = str(r.get("gps_status") or r.get("status") or "").upper()
                by_carrier.setdefault(carrier, []).append(delay_h > 4.0 or status in ("DELAYED", "STOPPED"))
            for carrier, flags in by_carrier.items():
                db.staging_push(
                    c.task_id,
                    c.target_date,
                    ("share", carrier, _mean([float(f) for f in flags])),
                    check=False,
                )
            db.staging_push(c.task_id, c.target_date, ("sev", d["sev"]), check=False)
        elif i == 4:
            # Проверка целостности staging-буфера dom_sla_staging_buffer (owner_task_id)
            vals = db.staging_values(c.task_id, c.target_date)
            sev = next((float(v[1]) for v in vals if v[0] == "sev"), 0.0)
            old = {
                str(r.get("carrier_id") or r.get("carrier")): float(r.get("on_time_rate", 0.9))
                for r in d["sla"]
            }
            d["ratings"] = {
                str(v[1]): round(
                    0.7 * old.get(str(v[1]), 0.9) + 0.3 * max(0.0, 1.0 - float(v[2]) * (1.0 + sev)),
                    3,
                )
                for v in vals
                if v[0] == "share"
            }
        else:
            db.staging_values(c.task_id, c.target_date)
            db.commit(
                c.task_id,
                {SLA: {f"rolling_rating:{c.target_date}": d["ratings"]}},
                c.target_date,
            )
            c.result = {"rolling_rating": d["ratings"]}


PROCESS_SPECS: dict[str, ProcessSpec] = {
    cls.spec.process_id: cls.spec
    for cls in (
        StockoutRiskService,
        DelayRiskService,
        SpoilageRiskService,
        CarrierDefaultRiskService,
    )
}


def create_services(db: SharedDataLayer) -> dict[str, BaseRiskService]:
    return {
        STOCKOUT: StockoutRiskService(db),
        DELAY: DelayRiskService(db),
        SPOILAGE: SpoilageRiskService(db),
        CARRIER: CarrierDefaultRiskService(db),
    }
