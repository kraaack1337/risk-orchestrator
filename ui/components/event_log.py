"""Right Inspector Dock (340px, DBeaver Session Manager style):
1. RWLockManager Live Lock Monitor (4 data marts + Staging buffer)
2. Selected Task Inspector (JSON risk payload, lock keys, error details)
3. Monospace Live Event Log with semantic level highlighting
"""

from __future__ import annotations

import json
from typing import Any

from ui.assets.theme import (
    BG_DEEP,
    BG_ELEVATED,
    BG_MAIN,
    BG_PANEL,
    BORDER,
    CORNER_RADIUS,
    STATUS_COMPLETED,
    STATUS_DEDUPLICATED,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    TEXT_MUTED,
    TEXT_PRIMARY,
    font_mono,
    font_ui,
    status_badge_colors,
)
from ui.contracts import (
    SOURCE_IDS,
    OrchestratorEvent,
    OrchestratorProtocol,
    TaskRun,
    extract_error_type,
    status_key,
    trigger_key,
)

LOCK_MONITOR_ROWS: tuple[tuple[str, str], ...] = (
    ("WarehouseStocks", "WarehouseStocks"),
    ("TransitTelemetry", "TransitTelemetry"),
    ("WeatherAndTraffic", "WeatherAndTraffic"),
    ("SupplierSLA", "SupplierSLA"),
    ("__STAGING__", "Staging-буфер"),
)


def compute_lock_monitor_rows(
    active_locks: dict[str, list[int]],
    tasks_by_id: dict[int, TaskRun],
) -> list[tuple[str, str, str]]:
    """Map the 5 canonical resources to (display_name, mode_key, badge_text).

    mode_key is one of 'FREE', 'SHARED', 'EXCLUSIVE'.
    """
    out: list[tuple[str, str, str]] = []
    for res_id, label in LOCK_MONITOR_ROWS:
        excl_holders: list[int] = []
        shrd_holders: list[int] = []

        for key, holders in active_locks.items():
            upper = key.upper()
            if res_id == "__STAGING__":
                matched = ":STAGING:" in upper or ":PROCESS:" in upper or upper.startswith("PROCESS:")
            else:
                matched = res_id in key
            if not matched:
                continue

            if upper.startswith("WRITE") or "EXCLUSIVE" in upper or ":PROCESS:" in upper or ":STAGING:" in upper:
                excl_holders.extend(holders)
            else:
                shrd_holders.extend(holders)

        if excl_holders:
            uniq = sorted(set(excl_holders))
            first_id = uniq[0]
            proc = tasks_by_id[first_id].process_id if first_id in tasks_by_id else ""
            suffix = f" +{len(uniq) - 1}" if len(uniq) > 1 else ""
            proc_part = f" {proc}" if proc else ""
            out.append((label, "EXCLUSIVE", f"[EXCLUSIVE: #{first_id}{proc_part}{suffix}]"))
        elif shrd_holders:
            uniq = sorted(set(shrd_holders))
            ids_txt = ", ".join(f"#{tid}" for tid in uniq[:3])
            if len(uniq) > 3:
                ids_txt += f" +{len(uniq) - 3}"
            out.append((label, "SHARED", f"[SHARED: {ids_txt}]"))
        else:
            out.append((label, "FREE", "[FREE]"))
    return out


def format_task_inspector_text(
    task: TaskRun | None,
    active_locks: dict[str, list[int]],
    spec_locks_hint: tuple[str, ...] = (),
) -> str:
    """Format detailed inspection text for the selected TaskRun."""
    if task is None:
        return "Кликните по любой карточке задачи в колонках статусов или на графике Ганта для просмотра деталей, ключей блокировок и JSON-результата."

    st = status_key(task.status)
    trig = trigger_key(task.trigger)
    peff = task.effective_priority if task.effective_priority is not None else float(task.priority)
    wait_s = (
        max(0.0, (task.started_at - task.queued_at).total_seconds())
        if task.started_at
        else 0.0
    )
    exec_s = (
        max(0.0, (task.finished_at - task.started_at).total_seconds())
        if task.started_at and task.finished_at
        else 0.0
    )

    held_keys = [k for k, owners in active_locks.items() if task.task_id in owners]
    keys_display = held_keys or list(spec_locks_hint)

    lines = [
        f"TASK #{task.task_id}  [{st}]",
        f"Сервис     : {task.process_id}",
        f"Дата       : {task.target_date}",
        f"Инициатор  : {task.initiated_by} ({trig})",
        f"Приоритет  : P={task.priority} (Peff={peff:.1f})",
        f"Тайминг    : wait={wait_s:.2f}s | exec={exec_s:.2f}s | {task.progress}%",
    ]
    if keys_display:
        lines.append("Ключи RWLockManager:")
        for k in keys_display:
            lines.append(f"  • {k}")

    if task.blocked_by:
        lines.append(f"\nБЛОКИРОВКА ОЧЕРЕДИ:\n  {task.blocked_by}")

    if task.error_message:
        exc_name = extract_error_type(task)
        lines.append(f"\nИСКЛЮЧЕНИЕ ({exc_name}):\n  {task.error_message}")

    if task.result is not None:
        pretty_json = json.dumps(task.result, ensure_ascii=False, indent=2)
        lines.append(f"\nJSON РЕЗУЛЬТАТ РАСЧЁТА:\n{pretty_json}")

    return "\n".join(lines)


class InspectorAndLogDock:
    """Right 340px vertical dock: Lock Monitor + Selected Task Inspector + Live Event Log."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
    ) -> None:
        import customtkinter as ctk

        self._ctk = ctk
        self._orchestrator = orchestrator
        self._selected_task_id: int | None = None
        self._spec_map = {s.process_id: s for s in orchestrator.get_process_specs()}

        self._locks_fp: Any = None
        self._inspector_fp: Any = None
        self._events_fp: Any = None

        self.frame = ctk.CTkFrame(parent, width=370, fg_color="transparent")
        self.frame.grid_propagate(False)
        self.frame.grid_columnconfigure(0, weight=1)
        self.frame.grid_rowconfigure(0, weight=0)
        self.frame.grid_rowconfigure(1, weight=1)
        self.frame.grid_rowconfigure(2, weight=1)

        # ---- 1. Top Block: RWLockManager Monitor ----
        lock_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        lock_card.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        lock_card.grid_columnconfigure(0, weight=1)

        hdr1 = ctk.CTkFrame(lock_card, height=26, fg_color=BG_ELEVATED, corner_radius=5)
        hdr1.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 4))
        hdr1.grid_propagate(False)
        ctk.CTkLabel(
            hdr1,
            text="МОНИТОР БЛОКИРОВОК (RWLOCKMANAGER)",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        self._locks_mode_badge = ctk.CTkLabel(
            hdr1,
            text="ACTIVE",
            font=font_mono(9, "bold"),
            text_color=STATUS_COMPLETED,
        )
        self._locks_mode_badge.pack(side="right", padx=8)

        rows_box = ctk.CTkFrame(lock_card, fg_color="transparent")
        rows_box.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))
        rows_box.grid_columnconfigure(0, weight=1)

        self._lock_badges: dict[str, Any] = {}
        for idx, (_res_id, label) in enumerate(LOCK_MONITOR_ROWS):
            r_frame = ctk.CTkFrame(rows_box, height=26, fg_color=BG_MAIN, corner_radius=4)
            r_frame.grid(row=idx, column=0, sticky="ew", pady=1)
            r_frame.grid_propagate(False)

            ctk.CTkLabel(
                r_frame,
                text=label,
                font=font_mono(10),
                text_color=TEXT_PRIMARY,
            ).pack(side="left", padx=7)

            bg_c, fg_c = status_badge_colors("FREE")
            b_lbl = ctk.CTkLabel(
                r_frame,
                text="[FREE]",
                font=font_mono(9, "bold"),
                fg_color=bg_c,
                text_color=fg_c,
                corner_radius=4,
                padx=6,
                pady=1,
            )
            b_lbl.pack(side="right", padx=5, pady=3)
            self._lock_badges[label] = b_lbl

        # ---- 2. Middle Block: Selected Task Inspector ----
        insp_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        insp_card.grid(row=1, column=0, sticky="nsew", pady=(0, 6))
        insp_card.grid_columnconfigure(0, weight=1)
        insp_card.grid_rowconfigure(1, weight=1)

        hdr2 = ctk.CTkFrame(insp_card, height=26, fg_color=BG_ELEVATED, corner_radius=5)
        hdr2.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 4))
        hdr2.grid_propagate(False)
        ctk.CTkLabel(
            hdr2,
            text="ИНСПЕКТОР ВЫБРАННОЙ ЗАДАЧИ",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        self._insp_id_lbl = ctk.CTkLabel(
            hdr2,
            text="не выбрана",
            font=font_mono(9),
            text_color=TEXT_MUTED,
        )
        self._insp_id_lbl.pack(side="right", padx=8)

        self._insp_box = ctk.CTkTextbox(
            insp_card,
            fg_color=BG_DEEP,
            border_width=1,
            border_color=BORDER,
            corner_radius=4,
            text_color=TEXT_PRIMARY,
            font=font_mono(11),
            wrap="word",
        )
        self._insp_box.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0, 5))
        self._insp_box.configure(state="disabled")

        # ---- 3. Bottom Block: Live Event Log ----
        log_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        log_card.grid(row=2, column=0, sticky="nsew")
        log_card.grid_columnconfigure(0, weight=1)
        log_card.grid_rowconfigure(1, weight=1)

        hdr3 = ctk.CTkFrame(log_card, height=26, fg_color=BG_ELEVATED, corner_radius=5)
        hdr3.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 4))
        hdr3.grid_propagate(False)
        ctk.CTkLabel(
            hdr3,
            text="ЖИВОЙ ЖУРНАЛ СОБЫТИЙ",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        self._log_count_lbl = ctk.CTkLabel(
            hdr3,
            text="0 записей",
            font=font_mono(9),
            text_color=TEXT_MUTED,
        )
        self._log_count_lbl.pack(side="right", padx=8)

        self._log_box = ctk.CTkTextbox(
            log_card,
            fg_color=BG_DEEP,
            border_width=1,
            border_color=BORDER,
            corner_radius=4,
            text_color=TEXT_PRIMARY,
            font=font_mono(11),
            wrap="word",
        )
        self._log_box.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0, 5))

        # Configure colored semantic tags on underlying tk.Text widget
        raw_text = self._log_box._textbox
        raw_text.tag_config("TS", foreground=TEXT_MUTED)
        raw_text.tag_config("INFO", foreground=STATUS_RUNNING)
        raw_text.tag_config("OK", foreground=STATUS_COMPLETED)
        raw_text.tag_config("DEDUP", foreground=STATUS_DEDUPLICATED)
        raw_text.tag_config("WARN", foreground=STATUS_QUEUED)
        raw_text.tag_config("ERROR", foreground=STATUS_FAILED)
        raw_text.tag_config("MSG", foreground=TEXT_PRIMARY)
        self._log_box.configure(state="disabled")

    def set_selected_task(self, task_id: int | None) -> None:
        if self._selected_task_id != task_id:
            self._selected_task_id = task_id
            self._inspector_fp = None

    def _contract_keys_for_task(self, task: TaskRun) -> tuple[str, ...]:
        spec = self._spec_map.get(task.process_id)
        if spec is None:
            return ()
        keys: list[str] = []
        for src in spec.write_sources:
            scope = "GLOBAL" if src == "SupplierSLA" else task.target_date
            keys.append(f"WRITE:SOURCE:{src}:{scope}")
        for src in spec.read_sources:
            if src in spec.write_sources:
                continue
            scope = "GLOBAL" if src == "SupplierSLA" else task.target_date
            keys.append(f"READ:SOURCE:{src}:{scope}")
        if not spec.allow_parallel_dates:
            keys.append(f"WRITE:PROCESS:{spec.process_id}:GLOBAL")
        if spec.uses_shared_staging and spec.staging_key:
            keys.append(f"WRITE:STAGING:{spec.staging_key}:GLOBAL")
        return tuple(keys)

    def refresh(self, tasks: list[TaskRun] | None = None) -> None:
        all_tasks = tasks if tasks is not None else self._orchestrator.get_tasks()
        tasks_by_id = {t.task_id: t for t in all_tasks}
        active_locks = self._orchestrator.get_active_locks() or {}
        orch_on = self._orchestrator.is_orchestration_enabled()

        # 1. Update Lock Monitor
        lock_rows = compute_lock_monitor_rows(active_locks, tasks_by_id)
        locks_fp = (orch_on, tuple(lock_rows))
        if locks_fp != self._locks_fp:
            self._locks_fp = locks_fp
            self._locks_mode_badge.configure(
                text="LOCK_MGR: ON" if orch_on else "BYPASSED (AS-IS)",
                text_color=STATUS_COMPLETED if orch_on else STATUS_FAILED,
            )
            for label, mode_key, badge_text in lock_rows:
                bg_c, fg_c = status_badge_colors(mode_key)
                self._lock_badges[label].configure(
                    text=badge_text,
                    fg_color=bg_c,
                    text_color=fg_c,
                )

        # 2. Auto-select latest task if none selected yet
        if self._selected_task_id is not None and self._selected_task_id not in tasks_by_id:
            self._selected_task_id = None
        if self._selected_task_id is None and all_tasks:
            self._selected_task_id = all_tasks[-1].task_id

        selected = tasks_by_id.get(self._selected_task_id) if self._selected_task_id else None
        insp_fp = (
            None
            if selected is None
            else (
                selected.task_id,
                status_key(selected.status),
                selected.progress,
                selected.blocked_by,
                selected.error_message,
                bool(selected.result),
                tuple(sorted(k for k, v in active_locks.items() if selected.task_id in v)),
            )
        )
        if insp_fp != self._inspector_fp:
            self._inspector_fp = insp_fp
            self._insp_id_lbl.configure(
                text=f"#{selected.task_id} {selected.process_id}" if selected else "не выбрана",
                text_color=STATUS_RUNNING if selected else TEXT_MUTED,
            )
            hint = self._contract_keys_for_task(selected) if selected else ()
            text = format_task_inspector_text(selected, active_locks, hint)
            self._insp_box.configure(state="normal")
            self._insp_box.delete("1.0", "end")
            self._insp_box.insert("1.0", text)
            self._insp_box.configure(state="disabled")

        # 3. Update Live Event Log
        events: list[OrchestratorEvent] = self._orchestrator.get_events(limit=80) or []
        ev_fp = (
            len(events),
            events[-1].timestamp if events else None,
            events[-1].message if events else None,
        )
        if ev_fp != self._events_fp:
            self._events_fp = ev_fp
            self._log_count_lbl.configure(text=f"{len(events)} событий")
            raw_text = self._log_box._textbox
            self._log_box.configure(state="normal")
            raw_text.delete("1.0", "end")

            for ev in reversed(events[-60:]):
                ts = ev.timestamp.strftime("%H:%M:%S")
                lvl = (ev.level or "INFO").upper()
                code = getattr(ev, "event_code", "") or ""
                if lvl in ("ERROR", "CRITICAL"):
                    tag = "ERROR"
                elif lvl in ("WARN", "WARNING"):
                    tag = "WARN"
                elif code == "COMPLETED":
                    tag = "OK"
                elif code == "DEDUPLICATED":
                    tag = "DEDUP"
                else:
                    tag = "INFO"

                tid_part = f" #{ev.task_id}" if ev.task_id is not None else ""
                raw_text.insert("end", f"{ts} ", "TS")
                raw_text.insert("end", f"[{lvl[:4]:4s}]{tid_part} ", tag)
                raw_text.insert("end", f"{ev.message}\n", "ERROR" if tag == "ERROR" else "MSG")

            self._log_box.configure(state="disabled")


__all__ = [
    "InspectorAndLogDock",
    "LOCK_MONITOR_ROWS",
    "SOURCE_IDS",
    "compute_lock_monitor_rows",
    "format_task_inspector_text",
]
