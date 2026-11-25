"""Tab 2: Conflict Matrix, Resource Contracts & Reglament Scheduler Configuration."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ui.assets.theme import (
    BADGE_BG,
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
)
from ui.contracts import (
    SOURCE_IDS,
    SUPPLIER_SLA,
    OrchestratorProtocol,
    ProcessSpec,
    SchedulerProtocol,
)

INTERVAL_OPTIONS: dict[str, float] = {
    "5 сек": 5.0,
    "10 сек": 10.0,
    "15 сек": 15.0,
    "30 сек": 30.0,
}


def analyze_pair_conflict(a: ProcessSpec, b: ProcessSpec) -> tuple[str, str]:
    """Return (short_label, status_key_for_color) for concurrent execution of A and B."""
    if a.process_id == b.process_id:
        if not a.allow_parallel_dates:
            return "STAGING BLOCK", "QUEUED"
        return "DIFF DATES OK", "COMPLETED"

    aw, ar = set(a.write_sources), set(a.read_sources)
    bw, br = set(b.write_sources), set(b.read_sources)

    ab_rw = aw & br
    ba_rw = bw & ar
    ww = aw & bw

    if ab_rw and ba_rw:
        return "AB-BA DEADLOCK", "FAILED"
    if ww:
        src = sorted(ww)[0]
        return f"EXCL W/W ({src[:9]})", "FAILED"
    rw = ab_rw | ba_rw
    if SUPPLIER_SLA in rw:
        return "GLOBAL SLA R/W", "QUEUED"
    if rw:
        src = sorted(rw)[0]
        return f"DATE R/W ({src[:9]})", "QUEUED"
    return "SHARED PARALLEL", "COMPLETED"


def format_source_cell(spec: ProcessSpec, source_id: str) -> tuple[str, str]:
    """Return (cell_text, semantic_status_key) for (process, source) access rule."""
    rule_map = {r.source_id: r for r in getattr(spec, "resource_rules", ())}
    if source_id in rule_map:
        rule = rule_map[source_id]
        mode = "WRITE" if rule.access_mode.value != "READ" else "READ"
        scope = rule.lock_scope.value
        order = f"#{rule.acquire_order}"
        return f"{mode} · {scope} ({order})", ("FAILED" if mode == "WRITE" else "RUNNING")

    if source_id in spec.write_sources:
        scope = "GLOBAL" if source_id == SUPPLIER_SLA else "BY_DATE"
        return f"WRITE · {scope}", "FAILED"
    if source_id in spec.read_sources:
        scope = "GLOBAL" if source_id == SUPPLIER_SLA else "BY_DATE"
        return f"READ · {scope}", "RUNNING"
    return "—", "CANCELLED"


class RulesAndReglamentView:
    """Tab 2 workspace: declarative access rules table, pairwise matrix, and scheduler controls."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        scheduler: SchedulerProtocol | None,
        on_force_refresh: Callable[[], None],
    ) -> None:
        import customtkinter as ctk

        self._ctk = ctk
        self._orchestrator = orchestrator
        self._scheduler = scheduler
        self._on_force_refresh = on_force_refresh
        self._syncing = False
        self._runs_fp: Any = None

        self.frame = ctk.CTkScrollableFrame(
            parent,
            fg_color="transparent",
            corner_radius=0,
            scrollbar_button_color=BORDER,
            scrollbar_button_hover_color=TEXT_MUTED,
        )
        self.frame.grid_columnconfigure(0, weight=1)

        specs = orchestrator.get_process_specs()

        # ---- Section 1: Reglament Scheduler Controls ----
        sched_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        sched_card.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        sched_card.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            sched_card,
            text="НАСТРОЙКИ АВТОРЕГЛАМЕНТА (REGLAMENT SCHEDULER)",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).grid(row=0, column=0, padx=10, pady=8, sticky="w")

        ctk.CTkLabel(
            sched_card,
            text="Периодический фоновый вброс AUTO-задач (priority=1, scheduler_bot) в общую очередь",
            font=font_ui(10),
            text_color=TEXT_MUTED,
        ).grid(row=0, column=1, padx=6, pady=8, sticky="w")

        ctrl = ctk.CTkFrame(sched_card, fg_color="transparent")
        ctrl.grid(row=0, column=2, padx=10, pady=6, sticky="e")

        ctk.CTkLabel(
            ctrl,
            text="Интервал:",
            font=font_ui(11),
            text_color=TEXT_MUTED,
        ).pack(side="left", padx=(0, 6))

        self._interval_menu = ctk.CTkOptionMenu(
            ctrl,
            values=list(INTERVAL_OPTIONS.keys()),
            command=self._handle_interval_change,
            width=96,
            height=26,
            corner_radius=5,
            fg_color=BG_MAIN,
            button_color=BORDER,
            button_hover_color=STATUS_RUNNING,
            dropdown_fg_color=BG_PANEL,
            dropdown_hover_color=BG_ELEVATED,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
            font=font_mono(10),
        )
        self._interval_menu.set("15 сек")
        self._interval_menu.pack(side="left", padx=(0, 10))

        self._sched_status_lbl = ctk.CTkLabel(
            ctrl,
            text="OFF",
            font=font_mono(10, "bold"),
            fg_color=BG_MAIN,
            text_color=TEXT_MUTED,
            corner_radius=4,
            padx=8,
            pady=3,
        )
        self._sched_status_lbl.pack(side="left")

        if self._scheduler is None:
            self._interval_menu.configure(state="disabled")

        # ---- Section 2: Access Rules Table (4 Processes x 4 Data Marts + Parallelism + TTL) ----
        rules_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        rules_card.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        rules_card.grid_columnconfigure(0, weight=1)

        hdr1 = ctk.CTkFrame(rules_card, height=28, fg_color=BG_ELEVATED, corner_radius=5)
        hdr1.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 6))
        hdr1.grid_propagate(False)
        ctk.CTkLabel(
            hdr1,
            text="ДЕКЛАРАТИВНЫЕ РЕСУРСНЫЕ КОНТРАКТЫ ПРОЦЕССОВ (READ/SHARED · WRITE/EXCLUSIVE · SCOPE · TTL)",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        tbl = ctk.CTkFrame(rules_card, fg_color="transparent")
        tbl.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 8))

        headers = [
            "Процесс",
            *SOURCE_IDS,
            "Паралл. даты",
            "Staging",
            "TTL",
        ]
        for col_i, h_text in enumerate(headers):
            tbl.grid_columnconfigure(col_i, weight=1)
            ctk.CTkLabel(
                tbl,
                text=h_text,
                anchor="w",
                font=font_mono(10, "bold"),
                fg_color=BG_ELEVATED,
                text_color=TEXT_MUTED,
                corner_radius=4,
                padx=6,
                pady=4,
            ).grid(row=0, column=col_i, sticky="ew", padx=2, pady=2)

        for row_i, spec in enumerate(specs, start=1):
            ctk.CTkLabel(
                tbl,
                text=spec.process_id,
                anchor="w",
                font=font_mono(10, "bold"),
                fg_color=BG_MAIN,
                text_color=TEXT_PRIMARY,
                corner_radius=4,
                padx=6,
                pady=5,
            ).grid(row=row_i, column=0, sticky="ew", padx=2, pady=2)

            for s_idx, src in enumerate(SOURCE_IDS, start=1):
                cell_txt, st_key = format_source_cell(spec, src)
                fg = {
                    "FAILED": STATUS_FAILED,
                    "RUNNING": STATUS_RUNNING,
                }.get(st_key, TEXT_MUTED)
                bg = BADGE_BG.get(st_key, BG_MAIN) if cell_txt != "—" else BG_MAIN
                ctk.CTkLabel(
                    tbl,
                    text=cell_txt,
                    anchor="w",
                    font=font_mono(9, "bold" if cell_txt != "—" else "normal"),
                    fg_color=bg,
                    text_color=fg,
                    corner_radius=4,
                    padx=6,
                    pady=5,
                ).grid(row=row_i, column=s_idx, sticky="ew", padx=2, pady=2)

            par_ok = spec.allow_parallel_dates
            ctk.CTkLabel(
                tbl,
                text="ALLOW (True)" if par_ok else "BLOCK (False)",
                anchor="center",
                font=font_mono(9, "bold"),
                fg_color=BADGE_BG["COMPLETED"] if par_ok else BADGE_BG["QUEUED"],
                text_color=STATUS_COMPLETED if par_ok else STATUS_QUEUED,
                corner_radius=4,
                padx=6,
                pady=5,
            ).grid(row=row_i, column=5, sticky="ew", padx=2, pady=2)

            stg_txt = spec.staging_key or "—"
            ctk.CTkLabel(
                tbl,
                text=stg_txt if len(stg_txt) <= 18 else stg_txt[:15] + "...",
                anchor="w",
                font=font_mono(9),
                fg_color=BADGE_BG["DEDUPLICATED"] if spec.staging_key else BG_MAIN,
                text_color=STATUS_DEDUPLICATED if spec.staging_key else TEXT_MUTED,
                corner_radius=4,
                padx=6,
                pady=5,
            ).grid(row=row_i, column=6, sticky="ew", padx=2, pady=2)

            ctk.CTkLabel(
                tbl,
                text=f"{spec.timeout_sec:.0f}s",
                anchor="center",
                font=font_mono(10),
                fg_color=BG_MAIN,
                text_color=TEXT_PRIMARY,
                corner_radius=4,
                padx=6,
                pady=5,
            ).grid(row=row_i, column=7, sticky="ew", padx=2, pady=2)

        # ---- Section 3: Pairwise Conflict Matrix (4 x 4) ----
        pair_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        pair_card.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        pair_card.grid_columnconfigure(0, weight=1)

        hdr2 = ctk.CTkFrame(pair_card, height=28, fg_color=BG_ELEVATED, corner_radius=5)
        hdr2.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 6))
        hdr2.grid_propagate(False)
        ctk.CTkLabel(
            hdr2,
            text="МАТРИЦА ПОПАРНЫХ КОНФЛИКТОВ ПРИ КОНКУРЕНТНОМ ЗАПУСКЕ",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        ptbl = ctk.CTkFrame(pair_card, fg_color="transparent")
        ptbl.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 8))

        ptbl.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            ptbl,
            text="Процесс A \\ Процесс B",
            anchor="w",
            font=font_mono(10, "bold"),
            fg_color=BG_ELEVATED,
            text_color=TEXT_MUTED,
            corner_radius=4,
            padx=6,
            pady=4,
        ).grid(row=0, column=0, sticky="ew", padx=2, pady=2)

        for c_idx, col_spec in enumerate(specs, start=1):
            ptbl.grid_columnconfigure(c_idx, weight=1)
            ctk.CTkLabel(
                ptbl,
                text=col_spec.process_id,
                anchor="center",
                font=font_mono(10, "bold"),
                fg_color=BG_ELEVATED,
                text_color=TEXT_PRIMARY,
                corner_radius=4,
                padx=6,
                pady=4,
            ).grid(row=0, column=c_idx, sticky="ew", padx=2, pady=2)

        for r_idx, row_spec in enumerate(specs, start=1):
            ctk.CTkLabel(
                ptbl,
                text=row_spec.process_id,
                anchor="w",
                font=font_mono(10, "bold"),
                fg_color=BG_MAIN,
                text_color=TEXT_PRIMARY,
                corner_radius=4,
                padx=6,
                pady=5,
            ).grid(row=r_idx, column=0, sticky="ew", padx=2, pady=2)

            for c_idx, col_spec in enumerate(specs, start=1):
                lbl_txt, st_key = analyze_pair_conflict(row_spec, col_spec)
                fg_col = {
                    "FAILED": STATUS_FAILED,
                    "QUEUED": STATUS_QUEUED,
                    "COMPLETED": STATUS_COMPLETED,
                }.get(st_key, TEXT_PRIMARY)
                ctk.CTkLabel(
                    ptbl,
                    text=lbl_txt,
                    anchor="center",
                    font=font_mono(9, "bold"),
                    fg_color=BADGE_BG.get(st_key, BG_MAIN),
                    text_color=fg_col,
                    corner_radius=4,
                    padx=6,
                    pady=5,
                ).grid(row=r_idx, column=c_idx, sticky="ew", padx=2, pady=2)

        # ---- Section 4: Before / After SQLite Runs Summary ----
        self._history_card = ctk.CTkFrame(
            self.frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        self._history_card.grid(row=3, column=0, sticky="ew")
        self._history_card.grid_columnconfigure(0, weight=1)

        hdr3 = ctk.CTkFrame(self._history_card, height=28, fg_color=BG_ELEVATED, corner_radius=5)
        hdr3.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 6))
        hdr3.grid_propagate(False)
        ctk.CTkLabel(
            hdr3,
            text="ЖУРНАЛ ПРОГОНОВ СРАВНЕНИЯ «AS-IS vs TO-BE» (SQLITE VIEW: v_before_after_comparison)",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        self._history_body = ctk.CTkFrame(self._history_card, fg_color="transparent")
        self._history_body.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 8))
        self._history_body.grid_columnconfigure(0, weight=1)

        self.refresh()

    def _handle_interval_change(self, label: str) -> None:
        if self._syncing or self._scheduler is None:
            return
        seconds = INTERVAL_OPTIONS.get(label, 15.0)
        self._scheduler.set_interval(seconds)
        self._on_force_refresh()

    def refresh(self) -> None:
        ctk = self._ctk
        if self._scheduler is not None:
            on = self._scheduler.is_enabled()
            interval = int(getattr(self._scheduler, "get_interval", lambda: 15)())
            self._sched_status_lbl.configure(
                text=f"ACTIVE · {interval}s" if on else f"STANDBY · {interval}s",
                fg_color=BADGE_BG["RUNNING"] if on else BG_MAIN,
                text_color=STATUS_RUNNING if on else TEXT_MUTED,
            )

        storage = getattr(self._orchestrator, "storage", None)
        rows = storage.get_before_after_comparison()[:6] if storage is not None else []
        fp = tuple(
            (
                r.get("run_id"),
                r.get("mode"),
                r.get("total_tasks"),
                r.get("completed_tasks"),
                r.get("failed_tasks"),
            )
            for r in rows
        )
        if fp == self._runs_fp:
            return
        self._runs_fp = fp

        for child in self._history_body.winfo_children():
            child.destroy()

        if not rows:
            ctk.CTkLabel(
                self._history_body,
                text="Запустите сценарии в режимах AS-IS и TO-BE для накопления статистики в SQLite.",
                anchor="w",
                font=font_ui(10),
                text_color=TEXT_MUTED,
            ).grid(row=0, column=0, sticky="w", padx=4, pady=4)
            return

        cols = ("Run", "Режим", "Всего", "OK", "Сбои", "Ошибки %", "DL / LU / STG", "Makespan")
        for idx, name in enumerate(cols):
            self._history_body.grid_columnconfigure(idx, weight=1)
            ctk.CTkLabel(
                self._history_body,
                text=name,
                anchor="w",
                font=font_mono(9, "bold"),
                fg_color=BG_ELEVATED,
                text_color=TEXT_MUTED,
                corner_radius=4,
                padx=6,
                pady=3,
            ).grid(row=0, column=idx, sticky="ew", padx=2, pady=1)

        for r_i, r in enumerate(rows, start=1):
            is_orch = r.get("mode") == "WITH_ORCHESTRATOR"
            mode_lbl = "TO-BE (ON)" if is_orch else "AS-IS (OFF)"
            mode_fg = STATUS_COMPLETED if is_orch else STATUS_FAILED
            vals = [
                f"#{r.get('run_id')}",
                mode_lbl,
                str(r.get("total_tasks", 0)),
                str(r.get("completed_tasks", 0)),
                str(r.get("failed_tasks", 0)),
                f"{r.get('error_rate_pct') or 0.0:.1f}%",
                f"{r.get('deadlock_count', 0)}/{r.get('data_corruption_count', 0)}/{r.get('cross_date_collisions', 0)}",
                f"{r.get('total_makespan_sec') or 0.0:.1f}s",
            ]
            for c_i, val in enumerate(vals):
                ctk.CTkLabel(
                    self._history_body,
                    text=val,
                    anchor="w",
                    font=font_mono(9, "bold" if c_i == 1 else "normal"),
                    fg_color=BG_MAIN,
                    text_color=mode_fg if c_i == 1 else TEXT_PRIMARY,
                    corner_radius=4,
                    padx=6,
                    pady=3,
                ).grid(row=r_i, column=c_i, sticky="ew", padx=2, pady=1)
