"""Center workspace KPI bar (5 metrics, 64px) and 3-column Temporal-style task board (290px)."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import datetime
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
    status_badge_colors,
)
from ui.contracts import (
    OrchestratorProtocol,
    TaskRun,
    extract_error_type,
    status_key,
    trigger_key,
)

AsyncDispatchFn = Callable[[Coroutine[Any, Any, Any]], None]


def summarize_kpi_metrics(tasks: list[TaskRun], raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize orchestrator.get_metrics() into the 5 KPI card values."""
    total = int(raw.get("total", len(tasks)))
    running = int(raw.get("running", sum(status_key(t.status) == "RUNNING" for t in tasks)))
    queued = int(raw.get("queued", sum(status_key(t.status) == "QUEUED" for t in tasks)))
    completed = int(raw.get("completed", sum(status_key(t.status) == "COMPLETED" for t in tasks)))
    failed = int(raw.get("failed", sum(status_key(t.status) == "FAILED" for t in tasks)))
    dedup = int(
        raw.get(
            "deduplicated_count",
            raw.get("deduplicated", sum(status_key(t.status) == "DEDUPLICATED" for t in tasks)),
        )
    )
    deadlocks = int(raw.get("deadlocks", 0))
    corruptions = int(raw.get("corruptions", 0))
    cross_date = int(raw.get("cross_date_collisions", 0))
    max_par = int(raw.get("max_parallelism", running))

    if "integrity_score" in raw and raw["integrity_score"] is not None:
        integrity = float(raw["integrity_score"])
    else:
        fin = completed + failed
        integrity = round(100.0 * completed / fin, 1) if fin > 0 else 100.0

    return {
        "total": total,
        "running": running,
        "queued": queued,
        "completed": completed,
        "failed": failed,
        "deduplicated_count": dedup,
        "deadlocks": deadlocks,
        "corruptions": corruptions,
        "cross_date": cross_date,
        "max_parallelism": max_par,
        "integrity_score": integrity,
    }


def _bind_click_recursive(widget: Any, callback: Callable[[Any], None], skip: set[Any] | None = None) -> None:
    if skip and widget in skip:
        return
    widget.bind("<Button-1>", callback)
    for child in widget.winfo_children():
        _bind_click_recursive(child, callback, skip)


class DashboardPanel:
    """Renders 5 compact KPI tiles (64px) + 3 task columns (290px): RUNNING, QUEUED, DONE/FAILED."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        dispatch_async: AsyncDispatchFn,
        on_select_task: Callable[[int], None],
        on_force_refresh: Callable[[], None],
    ) -> None:
        import customtkinter as ctk

        self._ctk = ctk
        self._orchestrator = orchestrator
        self._dispatch_async = dispatch_async
        self._on_select_task = on_select_task
        self._on_force_refresh = on_force_refresh
        self._selected_task_id: int | None = None

        # Structural fingerprints to avoid unnecessary widget rebuilds
        self._running_ids_fp: tuple[int, ...] = ()
        self._running_widgets: dict[int, dict[str, Any]] = {}
        self._queued_struct_fp: tuple[Any, ...] = ()
        self._queued_widgets: dict[int, dict[str, Any]] = {}
        self._done_struct_fp: tuple[Any, ...] = ()
        self._done_widgets: dict[int, Any] = {}

        self.frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.frame.grid_columnconfigure(0, weight=1)
        self.frame.grid_rowconfigure(0, weight=0)
        self.frame.grid_rowconfigure(1, weight=0)

        # ---- Row 0: 5 KPI Tiles (height 72px) ----
        self._kpi_bar = ctk.CTkFrame(self.frame, height=72, fg_color="transparent")
        self._kpi_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self._kpi_bar.grid_propagate(False)
        for col_idx in range(5):
            self._kpi_bar.grid_columnconfigure(col_idx, weight=1, uniform="kpi")
        self._kpi_bar.grid_rowconfigure(0, weight=1)

        self._kpi_total_val, self._kpi_total_sub = self._make_kpi_tile(
            0, "ВСЕГО ЗАДАЧ", "0", "0 завершено", TEXT_PRIMARY
        )
        self._kpi_active_val, self._kpi_active_sub = self._make_kpi_tile(
            1, "АКТИВНЫ / В ОЧЕРЕДИ", "0 / 0", "макс. параллельно: 0", STATUS_RUNNING
        )
        self._kpi_err_val, self._kpi_err_sub = self._make_kpi_tile(
            2, "DEADLOCK / ОШИБКИ", "0 / 0", "DL:0 · LU:0 · STG:0", STATUS_FAILED
        )
        self._kpi_dedup_val, self._kpi_dedup_sub = self._make_kpi_tile(
            3, "ДЕДУПЛИЦИРОВАНО", "0", "отсечено дублей", STATUS_DEDUPLICATED
        )
        self._kpi_integ_val, self._kpi_integ_sub = self._make_kpi_tile(
            4, "DATA INTEGRITY SCORE", "100%", "row_version консистентен", STATUS_COMPLETED
        )

        # ---- Row 1: 3 Status Columns (height 310px) ----
        self._cols_frame = ctk.CTkFrame(self.frame, height=310, fg_color="transparent")
        self._cols_frame.grid(row=1, column=0, sticky="ew")
        self._cols_frame.grid_propagate(False)
        for col_idx in range(3):
            self._cols_frame.grid_columnconfigure(col_idx, weight=1, uniform="task_col")
        self._cols_frame.grid_rowconfigure(0, weight=1)

        self._run_scroll, self._run_count_lbl = self._make_column(
            0, "В РАБОТЕ (RUNNING)", STATUS_RUNNING, padx=(0, 4)
        )
        self._queue_scroll, self._queue_count_lbl = self._make_column(
            1, "В ОЧЕРЕДИ (QUEUED)", STATUS_QUEUED, padx=4
        )
        self._done_scroll, self._done_count_lbl = self._make_column(
            2, "ЗАВЕРШЕНЫ / СБОИ", STATUS_COMPLETED, padx=(4, 0)
        )

    def _make_kpi_tile(
        self,
        col: int,
        title: str,
        initial_val: str,
        initial_sub: str,
        accent: str,
    ) -> tuple[Any, Any]:
        ctk = self._ctk
        pad_x = (0, 4) if col == 0 else ((4, 0) if col == 4 else 4)
        tile = ctk.CTkFrame(
            self._kpi_bar,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        tile.grid(row=0, column=col, sticky="nsew", padx=pad_x)
        tile.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            tile,
            text=title,
            anchor="w",
            font=font_ui(10, "bold"),
            text_color=TEXT_MUTED,
        ).grid(row=0, column=0, sticky="ew", padx=8, pady=(5, 0))

        val_lbl = ctk.CTkLabel(
            tile,
            text=initial_val,
            anchor="w",
            font=font_mono(18, "bold"),
            text_color=accent,
        )
        val_lbl.grid(row=1, column=0, sticky="ew", padx=8, pady=0)

        sub_lbl = ctk.CTkLabel(
            tile,
            text=initial_sub,
            anchor="w",
            font=font_ui(11),
            text_color=TEXT_MUTED,
        )
        sub_lbl.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 4))
        return val_lbl, sub_lbl

    def _make_column(
        self,
        col: int,
        title: str,
        dot_color: str,
        padx: int | tuple[int, int],
    ) -> tuple[Any, Any]:
        ctk = self._ctk
        box = ctk.CTkFrame(
            self._cols_frame,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        box.grid(row=0, column=col, sticky="nsew", padx=padx)
        box.grid_columnconfigure(0, weight=1)
        box.grid_rowconfigure(1, weight=1)

        hdr = ctk.CTkFrame(box, height=28, fg_color=BG_ELEVATED, corner_radius=5)
        hdr.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        hdr.grid_propagate(False)

        dot = ctk.CTkFrame(hdr, width=7, height=7, corner_radius=4, fg_color=dot_color)
        dot.pack(side="left", padx=(8, 6), pady=10)

        ctk.CTkLabel(
            hdr,
            text=title,
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left")

        cnt_lbl = ctk.CTkLabel(
            hdr,
            text="0",
            font=font_mono(10, "bold"),
            text_color=dot_color,
            fg_color=BG_MAIN,
            corner_radius=4,
            padx=6,
            pady=1,
        )
        cnt_lbl.pack(side="right", padx=6, pady=4)

        scroll = ctk.CTkScrollableFrame(
            box,
            fg_color="transparent",
            corner_radius=0,
            scrollbar_button_color=BORDER,
            scrollbar_button_hover_color=TEXT_MUTED,
        )
        scroll.grid(row=1, column=0, sticky="nsew", padx=2, pady=(0, 3))
        scroll.grid_columnconfigure(0, weight=1)
        return scroll, cnt_lbl

    def set_selected_task(self, task_id: int | None) -> None:
        if self._selected_task_id == task_id:
            return
        self._selected_task_id = task_id
        self._update_selection_borders()

    def _update_selection_borders(self) -> None:
        for tid, refs in self._running_widgets.items():
            refs["card"].configure(
                border_color=STATUS_RUNNING if tid == self._selected_task_id else BORDER
            )
        for tid, refs in self._queued_widgets.items():
            refs["card"].configure(
                border_color=STATUS_RUNNING if tid == self._selected_task_id else BORDER
            )
        for tid, card in self._done_widgets.items():
            card.configure(
                border_color=STATUS_RUNNING if tid == self._selected_task_id else BORDER
            )

    def _select_task(self, task_id: int) -> None:
        self._selected_task_id = task_id
        self._update_selection_borders()
        self._on_select_task(task_id)

    def _cancel_task(self, task_id: int) -> None:
        async def _do_cancel() -> None:
            await self._orchestrator.cancel(task_id)
            self._on_force_refresh()

        self._dispatch_async(_do_cancel())

    def refresh(self, tasks: list[TaskRun] | None = None, metrics: dict[str, Any] | None = None) -> None:
        all_tasks = tasks if tasks is not None else self._orchestrator.get_tasks()
        raw_metrics = metrics if metrics is not None else (self._orchestrator.get_metrics() or {})
        kpi = summarize_kpi_metrics(all_tasks, raw_metrics)
        now = datetime.now()

        # 1. Update 5 KPI tiles in-place
        self._kpi_total_val.configure(text=str(kpi["total"]))
        self._kpi_total_sub.configure(text=f"завершено: {kpi['completed']}")

        self._kpi_active_val.configure(text=f"{kpi['running']} / {kpi['queued']}")
        self._kpi_active_sub.configure(text=f"макс. паралл.: {kpi['max_parallelism']}")

        err_color = STATUS_FAILED if (kpi["deadlocks"] or kpi["failed"]) else TEXT_MUTED
        self._kpi_err_val.configure(
            text=f"{kpi['deadlocks']} / {kpi['failed']}",
            text_color=err_color if kpi["failed"] else STATUS_COMPLETED,
        )
        self._kpi_err_sub.configure(
            text=f"DL:{kpi['deadlocks']} · LU:{kpi['corruptions']} · STG:{kpi['cross_date']}"
        )

        self._kpi_dedup_val.configure(text=str(kpi["deduplicated_count"]))

        integ = kpi["integrity_score"]
        integ_ok = integ >= 99.9
        self._kpi_integ_val.configure(
            text=f"{integ:.0f}%" if integ.is_integer() else f"{integ:.1f}%",
            text_color=STATUS_COMPLETED if integ_ok else STATUS_FAILED,
        )
        self._kpi_integ_sub.configure(
            text="100% консистентность" if integ_ok else "нарушение целостности!",
            text_color=TEXT_MUTED if integ_ok else STATUS_FAILED,
        )

        # Partition tasks by column
        running_tasks = sorted(
            (t for t in all_tasks if status_key(t.status) == "RUNNING"),
            key=lambda t: (t.started_at or t.queued_at, t.task_id),
        )
        queued_tasks = sorted(
            (t for t in all_tasks if status_key(t.status) == "QUEUED"),
            key=lambda t: (
                t.effective_priority if t.effective_priority is not None else float(t.priority),
                t.queued_at,
                t.task_id,
            ),
        )
        done_tasks = sorted(
            (
                t
                for t in all_tasks
                if status_key(t.status) in ("COMPLETED", "FAILED", "DEDUPLICATED", "CANCELLED")
            ),
            key=lambda t: (t.finished_at or t.started_at or t.queued_at, t.task_id),
            reverse=True,
        )

        self._run_count_lbl.configure(text=str(len(running_tasks)))
        self._queue_count_lbl.configure(text=str(len(queued_tasks)))
        self._done_count_lbl.configure(text=str(len(done_tasks)))

        self._sync_running_column(running_tasks, now)
        self._sync_queued_column(queued_tasks, now)
        self._sync_done_column(done_tasks[:35])

    def _sync_running_column(self, tasks: list[TaskRun], now: datetime) -> None:
        ctk = self._ctk
        ids_fp = tuple(t.task_id for t in tasks)
        if ids_fp != self._running_ids_fp:
            self._running_ids_fp = ids_fp
            for child in self._run_scroll.winfo_children():
                child.destroy()
            self._running_widgets.clear()

            if not tasks:
                ctk.CTkLabel(
                    self._run_scroll,
                    text="Нет активных вычислений",
                    font=font_ui(11),
                    text_color=TEXT_MUTED,
                ).grid(row=0, column=0, pady=24)
                return

            for idx, task in enumerate(tasks):
                card = ctk.CTkFrame(
                    self._run_scroll,
                    fg_color=BG_MAIN,
                    corner_radius=5,
                    border_width=1,
                    border_color=STATUS_RUNNING if task.task_id == self._selected_task_id else BORDER,
                    cursor="hand2",
                )
                card.grid(row=idx, column=0, sticky="ew", padx=3, pady=2)
                card.grid_columnconfigure(0, weight=1)

                top = ctk.CTkFrame(card, fg_color="transparent")
                top.grid(row=0, column=0, sticky="ew", padx=6, pady=(5, 2))
                top.grid_columnconfigure(1, weight=1)

                ctk.CTkLabel(
                    top,
                    text=f"#{task.task_id}",
                    font=font_mono(10, "bold"),
                    text_color=STATUS_RUNNING,
                ).grid(row=0, column=0, padx=(0, 5))

                ctk.CTkLabel(
                    top,
                    text=task.process_id,
                    anchor="w",
                    font=font_ui(11, "bold"),
                    text_color=TEXT_PRIMARY,
                ).grid(row=0, column=1, sticky="ew")

                trig = trigger_key(task.trigger)
                init_text = "AUTO" if trig == "AUTO" else task.initiated_by
                init_bg = "#1E1B38" if trig == "AUTO" else BG_ELEVATED
                init_fg = STATUS_DEDUPLICATED if trig == "AUTO" else TEXT_MUTED
                ctk.CTkLabel(
                    top,
                    text=init_text,
                    font=font_mono(9, "bold"),
                    fg_color=init_bg,
                    text_color=init_fg,
                    corner_radius=4,
                    padx=5,
                    pady=1,
                ).grid(row=0, column=2, padx=(4, 0))

                mid = ctk.CTkFrame(card, fg_color="transparent")
                mid.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 3))
                mid.grid_columnconfigure(0, weight=1)

                ctk.CTkLabel(
                    mid,
                    text=f"дата: {task.target_date}",
                    anchor="w",
                    font=font_mono(10),
                    text_color=TEXT_MUTED,
                ).grid(row=0, column=0, sticky="w")

                timer_lbl = ctk.CTkLabel(
                    mid,
                    text="0.0s · 0%",
                    anchor="e",
                    font=font_mono(10, "bold"),
                    text_color=STATUS_RUNNING,
                )
                timer_lbl.grid(row=0, column=1, sticky="e")

                bar = ctk.CTkProgressBar(
                    card,
                    height=6,
                    corner_radius=3,
                    fg_color=BG_ELEVATED,
                    progress_color=STATUS_RUNNING,
                )
                bar.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 6))
                bar.set(max(0.0, min(1.0, task.progress / 100.0)))

                _bind_click_recursive(card, lambda _e, tid=task.task_id: self._select_task(tid))
                self._running_widgets[task.task_id] = {"card": card, "timer": timer_lbl, "bar": bar}

        # Live in-place update of timers & progress bars
        for task in tasks:
            refs = self._running_widgets.get(task.task_id)
            if not refs:
                continue
            elapsed = max(0.0, (now - (task.started_at or now)).total_seconds())
            pct = max(0, min(100, int(task.progress)))
            refs["timer"].configure(text=f"{elapsed:.1f}s · {pct}%")
            refs["bar"].set(pct / 100.0)

    def _sync_queued_column(self, tasks: list[TaskRun], now: datetime) -> None:
        ctk = self._ctk
        struct_fp = tuple((t.task_id, t.blocked_by) for t in tasks)
        if struct_fp != self._queued_struct_fp:
            self._queued_struct_fp = struct_fp
            for child in self._queue_scroll.winfo_children():
                child.destroy()
            self._queued_widgets.clear()

            if not tasks:
                ctk.CTkLabel(
                    self._queue_scroll,
                    text="Очередь ожидания пуста",
                    font=font_ui(11),
                    text_color=TEXT_MUTED,
                ).grid(row=0, column=0, pady=24)
                return

            for idx, task in enumerate(tasks):
                card = ctk.CTkFrame(
                    self._queue_scroll,
                    fg_color=BG_MAIN,
                    corner_radius=5,
                    border_width=1,
                    border_color=STATUS_RUNNING if task.task_id == self._selected_task_id else BORDER,
                    cursor="hand2",
                )
                card.grid(row=idx, column=0, sticky="ew", padx=3, pady=2)
                card.grid_columnconfigure(0, weight=1)

                top = ctk.CTkFrame(card, fg_color="transparent")
                top.grid(row=0, column=0, sticky="ew", padx=6, pady=(4, 1))
                top.grid_columnconfigure(1, weight=1)

                ctk.CTkLabel(
                    top,
                    text=f"#{task.task_id}",
                    font=font_mono(10, "bold"),
                    text_color=STATUS_QUEUED,
                ).grid(row=0, column=0, padx=(0, 5))

                ctk.CTkLabel(
                    top,
                    text=f"{task.process_id} ({task.target_date[-5:]})",
                    anchor="w",
                    font=font_ui(11, "bold"),
                    text_color=TEXT_PRIMARY,
                ).grid(row=0, column=1, sticky="ew")

                peff_lbl = ctk.CTkLabel(
                    top,
                    text=f"Peff={task.effective_priority or task.priority:.0f}",
                    font=font_mono(9, "bold"),
                    fg_color=BG_ELEVATED,
                    text_color=TEXT_MUTED,
                    corner_radius=4,
                    padx=4,
                    pady=1,
                )
                peff_lbl.grid(row=0, column=2, padx=(3, 4))

                cancel_btn = ctk.CTkButton(
                    top,
                    text="×",
                    width=20,
                    height=18,
                    corner_radius=4,
                    fg_color=BG_ELEVATED,
                    hover_color=STATUS_FAILED,
                    text_color=TEXT_PRIMARY,
                    font=font_mono(11, "bold"),
                    command=lambda tid=task.task_id: self._cancel_task(tid),
                )
                cancel_btn.grid(row=0, column=3)

                meta_lbl = ctk.CTkLabel(
                    card,
                    text="",
                    anchor="w",
                    font=font_mono(9),
                    text_color=TEXT_MUTED,
                )
                meta_lbl.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 2))

                reason = task.blocked_by or "Ожидание диспетчера очереди..."
                blk = ctk.CTkLabel(
                    card,
                    text=reason,
                    anchor="w",
                    justify="left",
                    wraplength=235,
                    font=font_mono(9),
                    fg_color=BADGE_BG["QUEUED"],
                    text_color=STATUS_QUEUED,
                    corner_radius=4,
                    padx=6,
                    pady=3,
                )
                blk.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 5))

                _bind_click_recursive(
                    card,
                    lambda _e, tid=task.task_id: self._select_task(tid),
                    skip={cancel_btn},
                )
                self._queued_widgets[task.task_id] = {
                    "card": card,
                    "peff": peff_lbl,
                    "meta": meta_lbl,
                }

        for task in tasks:
            refs = self._queued_widgets.get(task.task_id)
            if not refs:
                continue
            wait_s = max(0.0, (now - task.queued_at).total_seconds())
            peff = task.effective_priority if task.effective_priority is not None else float(task.priority)
            refs["peff"].configure(text=f"Peff={peff:.0f}")
            refs["meta"].configure(text=f"иниц.: {task.initiated_by} · ожидание: {wait_s:.1f}s")

    def _sync_done_column(self, tasks: list[TaskRun]) -> None:
        ctk = self._ctk
        struct_fp = tuple((t.task_id, status_key(t.status), t.error_message) for t in tasks)
        if struct_fp == self._done_struct_fp:
            return
        self._done_struct_fp = struct_fp

        for child in self._done_scroll.winfo_children():
            child.destroy()
        self._done_widgets.clear()

        if not tasks:
            ctk.CTkLabel(
                self._done_scroll,
                text="Пока нет завершённых задач",
                font=font_ui(11),
                text_color=TEXT_MUTED,
            ).grid(row=0, column=0, pady=24)
            return

        for idx, task in enumerate(tasks):
            st = status_key(task.status)
            bg_badge, fg_badge = status_badge_colors(st)

            card = ctk.CTkFrame(
                self._done_scroll,
                fg_color=BG_MAIN,
                corner_radius=5,
                border_width=1,
                border_color=STATUS_RUNNING if task.task_id == self._selected_task_id else BORDER,
                cursor="hand2",
            )
            card.grid(row=idx, column=0, sticky="ew", padx=3, pady=2)
            card.grid_columnconfigure(0, weight=1)

            top = ctk.CTkFrame(card, fg_color="transparent")
            top.grid(row=0, column=0, sticky="ew", padx=6, pady=(4, 1))
            top.grid_columnconfigure(1, weight=1)

            ctk.CTkLabel(
                top,
                text=f"#{task.task_id}",
                font=font_mono(10, "bold"),
                text_color=fg_badge,
            ).grid(row=0, column=0, padx=(0, 5))

            ctk.CTkLabel(
                top,
                text=task.process_id,
                anchor="w",
                font=font_ui(11, "bold"),
                text_color=TEXT_PRIMARY,
            ).grid(row=0, column=1, sticky="ew")

            ctk.CTkLabel(
                top,
                text=st,
                font=font_mono(9, "bold"),
                fg_color=bg_badge,
                text_color=fg_badge,
                corner_radius=4,
                padx=5,
                pady=1,
            ).grid(row=0, column=2, padx=(4, 0))

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
            timing_txt = (
                f"{task.target_date} · дубль отсечён (0.0s)"
                if st == "DEDUPLICATED"
                else f"{task.target_date} · wait {wait_s:.1f}s · exec {exec_s:.1f}s"
            )
            ctk.CTkLabel(
                card,
                text=timing_txt,
                anchor="w",
                font=font_mono(9),
                text_color=TEXT_MUTED,
            ).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 4 if st != "FAILED" else 2))

            if st == "FAILED":
                exc_name = extract_error_type(task)
                short_err = (task.error_message or exc_name).replace(f"{exc_name}: ", "", 1)
                if len(short_err) > 78:
                    short_err = short_err[:75] + "..."
                ctk.CTkLabel(
                    card,
                    text=f"[{exc_name}] {short_err}",
                    anchor="w",
                    justify="left",
                    wraplength=235,
                    font=font_mono(9, "bold"),
                    fg_color=BADGE_BG["FAILED"],
                    text_color=STATUS_FAILED,
                    corner_radius=4,
                    padx=6,
                    pady=3,
                ).grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 5))

            _bind_click_recursive(card, lambda _e, tid=task.task_id: self._select_task(tid))
            self._done_widgets[task.task_id] = card
