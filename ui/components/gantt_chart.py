"""Live matplotlib Gantt chart (FigureCanvasTkAgg) seamlessly styled for #161B22 Dark Mode."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from matplotlib.figure import Figure
from matplotlib.patches import Patch

from ui.assets.theme import (
    BG_ELEVATED,
    BG_PANEL,
    BORDER,
    CORNER_RADIUS,
    STATUS_CANCELLED,
    STATUS_COLORS,
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
from ui.contracts import OrchestratorProtocol, status_key, trigger_key


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value))
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            return None
    return None


def normalize_gantt_rows(orchestrator: OrchestratorProtocol) -> list[dict[str, Any]]:
    """Read get_gantt_data() with fallback to get_tasks()."""
    raw_rows = orchestrator.get_gantt_data() or []
    normalized: list[dict[str, Any]] = []

    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        task_id = row.get("task_id") or row.get("id")
        process_id = row.get("process_id") or row.get("process")
        queued_at = _parse_dt(row.get("queued_at"))
        started_at = _parse_dt(row.get("started_at") or row.get("start"))
        finished_at = _parse_dt(row.get("finished_at") or row.get("finish"))
        if task_id is None or process_id is None or (queued_at is None and started_at is None):
            continue
        normalized.append(
            {
                "task_id": int(task_id),
                "process_id": str(process_id),
                "target_date": str(row.get("target_date") or ""),
                "trigger": trigger_key(row.get("trigger") or row.get("trigger_type") or "MANUAL"),
                "priority": int(row.get("priority", 2) or 2),
                "status": status_key(row.get("status", "RUNNING")),
                "progress": int(row.get("progress", 0) or 0),
                "queued_at": queued_at or started_at,
                "started_at": started_at,
                "finished_at": finished_at,
            }
        )

    if normalized:
        normalized.sort(key=lambda r: r["task_id"])
        return normalized

    tasks = sorted(orchestrator.get_tasks(), key=lambda t: t.task_id)
    for t in tasks:
        normalized.append(
            {
                "task_id": t.task_id,
                "process_id": t.process_id,
                "target_date": t.target_date,
                "trigger": trigger_key(t.trigger),
                "priority": t.priority,
                "status": status_key(t.status),
                "progress": int(t.progress),
                "queued_at": t.queued_at,
                "started_at": t.started_at,
                "finished_at": t.finished_at,
            }
        )
    return normalized


def build_gantt_figure(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    selected_task_id: int | None = None,
    fig: Figure | None = None,
) -> tuple[Figure, list[int]]:
    """Render composite horizontal Gantt bars onto a matplotlib Figure (#161B22 dark surface).

    Returns (figure, visible_task_ids_top_to_bottom).
    """
    now = now or datetime.now()
    if fig is None:
        fig = Figure(figsize=(8.0, 2.8), dpi=100, facecolor=BG_PANEL)
    else:
        fig.clear()
        fig.patch.set_facecolor(BG_PANEL)

    ax = fig.add_subplot(111)
    ax.set_facecolor(BG_PANEL)

    visible = rows[-14:]
    if not visible:
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.text(
            0.5,
            0.5,
            "Таймлайн пуст — запустите сценарий проверки или ручную задачу",
            ha="center",
            va="center",
            color=TEXT_MUTED,
            fontsize=9.5,
            transform=ax.transAxes,
        )
        fig.subplots_adjust(left=0.02, right=0.98, top=0.92, bottom=0.08)
        return fig, []

    t0 = min(r["queued_at"] for r in visible)
    y_positions = list(range(len(visible)))
    y_labels: list[str] = []
    visible_ids: list[int] = []
    max_end = 1.0

    for idx, r in enumerate(visible):
        tid = int(r["task_id"])
        visible_ids.append(tid)
        st = status_key(r["status"])
        short_date = str(r["target_date"])[-5:]
        prefix = "▸ " if tid == selected_task_id else ""
        y_labels.append(f"{prefix}#{tid} {r['process_id']} ({short_date})")

        q_rel = max(0.0, (r["queued_at"] - t0).total_seconds())
        s_dt: datetime | None = r.get("started_at")
        f_dt: datetime | None = r.get("finished_at")

        if st == "DEDUPLICATED":
            # Instant deduplicated marker bar
            ax.barh(
                idx,
                0.12,
                left=q_rel,
                height=0.56,
                color=STATUS_DEDUPLICATED,
                edgecolor=STATUS_DEDUPLICATED,
                linewidth=0.9,
            )
            ax.text(
                q_rel + 0.16,
                idx,
                "DEDUP (0.0s)",
                va="center",
                ha="left",
                color=STATUS_DEDUPLICATED,
                fontsize=7.5,
                family="monospace",
            )
            max_end = max(max_end, q_rel + 0.9)
            continue

        s_rel = max(q_rel, ((s_dt or now) - t0).total_seconds())
        wait_dur = max(0.0, s_rel - q_rel)

        # 1. Pale amber wait segment (queued_at -> started_at)
        if wait_dur >= 0.03 or st == "QUEUED":
            draw_wait = max(wait_dur, 0.08)
            ax.barh(
                idx,
                draw_wait,
                left=q_rel,
                height=0.54,
                color=STATUS_QUEUED,
                alpha=0.34,
                edgecolor=STATUS_QUEUED,
                linewidth=0.9,
            )
            if st == "QUEUED":
                ax.text(
                    q_rel + draw_wait + 0.08,
                    idx,
                    f"wait {wait_dur:.1f}s",
                    va="center",
                    ha="left",
                    color=STATUS_QUEUED,
                    fontsize=7.5,
                    family="monospace",
                )
            max_end = max(max_end, q_rel + draw_wait + 0.6)

        # 2. Active execution segment (started_at -> finished_at / now)
        if s_dt is not None:
            f_rel = max(s_rel, ((f_dt or now) - t0).total_seconds())
            exec_dur = max(0.08, f_rel - s_rel)
            bar_color = STATUS_COLORS.get(st, STATUS_RUNNING)
            edge_color = "#FFFFFF" if tid == selected_task_id else bar_color

            ax.barh(
                idx,
                exec_dur,
                left=s_rel,
                height=0.58,
                color=bar_color,
                alpha=0.92,
                edgecolor=edge_color,
                linewidth=1.2 if tid == selected_task_id else 0.7,
            )

            dur_label = (
                f"{exec_dur:.1f}s"
                if wait_dur < 0.15
                else f"{exec_dur:.1f}s (w:{wait_dur:.1f}s)"
            )
            if exec_dur >= max_end * 0.18 and exec_dur >= 0.9:
                ax.text(
                    s_rel + exec_dur / 2.0,
                    idx,
                    f"{exec_dur:.1f}s",
                    va="center",
                    ha="center",
                    color="#FFFFFF",
                    fontsize=7.5,
                    weight="bold",
                    family="monospace",
                )
            else:
                ax.text(
                    s_rel + exec_dur + 0.08,
                    idx,
                    dur_label,
                    va="center",
                    ha="left",
                    color=TEXT_PRIMARY if st != "FAILED" else STATUS_FAILED,
                    fontsize=7.5,
                    family="monospace",
                )
            max_end = max(max_end, s_rel + exec_dur + 1.1)

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=9.0, color=TEXT_PRIMARY, family="monospace")
    ax.invert_yaxis()
    ax.set_xlim(0.0, max(4.0, max_end * 1.06))
    ax.set_xlabel("Время от начала волны (сек)", fontsize=9.0, color=TEXT_MUTED, labelpad=4)
    ax.tick_params(axis="x", colors=TEXT_MUTED, labelsize=8.5)
    ax.tick_params(axis="y", length=0, pad=5)
    ax.grid(axis="x", color=BORDER, linestyle="--", linewidth=0.6, alpha=0.75)
    ax.set_axisbelow(True)

    for name, spine in ax.spines.items():
        if name in ("top", "right"):
            spine.set_visible(False)
        else:
            spine.set_color(BORDER)
            spine.set_linewidth(0.8)

    legend_handles = [
        Patch(facecolor=STATUS_QUEUED, edgecolor=STATUS_QUEUED, alpha=0.45, label="Ожидание"),
        Patch(facecolor=STATUS_RUNNING, label="RUNNING"),
        Patch(facecolor=STATUS_COMPLETED, label="COMPLETED"),
        Patch(facecolor=STATUS_FAILED, label="FAILED / DEADLOCK"),
        Patch(facecolor=STATUS_DEDUPLICATED, label="DEDUP"),
        Patch(facecolor=STATUS_CANCELLED, label="CANCELLED"),
    ]
    leg = ax.legend(
        handles=legend_handles,
        loc="upper right",
        ncol=6,
        frameon=True,
        facecolor=BG_ELEVATED,
        edgecolor=BORDER,
        fontsize=7.0,
        handlelength=1.0,
        handletextpad=0.4,
        columnspacing=0.9,
    )
    for txt in leg.get_texts():
        txt.set_color(TEXT_MUTED)

    fig.subplots_adjust(left=0.25, right=0.985, top=0.93, bottom=0.17)
    return fig, visible_ids


class GanttChartPanel:
    """Bottom section of Dispatcher tab: seamless matplotlib FigureCanvasTkAgg Gantt chart."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        on_select_task: Callable[[int], None] | None = None,
    ) -> None:
        import customtkinter as ctk
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

        self._orchestrator = orchestrator
        self._on_select_task = on_select_task
        self._selected_task_id: int | None = None
        self._visible_ids: list[int] = []
        self._last_fp: Any = None
        self._last_draw_mono: float = 0.0

        self.frame = ctk.CTkFrame(
            parent,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        self.frame.grid_columnconfigure(0, weight=1)
        self.frame.grid_rowconfigure(1, weight=1)

        hdr = ctk.CTkFrame(self.frame, height=26, fg_color=BG_ELEVATED, corner_radius=5)
        hdr.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        hdr.grid_propagate(False)

        ctk.CTkLabel(
            hdr,
            text="ТАЙМЛАЙН ВЫПОЛНЕНИЯ (GANTT · ОЖИДАНИЕ VS ВЫПОЛНЕНИЕ)",
            font=font_ui(10, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=8)

        self._summary_lbl = ctk.CTkLabel(
            hdr,
            text="0 задач",
            font=font_mono(9),
            text_color=TEXT_MUTED,
        )
        self._summary_lbl.pack(side="right", padx=8)

        self._fig = Figure(figsize=(8.0, 2.7), dpi=100, facecolor=BG_PANEL)
        self._canvas = FigureCanvasTkAgg(self._fig, master=self.frame)
        canvas_widget = self._canvas.get_tk_widget()
        canvas_widget.configure(bg=BG_PANEL, highlightthickness=0, bd=0)
        canvas_widget.grid(row=1, column=0, sticky="nsew", padx=4, pady=(0, 4))

        self._canvas.mpl_connect("button_press_event", self._handle_chart_click)
        self.refresh(force=True)

    def set_selected_task(self, task_id: int | None) -> None:
        if self._selected_task_id != task_id:
            self._selected_task_id = task_id
            self.refresh(force=True)

    def _handle_chart_click(self, event: Any) -> None:
        if self._on_select_task is None or event.ydata is None or not self._visible_ids:
            return
        idx = int(round(float(event.ydata)))
        if 0 <= idx < len(self._visible_ids):
            self._on_select_task(self._visible_ids[idx])

    def refresh(self, *, force: bool = False) -> None:
        mono = time.monotonic()
        if not force and (mono - self._last_draw_mono) < 0.30:
            return

        rows = normalize_gantt_rows(self._orchestrator)
        has_active = any(r["status"] in ("RUNNING", "QUEUED") for r in rows)
        now = datetime.now()
        active_bucket = int(mono / 0.30) if has_active else 0

        fp = (
            self._selected_task_id,
            active_bucket,
            tuple(
                (r["task_id"], r["status"], r["progress"], r["started_at"], r["finished_at"])
                for r in rows[-14:]
            ),
        )
        if not force and fp == self._last_fp:
            return

        self._last_fp = fp
        self._last_draw_mono = mono
        self._summary_lbl.configure(
            text=f"показано {min(len(rows), 14)} из {len(rows)} задач"
        )
        _, self._visible_ids = build_gantt_figure(
            rows,
            now=now,
            selected_task_id=self._selected_task_id,
            fig=self._fig,
        )
        self._canvas.draw_idle()
