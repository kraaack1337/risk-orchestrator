"""Left sidebar Block B: Compact manual process launch form."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from ui.assets.theme import (
    BG_ELEVATED,
    BG_MAIN,
    BG_PANEL,
    BORDER,
    CORNER_RADIUS,
    STATUS_RUNNING,
    TEXT_MUTED,
    TEXT_PRIMARY,
    font_mono,
    font_ui,
)
from ui.contracts import (
    OrchestratorProtocol,
    ProcessSpec,
    TaskRequest,
    TriggerType,
)

DEMO_DATES: list[str] = [
    "2026-11-18",
    "2026-11-19",
    "2026-11-20",
    "2026-11-21",
    "2026-11-22",
]

PRIORITY_LABELS: dict[str, int] = {
    "1 (Высокий)": 1,
    "2 (Обычный)": 2,
    "3 (Фоновый)": 3,
}

AsyncDispatchFn = Callable[[Coroutine[Any, Any, Any]], None]


class ManualLaunchForm:
    """Compact manual task submission form inside the 290px left dock."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        dispatch_async: AsyncDispatchFn,
        get_user_context: Callable[[], tuple[str, str | None]],
        on_force_refresh: Callable[[], None],
    ) -> None:
        import customtkinter as ctk

        self._orchestrator = orchestrator
        self._dispatch_async = dispatch_async
        self._get_user_context = get_user_context
        self._on_force_refresh = on_force_refresh

        self._specs: list[ProcessSpec] = orchestrator.get_process_specs()
        self._spec_map: dict[str, ProcessSpec] = {s.process_id: s for s in self._specs}
        process_ids = [s.process_id for s in self._specs] or [
            "StockoutRisk",
            "DelayRisk",
            "SpoilageRisk",
            "CarrierDefaultRisk",
        ]

        self.frame = ctk.CTkFrame(
            parent,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        self.frame.grid_columnconfigure(0, weight=1)

        # Header
        hdr = ctk.CTkFrame(
            self.frame,
            height=28,
            fg_color=BG_ELEVATED,
            corner_radius=5,
        )
        hdr.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 6))
        hdr.grid_propagate(False)

        ctk.CTkLabel(
            hdr,
            text="РУЧНОЙ ЗАПУСК ПРОЦЕССА",
            font=font_ui(11, "bold"),
            text_color=TEXT_MUTED,
        ).pack(side="left", padx=8)

        body = ctk.CTkFrame(self.frame, fg_color="transparent")
        body.grid(row=1, column=0, sticky="ew", padx=7, pady=(0, 7))
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=1)

        # Process selector
        ctk.CTkLabel(
            body,
            text="Сервис оценки рисков",
            anchor="w",
            font=font_ui(10),
            text_color=TEXT_MUTED,
        ).grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 2))

        self._proc_menu = ctk.CTkOptionMenu(
            body,
            values=process_ids,
            command=self._on_process_change,
            height=28,
            corner_radius=5,
            fg_color=BG_MAIN,
            button_color=BORDER,
            button_hover_color=STATUS_RUNNING,
            dropdown_fg_color=BG_PANEL,
            dropdown_hover_color=BG_ELEVATED,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
            font=font_mono(11),
        )
        self._proc_menu.set(process_ids[0])
        self._proc_menu.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 4))

        # Resource contract inline summary
        self._contract_lbl = ctk.CTkLabel(
            body,
            text="",
            anchor="w",
            justify="left",
            wraplength=296,
            font=font_mono(10),
            text_color=TEXT_MUTED,
            fg_color=BG_MAIN,
            corner_radius=4,
            padx=6,
            pady=4,
        )
        self._contract_lbl.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))

        # Target date & priority side-by-side
        ctk.CTkLabel(
            body,
            text="Расчётная дата",
            anchor="w",
            font=font_ui(10),
            text_color=TEXT_MUTED,
        ).grid(row=3, column=0, sticky="ew", padx=(0, 3), pady=(0, 2))

        ctk.CTkLabel(
            body,
            text="Приоритет",
            anchor="w",
            font=font_ui(10),
            text_color=TEXT_MUTED,
        ).grid(row=3, column=1, sticky="ew", padx=(3, 0), pady=(0, 2))

        self._date_menu = ctk.CTkOptionMenu(
            body,
            values=DEMO_DATES,
            height=28,
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
        self._date_menu.set("2026-11-20")
        self._date_menu.grid(row=4, column=0, sticky="ew", padx=(0, 3), pady=(0, 8))

        prio_keys = list(PRIORITY_LABELS.keys())
        self._prio_menu = ctk.CTkOptionMenu(
            body,
            values=prio_keys,
            height=28,
            corner_radius=5,
            fg_color=BG_MAIN,
            button_color=BORDER,
            button_hover_color=STATUS_RUNNING,
            dropdown_fg_color=BG_PANEL,
            dropdown_hover_color=BG_ELEVATED,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
            font=font_ui(11),
        )
        self._prio_menu.set("2 (Обычный)")
        self._prio_menu.grid(row=4, column=1, sticky="ew", padx=(3, 0), pady=(0, 8))

        # Submit accent button
        ctk.CTkButton(
            body,
            text="Запустить задачу",
            command=self._handle_submit,
            height=30,
            corner_radius=5,
            fg_color=STATUS_RUNNING,
            hover_color="#1F6FEB",
            text_color="#FFFFFF",
            font=font_ui(12, "bold"),
        ).grid(row=5, column=0, columnspan=2, sticky="ew")

        self._update_contract_preview(process_ids[0])

    def _on_process_change(self, process_id: str) -> None:
        self._update_contract_preview(process_id)

    def _update_contract_preview(self, process_id: str) -> None:
        spec = self._spec_map.get(process_id)
        if spec is None:
            self._contract_lbl.configure(text="")
            return
        writes = ", ".join(spec.write_sources) or "—"
        reads = ", ".join(s for s in spec.read_sources if s not in spec.write_sources) or "—"
        par = "PARALLEL_DATES=ON" if spec.allow_parallel_dates else "STAGING_LOCK=ON"
        self._contract_lbl.configure(text=f"EXCL(W): {writes}\nSHRD(R): {reads}\n{par}")

    def _handle_submit(self) -> None:
        pid = self._proc_menu.get()
        t_date = self._date_menu.get()
        prio = PRIORITY_LABELS.get(self._prio_menu.get(), 2)
        user_id, session_id = self._get_user_context()

        req = TaskRequest(
            process_id=pid,
            target_date=t_date,
            trigger=TriggerType.MANUAL,
            priority=prio,
            initiated_by=user_id,
            session_id=session_id,
        )

        async def _send() -> None:
            await self._orchestrator.submit(req)
            self._on_force_refresh()

        self._dispatch_async(_send())
