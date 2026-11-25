"""Command Toolbar (top 48px bar) and 1-click Scenarios Dock Block (left sidebar Block A)."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from ui.assets.theme import (
    BG_ELEVATED,
    BG_MAIN,
    BG_PANEL,
    BORDER,
    CORNER_RADIUS,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    TEXT_MUTED,
    TEXT_PRIMARY,
    font_mono,
    font_ui,
)
from ui.contracts import (
    OrchestratorProtocol,
    ScenarioSpec,
    SchedulerProtocol,
)

MODE_AS_IS = "БЕЗ ОРКЕСТРАТОРА (AS-IS)"
MODE_TO_BE = "С ОРКЕСТРАТОРОМ (TO-BE)"
USER_IDS: list[str] = ["analyst_1", "analyst_2", "manager_1"]

# Compact engineering titles & conflict summaries for the 5 canonical scenarios
SCENARIO_COMPACT_META: dict[str, tuple[str, str]] = {
    "s1_manual_vs_reglament": (
        "1. Ручной vs Регламент",
        "Lost Update на WarehouseStocks + дедупликация",
    ),
    "s2_abba_deadlock": (
        "2. AB-BA Deadlock",
        "Встречный захват WarehouseStocks ↔ TransitTelemetry",
    ),
    "s3_cross_date_collision": (
        "3. Перекрёстные даты и Global SLA",
        "Коллизия staging-буфера + GLOBAL SupplierSLA",
    ),
    "s4_parallel_safe": (
        "4. Безопасный параллелизм",
        "Независимые партиции дат и чтение SHARED+SHARED",
    ),
    "s5_multiuser_storm": (
        "5. Шторм пользователей — 10 задач",
        "Одновременный вброс регламента и 3 аналитиков",
    ),
}

AsyncDispatchFn = Callable[[Coroutine[Any, Any, Any]], None]


def compact_scenario_labels(scenario: ScenarioSpec, index: int) -> tuple[str, str]:
    """Return concise card title and subtitle for the 290px dock sidebar."""
    if scenario.scenario_id in SCENARIO_COMPACT_META:
        return SCENARIO_COMPACT_META[scenario.scenario_id]
    short_title = scenario.title
    if len(short_title) > 36:
        short_title = f"{index + 1}. {short_title[:33]}..."
    short_desc = scenario.description
    if len(short_desc) > 58:
        short_desc = short_desc[:55] + "..."
    return short_title, short_desc


class CommandToolbar:
    """Top 48px unified command bar (JetBrains Fleet / Docker Desktop style)."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        scheduler: SchedulerProtocol | None,
        dispatch_async: AsyncDispatchFn,
        on_user_changed: Callable[[str], None],
        on_open_new_window: Callable[[], None] | None,
        on_force_refresh: Callable[[], None],
        initial_user: str = "analyst_1",
        is_secondary_window: bool = False,
    ) -> None:
        import customtkinter as ctk

        self._ctk = ctk
        self._orchestrator = orchestrator
        self._scheduler = scheduler
        self._dispatch_async = dispatch_async
        self._on_user_changed = on_user_changed
        self._on_open_new_window = on_open_new_window
        self._on_force_refresh = on_force_refresh
        self._syncing = False

        self.frame = ctk.CTkFrame(
            parent,
            height=52,
            fg_color=BG_ELEVATED,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        self.frame.grid_propagate(False)
        self.frame.grid_columnconfigure(0, weight=0)
        self.frame.grid_columnconfigure(1, weight=1)
        self.frame.grid_columnconfigure(2, weight=0)

        # ---- Left section: Brand badge + AS-IS / TO-BE Segmented Switch ----
        left = ctk.CTkFrame(self.frame, fg_color="transparent")
        left.grid(row=0, column=0, padx=(8, 6), pady=6, sticky="w")

        brand = ctk.CTkFrame(
            left,
            fg_color=BG_MAIN,
            corner_radius=4,
            border_width=1,
            border_color=BORDER,
        )
        brand.pack(side="left", padx=(0, 8))

        dot = ctk.CTkFrame(brand, width=7, height=7, corner_radius=4, fg_color=STATUS_RUNNING)
        dot.pack(side="left", padx=(7, 5), pady=7)

        title_txt = "RISKFLOW // ANALYST" if is_secondary_window else "RISKFLOW // ORCHESTRATOR"
        ctk.CTkLabel(
            brand,
            text=title_txt,
            font=font_mono(12, "bold"),
            text_color=TEXT_PRIMARY,
        ).pack(side="left", padx=(0, 8), pady=5)

        self._mode_seg = ctk.CTkSegmentedButton(
            left,
            values=[MODE_AS_IS, MODE_TO_BE],
            command=self._handle_mode_change,
            height=32,
            corner_radius=5,
            font=font_ui(12, "bold"),
            fg_color=BG_MAIN,
            unselected_color=BG_MAIN,
            unselected_hover_color=BG_PANEL,
            selected_color=STATUS_COMPLETED,
            selected_hover_color=STATUS_COMPLETED,
            text_color=TEXT_PRIMARY,
            border_width=1,
        )
        self._mode_seg.pack(side="left")

        # ---- Center section: Active User selector + New Analyst Window ----
        center = ctk.CTkFrame(self.frame, fg_color="transparent")
        center.grid(row=0, column=1, padx=6, pady=6)

        ctk.CTkLabel(
            center,
            text="Сессия:",
            font=font_ui(11),
            text_color=TEXT_MUTED,
        ).pack(side="left", padx=(0, 5))

        self._user_menu = ctk.CTkOptionMenu(
            center,
            values=USER_IDS,
            command=self._handle_user_select,
            width=118,
            height=28,
            corner_radius=5,
            fg_color=BG_PANEL,
            button_color=BORDER,
            button_hover_color=STATUS_RUNNING,
            dropdown_fg_color=BG_PANEL,
            dropdown_hover_color=BG_ELEVATED,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
            font=font_mono(11),
        )
        self._user_menu.set(initial_user if initial_user in USER_IDS else USER_IDS[0])
        self._user_menu.pack(side="left", padx=(0, 6))

        if on_open_new_window is not None:
            ctk.CTkButton(
                center,
                text="+ Новое окно аналитика",
                command=on_open_new_window,
                height=28,
                width=164,
                corner_radius=5,
                fg_color=BG_PANEL,
                hover_color=BG_MAIN,
                border_width=1,
                border_color=BORDER,
                text_color=TEXT_PRIMARY,
                font=font_ui(11),
            ).pack(side="left")

        # ---- Right section: Auto-reglament switch + Pause/Resume + Reset ----
        right = ctk.CTkFrame(self.frame, fg_color="transparent")
        right.grid(row=0, column=2, padx=(6, 8), pady=6, sticky="e")

        self._reg_switch = ctk.CTkSwitch(
            right,
            text="Авторегламент (15с)",
            command=self._handle_reglament_toggle,
            font=font_ui(11),
            text_color=TEXT_PRIMARY,
            fg_color=BORDER,
            progress_color=STATUS_RUNNING,
            button_color=TEXT_PRIMARY,
            button_hover_color=TEXT_PRIMARY,
            switch_width=34,
            switch_height=18,
        )
        self._reg_switch.pack(side="left", padx=(0, 10))
        if self._scheduler is None:
            self._reg_switch.configure(state="disabled")

        self._pause_btn = ctk.CTkButton(
            right,
            text="Пауза",
            command=self._handle_pause_toggle,
            height=28,
            width=96,
            corner_radius=5,
            fg_color=BG_PANEL,
            hover_color=BG_MAIN,
            border_width=1,
            border_color=BORDER,
            text_color=TEXT_PRIMARY,
            font=font_ui(11),
        )
        self._pause_btn.pack(side="left", padx=(0, 6))

        ctk.CTkButton(
            right,
            text="Сбросить стенд",
            command=self._handle_reset_click,
            height=28,
            width=116,
            corner_radius=5,
            fg_color="#2B1418",
            hover_color="#3D1A20",
            border_width=1,
            border_color=STATUS_FAILED,
            text_color=STATUS_FAILED,
            font=font_ui(11, "bold"),
        ).pack(side="left")

        self.refresh()

    def _handle_mode_change(self, value: str) -> None:
        if self._syncing:
            return
        want_enabled = value == MODE_TO_BE

        async def _switch() -> None:
            try:
                self._orchestrator.set_orchestration_enabled(want_enabled)
            except RuntimeError:
                await self._orchestrator.reset()
                self._orchestrator.set_orchestration_enabled(want_enabled)
            self._on_force_refresh()

        self._dispatch_async(_switch())

    def _handle_user_select(self, user_id: str) -> None:
        self._on_user_changed(user_id)

    def _handle_reglament_toggle(self) -> None:
        if self._syncing or self._scheduler is None:
            return
        enabled = bool(self._reg_switch.get())

        async def _toggle() -> None:
            await self._orchestrator.start()
            self._scheduler.set_enabled(enabled)
            self._on_force_refresh()

        self._dispatch_async(_toggle())

    def _handle_pause_toggle(self) -> None:
        is_paused_fn = getattr(self._orchestrator, "is_paused", None)
        paused = bool(is_paused_fn()) if callable(is_paused_fn) else False
        if paused:
            self._orchestrator.resume()
        else:
            self._orchestrator.pause()
        self._on_force_refresh()

    def _handle_reset_click(self) -> None:
        async def _do_reset() -> None:
            if self._scheduler is not None and self._scheduler.is_enabled():
                self._scheduler.set_enabled(False)
            try:
                await self._orchestrator.reset(clear_storage=True)  # type: ignore[call-arg]
            except TypeError:
                await self._orchestrator.reset()
            self._on_force_refresh()

        self._dispatch_async(_do_reset())

    def refresh(self) -> None:
        self._syncing = True
        try:
            enabled = self._orchestrator.is_orchestration_enabled()
            target_mode = MODE_TO_BE if enabled else MODE_AS_IS
            if self._mode_seg.get() != target_mode:
                self._mode_seg.set(target_mode)

            if enabled:
                self.frame.configure(border_color=BORDER)
                self._mode_seg.configure(
                    selected_color=STATUS_COMPLETED,
                    selected_hover_color=STATUS_COMPLETED,
                )
            else:
                # AS-IS mode highlights toolbar with thin 1px red border
                self.frame.configure(border_color=STATUS_FAILED)
                self._mode_seg.configure(
                    selected_color=STATUS_FAILED,
                    selected_hover_color=STATUS_FAILED,
                )

            if self._scheduler is not None:
                interval = int(getattr(self._scheduler, "get_interval", lambda: 15)())
                self._reg_switch.configure(text=f"Авторегламент ({interval}с)")
                if bool(self._reg_switch.get()) != self._scheduler.is_enabled():
                    if self._scheduler.is_enabled():
                        self._reg_switch.select()
                    else:
                        self._reg_switch.deselect()

            is_paused_fn = getattr(self._orchestrator, "is_paused", None)
            paused = bool(is_paused_fn()) if callable(is_paused_fn) else False
            if paused:
                self._pause_btn.configure(
                    text="Продолжить",
                    border_color=STATUS_QUEUED,
                    text_color=STATUS_QUEUED,
                )
            else:
                self._pause_btn.configure(
                    text="Пауза",
                    border_color=BORDER,
                    text_color=TEXT_PRIMARY,
                )
        finally:
            self._syncing = False


class ScenariosDockBlock:
    """Left sidebar Block A: 5 compact 1-click verification scenario cards."""

    def __init__(
        self,
        parent: Any,
        orchestrator: OrchestratorProtocol,
        scenarios: list[ScenarioSpec],
        dispatch_async: AsyncDispatchFn,
        on_force_refresh: Callable[[], None],
    ) -> None:
        import customtkinter as ctk

        self._orchestrator = orchestrator
        self._scenarios = scenarios
        self._dispatch_async = dispatch_async
        self._on_force_refresh = on_force_refresh

        self.frame = ctk.CTkFrame(
            parent,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
        )
        self.frame.grid_columnconfigure(0, weight=1)

        # Compact block header
        hdr = ctk.CTkFrame(
            self.frame,
            height=30,
            fg_color=BG_ELEVATED,
            corner_radius=5,
        )
        hdr.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 4))
        hdr.grid_propagate(False)

        ctk.CTkLabel(
            hdr,
            text="СЦЕНАРИИ ПРОВЕРКИ (1 КЛИК)",
            font=font_ui(11, "bold"),
            text_color=TEXT_MUTED,
        ).pack(side="left", padx=8)

        cards_wrap = ctk.CTkFrame(self.frame, fg_color="transparent")
        cards_wrap.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0, 5))
        cards_wrap.grid_columnconfigure(0, weight=1)

        for idx, scenario in enumerate(self._scenarios[:5]):
            self._build_scenario_card(ctk, cards_wrap, scenario, idx)

    def _build_scenario_card(
        self,
        ctk: Any,
        parent: Any,
        scenario: ScenarioSpec,
        index: int,
    ) -> None:
        title, subtitle = compact_scenario_labels(scenario, index)
        is_storm = "s5" in scenario.scenario_id or len(scenario.tasks) >= 8

        card = ctk.CTkFrame(
            parent,
            fg_color=BG_MAIN,
            corner_radius=5,
            border_width=1,
            border_color="#3B2E58" if is_storm else BORDER,
            cursor="hand2",
        )
        card.grid(row=index, column=0, sticky="ew", pady=2)
        card.grid_columnconfigure(0, weight=1)

        top_row = ctk.CTkFrame(card, fg_color="transparent")
        top_row.grid(row=0, column=0, sticky="ew", padx=7, pady=(6, 2))
        top_row.grid_columnconfigure(0, weight=1)

        lbl_title = ctk.CTkLabel(
            top_row,
            text=title,
            anchor="w",
            justify="left",
            wraplength=235,
            font=font_ui(12, "bold"),
            text_color=TEXT_PRIMARY,
        )
        lbl_title.grid(row=0, column=0, sticky="ew")

        badge_lbl = ctk.CTkLabel(
            top_row,
            text=f"{len(scenario.tasks)} зад.",
            font=font_mono(10, "bold"),
            text_color=STATUS_RUNNING if not is_storm else "#A371F7",
            fg_color=BG_ELEVATED,
            corner_radius=4,
            padx=6,
            pady=2,
        )
        badge_lbl.grid(row=0, column=1, padx=(6, 0), sticky="ne")

        lbl_sub = ctk.CTkLabel(
            card,
            text=subtitle,
            anchor="w",
            justify="left",
            wraplength=296,
            font=font_ui(11),
            text_color=TEXT_MUTED,
        )
        lbl_sub.grid(row=1, column=0, sticky="ew", padx=7, pady=(0, 6))

        def _on_enter(_e: Any) -> None:
            card.configure(fg_color=BG_ELEVATED, border_color=STATUS_RUNNING)

        def _on_leave(_e: Any) -> None:
            card.configure(
                fg_color=BG_MAIN,
                border_color="#3B2E58" if is_storm else BORDER,
            )

        def _on_click(_e: Any, sc: ScenarioSpec = scenario) -> None:
            self._run_scenario(sc)

        for widget in (card, top_row, lbl_title, badge_lbl, lbl_sub):
            widget.bind("<Enter>", _on_enter)
            widget.bind("<Leave>", _on_leave)
            widget.bind("<Button-1>", _on_click)

    def _run_scenario(self, scenario: ScenarioSpec) -> None:
        async def _submit() -> None:
            await self._orchestrator.submit_batch(list(scenario.tasks))
            self._on_force_refresh()

        self._dispatch_async(_submit())
