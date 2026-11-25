"""Native Desktop Application 'RiskFlow Orchestrator' (CustomTkinter + matplotlib FigureCanvasTkAgg).

Designed after real engineering consoles (Temporal UI, DBeaver Session Manager, Linear).
Supports standalone execution (`python ui/app.py` -> MockOrchestrator) as well as full
integration with Core Orchestrator + SQLite WAL via `main.py`.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    _ROOT = Path(__file__).resolve().parent.parent
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))

from ui.assets.theme import (
    BG_ELEVATED,
    BG_MAIN,
    BG_PANEL,
    BORDER,
    CORNER_RADIUS,
    STATUS_RUNNING,
    TEXT_MUTED,
    TEXT_PRIMARY,
    font_ui,
)
from ui.components import (
    CommandToolbar,
    DashboardPanel,
    GanttChartPanel,
    InspectorAndLogDock,
    ManualLaunchForm,
    RulesAndReglamentView,
    ScenariosDockBlock,
)
from ui.contracts import (
    OrchestratorProtocol,
    ScenarioSpec,
    SchedulerProtocol,
    status_key,
)

TAB_DISPATCHER = "Диспетчер и Таймлайн"
TAB_MATRIX = "Матрица конфликтов и Регламент"


def resolve_demo_scenarios(scenarios: list[ScenarioSpec] | None = None) -> list[ScenarioSpec]:
    if scenarios is not None:
        return list(scenarios)
    from services.scenarios import get_demo_scenarios

    return list(get_demo_scenarios())


class _WorkspaceController:
    """Reusable 4-zone workspace mounted inside the main CTk window or secondary CTkToplevel."""

    def __init__(
        self,
        window: Any,
        orchestrator: OrchestratorProtocol,
        scheduler: SchedulerProtocol | None,
        scenarios: list[ScenarioSpec],
        loop: asyncio.AbstractEventLoop,
        *,
        initial_user: str = "analyst_1",
        on_open_new_window: Any = None,
        is_secondary: bool = False,
        on_close_callback: Any = None,
    ) -> None:
        import customtkinter as ctk

        self.window = window
        self._ctk = ctk
        self._orchestrator = orchestrator
        self._scheduler = scheduler
        self._scenarios = scenarios
        self._loop = loop
        self._user_id = initial_user
        self._session_id: str | None = None
        self._dirty = True
        self._closed = False
        self._after_id: str | None = None
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._on_close_callback = on_close_callback

        self._register_db_session()
        self._orchestrator.subscribe(self.mark_dirty)

        window.configure(fg_color=BG_MAIN)
        window.grid_columnconfigure(0, weight=1)
        window.grid_rowconfigure(0, weight=0)
        window.grid_rowconfigure(1, weight=1)

        # ---- Zone 1: Top Unified Command Toolbar (48px) ----
        self._toolbar = CommandToolbar(
            parent=window,
            orchestrator=orchestrator,
            scheduler=scheduler,
            dispatch_async=self.dispatch_async,
            on_user_changed=self._handle_user_changed,
            on_open_new_window=on_open_new_window,
            on_force_refresh=self.force_refresh,
            initial_user=initial_user,
            is_secondary_window=is_secondary,
        )
        self._toolbar.frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 6))

        # ---- Main 3-Column Body Container ----
        body = ctk.CTkFrame(window, fg_color="transparent")
        body.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        body.grid_columnconfigure(0, weight=0, minsize=330)
        body.grid_columnconfigure(1, weight=1)
        body.grid_columnconfigure(2, weight=0, minsize=370)
        body.grid_rowconfigure(0, weight=1)

        # ---- Zone 2: Left Dock Sidebar (330px: Scenarios + Manual Launch) ----
        left_dock = ctk.CTkFrame(body, width=330, fg_color="transparent")
        left_dock.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        left_dock.grid_propagate(False)
        left_dock.grid_columnconfigure(0, weight=1)
        left_dock.grid_rowconfigure(0, weight=0)
        left_dock.grid_rowconfigure(1, weight=1)

        self._scenarios_block = ScenariosDockBlock(
            parent=left_dock,
            orchestrator=orchestrator,
            scenarios=scenarios,
            dispatch_async=self.dispatch_async,
            on_force_refresh=self.force_refresh,
        )
        self._scenarios_block.frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        self._manual_form = ManualLaunchForm(
            parent=left_dock,
            orchestrator=orchestrator,
            dispatch_async=self.dispatch_async,
            get_user_context=self.get_user_context,
            on_force_refresh=self.force_refresh,
        )
        self._manual_form.frame.grid(row=1, column=0, sticky="new")

        # ---- Zone 3: Center Workspace (CTkTabview with 2 tabs) ----
        self._tabs = ctk.CTkTabview(
            body,
            fg_color=BG_PANEL,
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=BORDER,
            segmented_button_fg_color=BG_MAIN,
            segmented_button_selected_color=STATUS_RUNNING,
            segmented_button_selected_hover_color="#1F6FEB",
            segmented_button_unselected_color=BG_ELEVATED,
            segmented_button_unselected_hover_color=BG_PANEL,
            text_color=TEXT_PRIMARY,
            text_color_disabled=TEXT_MUTED,
        )
        self._tabs.grid(row=0, column=1, sticky="nsew", padx=0)
        self._tabs._segmented_button.configure(font=font_ui(11, "bold"), height=28)

        tab_disp = self._tabs.add(TAB_DISPATCHER)
        tab_rules = self._tabs.add(TAB_MATRIX)

        tab_disp.grid_columnconfigure(0, weight=1)
        tab_disp.grid_rowconfigure(0, weight=0)
        tab_disp.grid_rowconfigure(1, weight=1)

        self._dashboard = DashboardPanel(
            parent=tab_disp,
            orchestrator=orchestrator,
            dispatch_async=self.dispatch_async,
            on_select_task=self._handle_select_task,
            on_force_refresh=self.force_refresh,
        )
        self._dashboard.frame.grid(row=0, column=0, sticky="ew", padx=2, pady=(2, 6))

        self._gantt = GanttChartPanel(
            parent=tab_disp,
            orchestrator=orchestrator,
            on_select_task=self._handle_select_task,
        )
        self._gantt.frame.grid(row=1, column=0, sticky="nsew", padx=2, pady=(0, 2))

        tab_rules.grid_columnconfigure(0, weight=1)
        tab_rules.grid_rowconfigure(0, weight=1)
        self._rules_view = RulesAndReglamentView(
            parent=tab_rules,
            orchestrator=orchestrator,
            scheduler=scheduler,
            on_force_refresh=self.force_refresh,
        )
        self._rules_view.frame.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)

        # ---- Zone 4: Right Inspector & Logs Dock (340px) ----
        self._inspector_dock = InspectorAndLogDock(
            parent=body,
            orchestrator=orchestrator,
        )
        self._inspector_dock.frame.grid(row=0, column=2, sticky="nsew", padx=(6, 0))

        window.protocol("WM_DELETE_WINDOW", self.close)
        self.force_refresh()
        self._schedule_next_poll()

    def _register_db_session(self) -> None:
        storage = getattr(self._orchestrator, "storage", None)
        if storage is None:
            return
        sid = f"desk-{uuid.uuid4().hex[:10]}"
        try:
            storage.create_session(
                session_id=sid,
                user_id=self._user_id,
                client_id="customtkinter",
            )
            self._session_id = sid
        except Exception:
            self._session_id = None

    def get_user_context(self) -> tuple[str, str | None]:
        return self._user_id, self._session_id

    def _handle_user_changed(self, user_id: str) -> None:
        self._user_id = user_id
        storage = getattr(self._orchestrator, "storage", None)
        if storage is not None and self._session_id is not None:
            try:
                storage.update_session(self._session_id, user_id=user_id)
            except Exception:
                pass

    def _handle_select_task(self, task_id: int) -> None:
        self._dashboard.set_selected_task(task_id)
        self._gantt.set_selected_task(task_id)
        self._inspector_dock.set_selected_task(task_id)
        self._inspector_dock.refresh()

    def dispatch_async(self, coro: Coroutine[Any, Any, Any]) -> None:
        task = self._loop.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def mark_dirty(self) -> None:
        self._dirty = True

    def force_refresh(self) -> None:
        if self._closed:
            return
        self._dirty = False
        tasks = self._orchestrator.get_tasks()
        metrics = self._orchestrator.get_metrics() or {}
        self._toolbar.refresh()
        self._dashboard.refresh(tasks=tasks, metrics=metrics)
        self._gantt.refresh(force=False)
        self._inspector_dock.refresh(tasks=tasks)
        if self._tabs.get() == TAB_MATRIX:
            self._rules_view.refresh()

    def _schedule_next_poll(self) -> None:
        if not self._closed:
            self._after_id = self.window.after(120, self._refresh_ui)

    def _refresh_ui(self) -> None:
        if self._closed:
            return
        try:
            tasks = self._orchestrator.get_tasks()
            has_active = any(status_key(t.status) in ("RUNNING", "QUEUED") for t in tasks)
            if self._dirty or has_active:
                self._dirty = False
                metrics = self._orchestrator.get_metrics() or {}
                self._toolbar.refresh()
                self._dashboard.refresh(tasks=tasks, metrics=metrics)
                self._gantt.refresh(force=False)
                self._inspector_dock.refresh(tasks=tasks)
                if self._tabs.get() == TAB_MATRIX:
                    self._rules_view.refresh()
        finally:
            self._schedule_next_poll()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        unsub = getattr(self._orchestrator, "unsubscribe", None)
        if callable(unsub):
            try:
                unsub(self.mark_dirty)
            except Exception:
                pass
        if self._after_id is not None:
            try:
                self.window.after_cancel(self._after_id)
            except Exception:
                pass
        storage = getattr(self._orchestrator, "storage", None)
        if storage is not None and self._session_id is not None:
            try:
                storage.mark_session_disconnected(self._session_id)
            except Exception:
                pass
        if self._on_close_callback is not None:
            self._on_close_callback(self)
        try:
            self.window.destroy()
        except Exception:
            pass


class RiskFlowApp:
    """Top-level native desktop application managing root CTk + secondary CTkToplevel windows."""

    def __init__(
        self,
        orchestrator: OrchestratorProtocol | None = None,
        scheduler: SchedulerProtocol | None = None,
        scenarios: list[ScenarioSpec] | None = None,
        *,
        title: str = "RiskFlow Orchestrator — Система оркестрации оценки рисков",
    ) -> None:
        if orchestrator is None:
            from services.mocks import MockOrchestrator, MockReglamentScheduler

            mock_orch = MockOrchestrator()
            orchestrator = mock_orch
            if scheduler is None:
                scheduler = MockReglamentScheduler(mock_orch, interval=15.0)
            self._is_standalone_mock = True
        else:
            self._is_standalone_mock = False

        self.orchestrator = orchestrator
        self.scheduler = scheduler
        self.scenarios = resolve_demo_scenarios(scenarios)
        self.title = title
        self._secondary_controllers: list[_WorkspaceController] = []
        self._window_counter = 1

    def _open_analyst_window(self, root: Any, loop: asyncio.AbstractEventLoop) -> None:
        import customtkinter as ctk

        self._window_counter += 1
        default_users = ["analyst_2", "manager_1", "analyst_1"]
        chosen_user = default_users[(self._window_counter - 2) % len(default_users)]

        top = ctk.CTkToplevel(root)
        top.title(f"RiskFlow Orchestrator — Окно аналитика #{self._window_counter} ({chosen_user})")
        top.geometry("1380x860")
        top.minsize(1200, 760)

        ctrl = _WorkspaceController(
            window=top,
            orchestrator=self.orchestrator,
            scheduler=self.scheduler,
            scenarios=self.scenarios,
            loop=loop,
            initial_user=chosen_user,
            on_open_new_window=lambda: self._open_analyst_window(root, loop),
            is_secondary=True,
            on_close_callback=lambda c: self._secondary_controllers.remove(c)
            if c in self._secondary_controllers
            else None,
        )
        self._secondary_controllers.append(ctrl)

    def run(self) -> None:
        try:
            import customtkinter as ctk
        except ImportError as exc:
            if "libtk" in str(exc) or "_tkinter" in str(exc):
                raise SystemExit(
                    "Не найдена системная библиотека Tcl/Tk (libtk8.6.so) для запуска нативного окна.\n"
                    "Установите пакет tk:\n"
                    "  • Arch / CachyOS / Manjaro : sudo pacman -S tk\n"
                    "  • Ubuntu / Debian          : sudo apt install python3-tk\n"
                    "  • Fedora                   : sudo dnf install python3-tkinter"
                ) from exc
            raise

        ctk.set_appearance_mode("dark")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        root = ctk.CTk()
        if root.winfo_screenwidth() >= 2200:
            ctk.set_widget_scaling(1.15)
        title_suffix = " [Autonomous Mock Engine]" if self._is_standalone_mock else ""
        root.title(f"{self.title}{title_suffix}")
        root.geometry("1600x980")
        root.minsize(1360, 840)

        loop.run_until_complete(self.orchestrator.start())

        main_ctrl = _WorkspaceController(
            window=root,
            orchestrator=self.orchestrator,
            scheduler=self.scheduler,
            scenarios=self.scenarios,
            loop=loop,
            initial_user="analyst_1",
            on_open_new_window=lambda: self._open_analyst_window(root, loop),
            is_secondary=False,
        )

        def _on_root_close() -> None:
            for sec in list(self._secondary_controllers):
                sec.close()
            main_ctrl.close()

        root.protocol("WM_DELETE_WINDOW", _on_root_close)

        try:
            from async_tkinter_loop import async_mainloop

            async_mainloop(root, event_loop=loop)
        finally:
            async def _shutdown() -> None:
                if self.scheduler is not None:
                    self.scheduler.set_enabled(False)
                    stop_fn = getattr(self.scheduler, "stop", None)
                    if callable(stop_fn):
                        res = stop_fn()
                        if hasattr(res, "__await__"):
                            await res
                await self.orchestrator.stop()

            if not loop.is_closed():
                loop.run_until_complete(_shutdown())
                loop.close()


def run_ui(
    orchestrator: OrchestratorProtocol | None = None,
    scheduler: SchedulerProtocol | None = None,
    scenarios: list[ScenarioSpec] | None = None,
    *,
    title: str = "RiskFlow Orchestrator — Система оркестрации оценки рисков",
) -> None:
    """Entry point used by main.py and standalone execution."""
    app = RiskFlowApp(
        orchestrator=orchestrator,
        scheduler=scheduler,
        scenarios=scenarios,
        title=title,
    )
    app.run()


if __name__ == "__main__":
    run_ui()
