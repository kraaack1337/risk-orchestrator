"""CustomTkinter desktop UI widgets for RiskFlow Orchestrator."""

from ui.components.control_panel import (
    CommandToolbar,
    ScenariosDockBlock,
    compact_scenario_labels,
)
from ui.components.dashboard import DashboardPanel, summarize_kpi_metrics
from ui.components.event_log import (
    InspectorAndLogDock,
    compute_lock_monitor_rows,
    format_task_inspector_text,
)
from ui.components.gantt_chart import (
    GanttChartPanel,
    build_gantt_figure,
    normalize_gantt_rows,
)
from ui.components.manual_form import ManualLaunchForm
from ui.components.rules_view import (
    RulesAndReglamentView,
    analyze_pair_conflict,
    format_source_cell,
)

__all__ = [
    "CommandToolbar",
    "DashboardPanel",
    "GanttChartPanel",
    "InspectorAndLogDock",
    "ManualLaunchForm",
    "RulesAndReglamentView",
    "ScenariosDockBlock",
    "analyze_pair_conflict",
    "build_gantt_figure",
    "compact_scenario_labels",
    "compute_lock_monitor_rows",
    "format_source_cell",
    "format_task_inspector_text",
    "normalize_gantt_rows",
    "summarize_kpi_metrics",
]
