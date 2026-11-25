"""Strict engineering Dark Mode palette & typography (Temporal / DBeaver / Linear style)."""

from __future__ import annotations

import sys

# Core surfaces
BG_MAIN = "#0F1117"       # Primary window background
BG_PANEL = "#161B22"      # Panels, cards, Gantt canvas
BG_ELEVATED = "#1F2630"   # Headers, toolbar, hover states
BG_DEEP = "#0B0E14"       # Log console & JSON inspector viewport
BORDER = "#30363D"        # 1px subtle border

CORNER_RADIUS = 6
BORDER_WIDTH = 1

# Typography colors
TEXT_PRIMARY = "#E6EDF3"
TEXT_MUTED = "#8B949E"

# Unified semantic status palette (badges, lock monitor, Gantt bars)
STATUS_RUNNING = "#2F81F7"
STATUS_QUEUED = "#D29922"
STATUS_COMPLETED = "#2EA043"
STATUS_FAILED = "#F85149"
STATUS_DEDUPLICATED = "#A371F7"
STATUS_CANCELLED = "#6E7681"

STATUS_COLORS: dict[str, str] = {
    "RUNNING": STATUS_RUNNING,
    "QUEUED": STATUS_QUEUED,
    "COMPLETED": STATUS_COMPLETED,
    "FAILED": STATUS_FAILED,
    "DEADLOCK": STATUS_FAILED,
    "DEDUPLICATED": STATUS_DEDUPLICATED,
    "CANCELLED": STATUS_CANCELLED,
    "FREE": STATUS_COMPLETED,
    "SHARED": STATUS_RUNNING,
    "EXCLUSIVE": STATUS_FAILED,
}

# Tinted dark badge backgrounds matching semantic foregrounds
BADGE_BG: dict[str, str] = {
    "RUNNING": "#132744",
    "QUEUED": "#2D220E",
    "COMPLETED": "#122B1D",
    "FAILED": "#361619",
    "DEADLOCK": "#361619",
    "DEDUPLICATED": "#251A3D",
    "CANCELLED": "#21262D",
    "FREE": "#122B1D",
    "SHARED": "#132744",
    "EXCLUSIVE": "#361619",
}

if sys.platform.startswith("win"):
    _UI_FAMILY = "Segoe UI"
    _MONO_FAMILY = "Consolas"
elif sys.platform == "darwin":
    _UI_FAMILY = "SF Pro Text"
    _MONO_FAMILY = "Menlo"
else:
    _UI_FAMILY = "DejaVu Sans"
    _MONO_FAMILY = "DejaVu Sans Mono"


def font_ui(size: int = 13, weight: str = "normal") -> tuple[str, int, str]:
    """Compact UI font tuple compatible with CustomTkinter widgets."""
    return (_UI_FAMILY, size, weight)


def font_mono(size: int = 12, weight: str = "normal") -> tuple[str, int, str]:
    """Compact monospace font tuple for IDs, timers, lock keys, and logs."""
    return (_MONO_FAMILY, size, weight)


def status_badge_colors(status: str) -> tuple[str, str]:
    """Return (bg_hex, fg_hex) for a status or lock mode badge."""
    key = status.upper()
    return BADGE_BG.get(key, BG_ELEVATED), STATUS_COLORS.get(key, TEXT_PRIMARY)
