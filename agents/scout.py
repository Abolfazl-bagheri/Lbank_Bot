"""
Scout — find symbols to watch this cycle.

Wraps existing get_top_movers logic; does not place trades.
"""

from __future__ import annotations

from typing import Callable


def run_scout(
    get_movers_fn: Callable[[int], list],
    top_n: int = 50,
    always_include: list | None = None,
) -> list[str]:
    """
    Returns unique symbol list for this cycle.
    get_movers_fn: typically bot.get_top_movers
    """
    symbols = list(get_movers_fn(top_n) or [])
    if always_include:
        symbols = list(dict.fromkeys(list(symbols) + list(always_include)))
    return symbols


def tag_sources(
    symbols: list[str],
    gainers: list[str] | None = None,
    volumes: list[str] | None = None,
    forced: list[str] | None = None,
) -> dict[str, str]:
    """Optional map symbol → pump|volume|forced for logging."""
    out: dict[str, str] = {}
    for s in forced or []:
        out[s] = "forced"
    for s in volumes or []:
        out.setdefault(s, "volume")
    for s in gainers or []:
        out.setdefault(s, "pump")
    for s in symbols:
        out.setdefault(s, "other")
    return out
