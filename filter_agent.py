"""
Filter — hard gates before analysis.

Cheap checks first. Reject with explicit reasons.
Does not compute gap quality; that is Analyst.
"""

from __future__ import annotations

from typing import Any, Callable

from .context import AgentDecision, SignalContext


def run_filter(
    ctx: SignalContext,
    *,
    is_blacklisted: Callable[[str], bool] | None = None,
    is_inno: Callable[[str], bool] | None = None,
    is_news_window: Callable[[], bool] | None = None,
    max_drift_pct: float = 8.0,
    min_volume: float = 0.0,
    skip_erratic: bool = True,
    is_erratic_row: Callable[[Any], bool] | None = None,
) -> AgentDecision:
    d = AgentDecision(ok=True, agent="filter")

    if is_blacklisted and is_blacklisted(ctx.symbol):
        return d.reject("blacklisted")

    if is_inno and is_inno(ctx.symbol):
        # soft flag in meta; hard reject optional
        d.meta["inno"] = True
        # uncomment to hard-block: return d.reject("inno")

    if is_news_window and is_news_window():
        return d.reject("news_window")

    if ctx.df is None or len(ctx.df) < 22:
        return d.reject("no_klines")

    if skip_erratic and is_erratic_row is not None:
        try:
            cur = ctx.df.iloc[-1]
            if is_erratic_row(cur):
                return d.reject("erratic_wick")
        except Exception:
            pass

    if min_volume > 0 and ctx.scout_volume is not None:
        if float(ctx.scout_volume) < min_volume:
            return d.reject("low_volume", vol=ctx.scout_volume)

    # drift: futures vs last close of kline source
    if ctx.fut_last and ctx.df is not None:
        try:
            close = float(ctx.df.iloc[-1]["c"])
            if close > 0:
                drift = abs(ctx.fut_last - close) / close * 100.0
                d.meta["drift_pct"] = round(drift, 2)
                if drift > max_drift_pct:
                    return d.reject("drift", drift_pct=drift)
        except Exception:
            pass

    return d.accept()
