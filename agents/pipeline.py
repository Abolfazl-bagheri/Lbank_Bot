"""
Orchestrator: Scout list → per symbol×tf Filter→Analyst→Risk→Executor→Logger
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

from .analyst import run_analyst
from .context import SignalContext
from .executor import run_executor
from .filter_agent import run_filter
from .logger_agent import format_cycle_stats, run_logger
from .risk import run_risk
from .scout import run_scout

log = logging.getLogger("agents.pipeline")


def run_pipeline_symbol(
    ctx: SignalContext,
    *,
    # Filter deps
    is_blacklisted: Callable[[str], bool] | None = None,
    is_inno: Callable[[str], bool] | None = None,
    is_news_window: Callable[[], bool] | None = None,
    is_erratic_row: Callable[[Any], bool] | None = None,
    max_drift_pct: float = 8.0,
    # Analyst deps
    check_signal_fn: Callable[..., dict | None],
    structure_fn: Callable[..., dict] | None = None,
    score_fn: Callable[..., dict] | None = None,
    # Risk deps
    only_sell: bool = True,
    open_trades: list | None = None,
    # Executor
    trade_mode: str = "off",
    paper_open_fn: Callable[..., dict | None] | None = None,
    live_open_fn: Callable[..., dict | None] | None = None,
    # Logger
    verbose_rejects: bool = False,
    send_telegram: Callable[[str], None] | None = None,
) -> SignalContext:
    """Run full chain for one context. Stops at first reject."""

    fd = run_filter(
        ctx,
        is_blacklisted=is_blacklisted,
        is_inno=is_inno,
        is_news_window=is_news_window,
        is_erratic_row=is_erratic_row,
        max_drift_pct=max_drift_pct,
    )
    ctx.filter_decision = fd
    ctx.add(fd)
    if not fd.ok:
        run_logger(ctx, verbose=verbose_rejects, send_telegram=send_telegram)
        return ctx

    ad = run_analyst(
        ctx,
        check_signal_fn=check_signal_fn,
        structure_fn=structure_fn,
        score_fn=score_fn,
    )
    ctx.analyst_decision = ad
    ctx.add(ad)
    if not ad.ok:
        run_logger(ctx, verbose=verbose_rejects, send_telegram=send_telegram)
        return ctx

    rd = run_risk(
        ctx,
        only_sell=only_sell,
        open_trades=open_trades,
    )
    ctx.risk_decision = rd
    ctx.add(rd)
    if not rd.ok:
        run_logger(ctx, verbose=verbose_rejects, send_telegram=send_telegram)
        return ctx

    ed = run_executor(
        ctx,
        mode=trade_mode,
        paper_open_fn=paper_open_fn,
        live_open_fn=live_open_fn,
    )
    ctx.exec_decision = ed
    ctx.add(ed)

    run_logger(ctx, verbose=verbose_rejects, send_telegram=send_telegram, notify_accept=False)
    return ctx


def run_pipeline(
    symbols: Iterable[str],
    tfs: Iterable[str],
    *,
    load_ctx: Callable[[str, str], SignalContext | None],
    check_signal_fn: Callable[..., dict | None],
    trade_mode: str = "off",
    open_trades: list | None = None,
    only_sell: bool = True,
    is_blacklisted: Callable[[str], bool] | None = None,
    is_erratic_row: Callable[[Any], bool] | None = None,
    structure_fn: Callable[..., dict] | None = None,
    verbose_rejects: bool = False,
) -> dict[str, Any]:
    """
    Batch runner.
    load_ctx(symbol, tf) must attach df + fut_last or return None to skip.
    """
    accepted = 0
    rejected = 0
    by_reason: dict[str, int] = {}
    accepted_ctx: list[SignalContext] = []

    for symbol in symbols:
        for tf in tfs:
            ctx = load_ctx(symbol, tf)
            if ctx is None:
                rejected += 1
                by_reason["load_fail"] = by_reason.get("load_fail", 0) + 1
                continue
            run_pipeline_symbol(
                ctx,
                check_signal_fn=check_signal_fn,
                structure_fn=structure_fn,
                only_sell=only_sell,
                open_trades=open_trades,
                trade_mode=trade_mode,
                is_blacklisted=is_blacklisted,
                is_erratic_row=is_erratic_row,
                verbose_rejects=verbose_rejects,
            )
            if ctx.rejected:
                rejected += 1
                r = ctx.reject_reason or "unknown"
                key = r.split(":")[0].strip() if r else "unknown"
                by_reason[key] = by_reason.get(key, 0) + 1
            else:
                accepted += 1
                accepted_ctx.append(ctx)

    stats = {
        "accepted": accepted,
        "rejected": rejected,
        "by_reason": by_reason,
        "accepted_list": accepted_ctx,
        "line": format_cycle_stats(accepted, rejected, by_reason),
    }
    log.info(stats["line"])
    return stats


# re-export scout helper
__all__ = ["run_pipeline", "run_pipeline_symbol", "run_scout"]
