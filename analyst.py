"""
Analyst — gap / structure / score.

Uses bot.check_signal + optional structure helpers passed in as callables
so this package stays independent of bot.py internals.
"""

from __future__ import annotations

from typing import Any, Callable

from .context import AgentDecision, SignalContext


def run_analyst(
    ctx: SignalContext,
    *,
    check_signal_fn: Callable[..., dict | None],
    structure_fn: Callable[..., dict] | None = None,
    score_fn: Callable[..., dict] | None = None,
) -> AgentDecision:
    d = AgentDecision(ok=True, agent="analyst")

    if ctx.df is None:
        return d.reject("no_df")

    sig = check_signal_fn(ctx.df, ctx.symbol, ctx.tf)
    if not sig:
        return d.reject("no_gap")

    ctx.analysis = dict(sig)
    ctx.candle_ts = sig.get("candle_ts") or ctx.candle_ts
    ctx.fut_last = sig.get("fut_last") or ctx.fut_last

    # optional HTF / structure enrichment
    if structure_fn is not None:
        try:
            st = structure_fn(ctx.df, float(sig.get("fut_last") or sig.get("close") or 0))
            if st:
                ctx.analysis["structure"] = st
        except Exception as e:
            d.meta["structure_err"] = str(e)[:80]

    if score_fn is not None:
        try:
            sc = score_fn(ctx.analysis)
            if isinstance(sc, dict):
                ctx.analysis["score"] = sc.get("score", sc)
                ctx.analysis["score_detail"] = sc
        except Exception:
            pass

    return d.accept(side=sig.get("side"), diff_pct=sig.get("diff_pct"))
