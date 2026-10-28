"""
Risk — position rules before Executor.

No exchange calls. Only policy.
"""

from __future__ import annotations

from typing import Any, Callable

from .context import AgentDecision, SignalContext

# Defaults aligned with current bot policy
DEFAULT_MARGIN = 1.0
DEFAULT_LEVERAGE = 10
DEFAULT_TP_MARGIN_PCT = 30.0
MAX_OPEN = 15
MAX_ADDS = 3


def run_risk(
    ctx: SignalContext,
    *,
    only_sell: bool = True,
    open_trades: list | None = None,
    max_open: int = MAX_OPEN,
    max_adds: int = MAX_ADDS,
    margin: float = DEFAULT_MARGIN,
    leverage: int = DEFAULT_LEVERAGE,
    tp_margin_pct: float = DEFAULT_TP_MARGIN_PCT,
    allow_avg_down: bool = True,
    same_candle_add_ok: bool = False,
) -> AgentDecision:
    d = AgentDecision(ok=True, agent="risk")
    side = (ctx.analysis or {}).get("side")

    if not side:
        return d.reject("no_side")

    if only_sell and side != "SELL":
        return d.reject("long_disabled")

    opens = [t for t in (open_trades or []) if t.get("status") == "open"]
    if len(opens) >= max_open:
        # still allow add on existing symbol
        existing = next((t for t in opens if t.get("symbol") == ctx.symbol), None)
        if existing is None:
            return d.reject("max_open", n=len(opens))

    existing = next(
        (t for t in opens if t.get("symbol") == ctx.symbol and t.get("side") == side),
        None,
    )

    plan: dict[str, Any] = {
        "side": side,
        "symbol": ctx.symbol,
        "tf": ctx.tf,
        "margin": margin,
        "leverage": leverage,
        "tp_margin_pct": tp_margin_pct,
        "entry": float(ctx.analysis.get("fut_last") or ctx.analysis.get("open") or 0),
        "candle_ts": ctx.candle_ts,
        "action": "open",
    }

    if existing is not None:
        if not allow_avg_down:
            return d.reject("already_open")
        adds = int(existing.get("adds") or 0)
        if adds >= max_adds:
            return d.reject("max_adds", adds=adds)
        last_cts = int(existing.get("last_leg_candle_ts") or existing.get("entry_candle_ts") or 0)
        cts = int(ctx.candle_ts or 0)
        if not same_candle_add_ok and cts and last_cts and cts <= last_cts:
            return d.reject("same_candle_add")
        # only add if short is in loss (price above avg entry)
        avg = float(existing.get("entry") or 0)
        px = plan["entry"]
        if avg > 0 and px <= avg * 1.001:
            return d.reject("not_in_loss_for_add")
        plan["action"] = "add"
        plan["existing"] = existing

    # TP price for short: entry * (1 - tp_margin_pct/leverage/100)
    entry = plan["entry"]
    if entry > 0 and side == "SELL":
        price_move = (tp_margin_pct / 100.0) / float(leverage)
        plan["tp_price"] = entry * (1.0 - price_move)
    elif entry > 0 and side == "LONG":
        price_move = (tp_margin_pct / 100.0) / float(leverage)
        plan["tp_price"] = entry * (1.0 + price_move)

    ctx.order_plan = plan
    return d.accept(**{k: plan[k] for k in ("action", "margin", "leverage") if k in plan})
