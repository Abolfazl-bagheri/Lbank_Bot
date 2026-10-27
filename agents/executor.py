"""
Executor — only places paper/live after Risk PASS.

Modes:
  off   → no position; analysis-only (signal still logged upstream)
  paper → virtual fill into local state (callables from bot)
  live  → real API (optional; currently unreliable on Actions)

This agent never decides strategy — only executes order_plan.
"""

from __future__ import annotations

from typing import Any, Callable

from .context import AgentDecision, SignalContext

# off | paper | live
TRADE_MODE_DEFAULT = "off"


def run_executor(
    ctx: SignalContext,
    *,
    mode: str = TRADE_MODE_DEFAULT,
    paper_open_fn: Callable[..., dict | None] | None = None,
    paper_add_fn: Callable[..., dict | None] | None = None,
    live_open_fn: Callable[..., dict | None] | None = None,
    live_add_fn: Callable[..., dict | None] | None = None,
) -> AgentDecision:
    d = AgentDecision(ok=True, agent="executor")
    mode = (mode or "off").lower().strip()
    plan = ctx.order_plan or {}

    if mode == "off":
        ctx.execution = {"mode": "off", "status": "skipped"}
        return d.accept(mode="off")

    if not plan:
        return d.reject("no_plan")

    action = plan.get("action", "open")
    result = None

    try:
        if mode == "paper":
            if action == "add" and paper_add_fn:
                result = paper_add_fn(ctx, plan)
            elif paper_open_fn:
                result = paper_open_fn(ctx, plan)
            else:
                # minimal local paper record
                result = {
                    "status": "open",
                    "mode": "paper",
                    "symbol": plan.get("symbol"),
                    "side": plan.get("side"),
                    "entry": plan.get("entry"),
                    "margin": plan.get("margin"),
                    "tp_price": plan.get("tp_price"),
                    "tf": plan.get("tf"),
                    "candle_ts": plan.get("candle_ts"),
                }
        elif mode == "live":
            if action == "add" and live_add_fn:
                result = live_add_fn(ctx, plan)
            elif live_open_fn:
                result = live_open_fn(ctx, plan)
            else:
                return d.reject("live_fn_missing")
        else:
            return d.reject("unknown_mode", mode=mode)
    except Exception as e:
        return d.reject("exec_error", error=str(e)[:120])

    if not result:
        return d.reject("exec_failed")

    ctx.execution = dict(result) if isinstance(result, dict) else {"raw": result}
    ctx.execution["mode"] = mode
    return d.accept(mode=mode, action=action)
