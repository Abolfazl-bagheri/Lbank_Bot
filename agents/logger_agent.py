"""
Logger — one place for accept/reject visibility.

Telegram stays quiet on rejects unless verbose=True.
"""

from __future__ import annotations

import logging
from typing import Callable

from .context import SignalContext

log = logging.getLogger("agents")


def run_logger(
    ctx: SignalContext,
    *,
    verbose: bool = False,
    send_telegram: Callable[[str], None] | None = None,
    notify_accept: bool = False,
) -> None:
    line = ctx.summary()
    if ctx.rejected:
        log.info(line)
        if verbose and send_telegram:
            send_telegram(f"⛔ {line}")
        return

    log.info(line)
    if notify_accept and send_telegram:
        side = ctx.analysis.get("side", "")
        diff = ctx.analysis.get("diff_pct")
        extra = f" · Δ{diff:.2f}%" if isinstance(diff, (int, float)) else ""
        mode = (ctx.execution or {}).get("mode", "off")
        send_telegram(
            f"✅ {side} <b>{ctx.symbol}</b> {ctx.tf}{extra}\n"
            f"exec={mode}"
        )


def format_cycle_stats(accepted: int, rejected: int, by_reason: dict[str, int]) -> str:
    top = sorted(by_reason.items(), key=lambda x: -x[1])[:5]
    reasons = ", ".join(f"{k}={v}" for k, v in top) if top else "—"
    return f"pipeline accept={accepted} reject={rejected} | {reasons}"
