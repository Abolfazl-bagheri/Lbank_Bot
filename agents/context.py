"""Shared context passed through the agent chain."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AgentDecision:
    """Result of one agent step."""
    ok: bool
    agent: str
    reasons: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def reject(self, reason: str, **meta: Any) -> "AgentDecision":
        self.ok = False
        self.reasons.append(reason)
        self.meta.update(meta)
        return self

    def accept(self, **meta: Any) -> "AgentDecision":
        self.ok = True
        self.meta.update(meta)
        return self


@dataclass
class SignalContext:
    """
    One symbol × timeframe candidate traveling through agents.
    Each agent only fills its own fields / decisions.
    """
    symbol: str
    tf: str

    # Scout
    scout_rank: int | None = None
    scout_change_pct: float | None = None
    scout_volume: float | None = None
    scout_source: str = ""  # pump | volume | forced

    # Market snapshot (filled early)
    df: Any = None  # pd.DataFrame klines with BB
    candle_ts: int | None = None
    fut_last: float | None = None
    kline_source: str = ""

    # Filter
    filter_decision: AgentDecision | None = None

    # Analyst
    analysis: dict[str, Any] = field(default_factory=dict)
    analyst_decision: AgentDecision | None = None

    # Risk
    risk_decision: AgentDecision | None = None
    order_plan: dict[str, Any] = field(default_factory=dict)

    # Executor
    exec_decision: AgentDecision | None = None
    execution: dict[str, Any] = field(default_factory=dict)

    # Trail of all decisions (for Logger)
    trail: list[AgentDecision] = field(default_factory=list)

    def add(self, decision: AgentDecision) -> None:
        self.trail.append(decision)

    @property
    def rejected(self) -> bool:
        for d in self.trail:
            if not d.ok:
                return True
        return False

    @property
    def reject_reason(self) -> str:
        for d in self.trail:
            if not d.ok and d.reasons:
                return f"{d.agent}: {d.reasons[0]}"
        return ""

    def summary(self) -> str:
        if self.rejected:
            return f"REJECT {self.symbol} {self.tf} | {self.reject_reason}"
        side = self.analysis.get("side", "?")
        return f"ACCEPT {side} {self.symbol} {self.tf} | score={self.analysis.get('score', '-')}"
