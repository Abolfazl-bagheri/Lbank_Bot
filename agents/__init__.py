"""
Multi-agent pipeline for LBank gap bot.

Scout → Filter → Analyst → Risk → Executor → Logger

Import:
    from agents.pipeline import run_pipeline, run_scout
"""

from .context import SignalContext, AgentDecision
from .pipeline import run_pipeline, run_scout

__all__ = [
    "SignalContext",
    "AgentDecision",
    "run_pipeline",
    "run_scout",
]
