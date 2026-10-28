# Agents pipeline (v1)

```
Scout → Filter → Analyst → Risk → Executor → Logger
```

## Files

| File | Role |
|------|------|
| `context.py` | `SignalContext` + `AgentDecision` |
| `scout.py` | symbol universe |
| `filter_agent.py` | blacklist, news, drift, erratic wick |
| `analyst.py` | gap via `check_signal_fn` + structure |
| `risk.py` | only_sell, max open, avg-down rules |
| `executor.py` | `off` / `paper` / `live` |
| `logger_agent.py` | accept/reject log |
| `pipeline.py` | orchestrator |

## Wire into current `bot.py` (later)

Inside `process_symbol` after you have `df`:

```python
from agents.context import SignalContext
from agents.pipeline import run_pipeline_symbol

ctx = SignalContext(symbol=symbol, tf=tf, df=df, fut_last=get_futures_last(symbol))
run_pipeline_symbol(
    ctx,
    check_signal_fn=check_signal,
    is_blacklisted=is_blacklisted,
    is_erratic_row=is_erratic_wick_candle,
    only_sell=ONLY_SELL,
    open_trades=open_trades,
    trade_mode=os.getenv("TRADE_MODE", "off"),  # off | paper | live
)
if not ctx.rejected:
    # existing telegram chart + format_signal_message(ctx.analysis)
    ...
```

## TRADE_MODE

- `off` — pipeline validates; you still send signal yourself (current behavior)
- `paper` — Executor builds virtual position (hook paper_open_fn)
- `live` — only when API works (VPS)

Live LBank from GitHub Actions remains blocked (403); keep `off` or `paper` for now.
