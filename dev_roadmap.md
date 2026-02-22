# Binance Perps Clean Panel – Pro Edition  
**Implementation Roadmap**  
**Target launch: March 10–12, 2026**  
**Version: 1.0**  
**Last updated: 2026-02-23**

---

### 0. Business and Technical Constraints

CRITICAL CONSTRAINTS THAT MUST BE MET , NON-NEGOTIABLE (User's 5 + 10 Extended)

1. Exact funding payment-time alignment — funding_rate column is non-NaN ONLY at the precise fundingTime (handles dynamic 1h/4h/8h schedules post-May 2025 perfectly). This is the $19/mo killer feature.
2. is_active boolean mandatory — prevents any look-ahead bias on delistings or "zombie" coins.
3. Zero lookahead + perfect cross-symbol timestamps — every symbol shares identical hourly UTC timestamps; missing = NaN.
4. Loads in <8 seconds for any 2-year slice on a normal laptop (Polars lazy + optimized Parquet).
5. Premium quality & feel — file size ≤1.2 GB compressed, zero obvious errors, pro-grade docs.

6. Missing candles / gaps handled explicitly — full hourly grid generated per symbol. Missing klines → OHLCV = NaN, volume = 0. Gaps >4h are logged; short gaps left as NaN (user decides ffill in notebook). Never silently forward-fill without trace.
7. is_active logic is automatic & defensible — per-symbol active window = first kline with volume > 0 to last kline with volume > 0. Outside = False. Handles listings/delisting automatically for the fixed 80-symbol universe.
8. Column sourcing locked (no ambiguity):

- timestamp → kline open_time (UTC, floored to hour)
- open/high/low/close/volume → klines 1h (volume = base asset, standard)
- funding_rate → fundingRate files @ exact fundingTime
- mark_price → premiumIndex + fundingRate markPrice (hourly last-known)
- open_interest → openInterest files (USD value, hourly resampled)
- is_active → derived as above


9. Precision & types fixed — Float64 everywhere (prices, rates, OI, volume). No float32, no Decimal (perf killer). Timestamps = datetime64[ns, UTC]. Parquet schema enforced.
10. Data validation layer — every run checks: OHLC consistency (H ≥ max(O,C,L), L ≤ min), volume ≥ 0, OI ≥ 0, no duplicate timestamps, monotonic timestamps. Failures logged + auto-corrected where safe.
11. Parquet production-grade — single file, zstd compression level 5, sorted by [timestamp, symbol], row-groups ~750k rows, dictionary encoding on symbol/is_active. Includes metadata: data_cutoff, universe_hash, version.
12. Reproducibility & update safety — process_data.py is fully deterministic, supports --end-date or "today", --force. Monthly updates = re-run once (manual for now, as specified).
13. Processing performance — concurrent download + Polars streaming/lazy = full 4+ year 80-symbol rebuild in <90 minutes on normal machine.
14. Edge-case coverage — dynamic funding freq, symbols listed mid-period (early NaNs), rare delistings, data.vision file lag, empty days for low-liquidity hours in 2022.
15. Backtest hygiene — closed-candle only, funding/mark only available at their exact timestamp, is_active gate, no synthetic data that creates artificial continuity.

## 1. Project Structure (final)
```
binance_perps_pro/
├── __init__.py
├── loader.py          # 8–15 lines, lazy Polars
├── process_data.py    # full pipeline
├── data/
│   └── binance_perps_panel_2022_2026.parquet   # final ~1 GB
├── notebooks/
│   ├── 01_momentum_funding_factor.ipynb
│   ├── 02_long_short_quantile_backtest_with_fees_funding.ipynb
│   └── 03_custom_factor_example.ipynb
├── tests/
│   ├── test_data_integrity.py
│   └── test_loader.py
├── requirements.txt
├── pyproject.toml
└── README.md
```

## 2. Phase Breakdown (10–12 calendar days)

**Day 0–1 (Feb 23–24) – Setup & Universe**
- Hardcode 80-symbol list (above)
- Create `process_data.py` skeleton with argparse (`--end-date`, `--force`, `--workers`)
- Implement concurrent downloader (aiohttp + tqdm)

**Day 2–4 – Core Pipeline (process_data.py)**
- Download & parse:
  - klines 1h (daily + monthly)
  - fundingRate
  - premiumIndex (for mark_price)
  - openInterest
- Build full hourly grid per symbol (timestamp alignment)
- Merge with exact funding-time logic
- Compute `is_active`, `mark_price`, `open_interest`
- Validation layer (15 checks)
- Write Parquet (zstd-5, sorted, metadata, dictionary encoding)

**Day 5 – Loader Package**
- `load_panel(start=None, end=None, symbols=None)` → Lazy Polars DataFrame
- Predicate pushdown + fast 2-year slice (<8s)

**Day 6–7 – Notebooks**
- 01: Momentum + funding factor construction
- 02: Realistic quantile L/S backtest (fees 0.04%, funding exact, is_active filter)
- 03: Template with custom factor + walk-forward

**Day 8 – Polish & Tests**
- Full reproducibility test (re-run produces identical parquet hash)
- Gap logging, README, Gumroad copy
- Package on PyPI/test.pypi

**Day 9–10 – QA & Final**
- User review of parquet + notebooks
- Size & speed benchmarks
- Deliverables zip

**Day 11–12 – Buffer / Launch prep**

## 3. Non-Negotiable Constraints (enforced in code)
(See full list in project brief section #0 – all 15 should have unit tests)

## 4. Tech Stack
- Python 3.11+
- Polars 1.0+ (lazy + streaming)
- aiohttp + tenacity
- pyarrow + zstd
- pytest + hypothesis (data integrity)
- black + ruff + mypy

## 5. Risks & Mitigations
- Data.vision lag → fallback to monthly files + retry
- Dynamic funding schedule → parse every fundingTime exactly
- New listings/delisting → automatic is_active handles 100%

## 6. Success Metrics (MVP done when…)
- parquet ≤1.2 GB
- `load_panel("2024-01-01")` <8s
- Notebook 02 reproduces realistic Sharpe with fees+funding
- Zero lookahead in any column
- Full process from scratch <90 min



note: the symbol list contains 80 symbols listed in `symbols.txt` file in project root. This is the universe for us.

---

### Important technical considerations.


Thoughts on `float64` (double) vs `int64` with scaling factor**
Stick with **float64 everywhere** (no scaling). It is the industry standard for hourly panels like this and offers zero practical benefit to switch to scaled `int64` here.

**Other common comp-sci / quant-research tricks (2025–2026 industry standard)**

These are the ones that separate weekend hacks from $49/mo products:

1. **Timestamps as `int64` nanoseconds UTC** (not datetime) → perfect alignment, zero timezone bugs, faster joins.
2. **Categorical dtype on `symbol`** → 50–70% memory saving on repeated strings, faster group-by.
3. **Parquet metadata + schema enforcement** → version, cutoff date, universe_hash, git commit. Loader can warn if stale.
4. **Sorted + row-group optimized Parquet** (`sorted by [timestamp, symbol]`, 500k–1M row groups) → blazing fast slice reads.
5. **Lazy Polars + predicate pushdown** → `load_panel(start, end)` reads <2% of file for 1-year slice.
6. **Exact point-in-time enforcement** (your #1 request):
   - `timestamp` = kline **open_time** floored to hour
   - funding_rate = NaN everywhere except **exact fundingTime** rows (handles 1h/4h/8h dynamic schedule)
   - mark_price & open_interest = last-known value at that exact timestamp (no forward leakage)
   - is_active = True only between first >0 volume candle and last >0 volume candle per symbol
7. **Gap & missing data policy** (logged, never silent):
   - Full hourly grid per symbol
   - Missing kline → OHLCV=NaN, volume=0
   - Gaps >6h → warning in process_data.py log
8. **No lookahead ever** → all columns are “as-of” the candle close.
9. **Reproducible seed + pinned deps** (`requirements.txt` + `pyproject.toml` with hashes)
10. **Data integrity tests** on every run: OHLC monotonicity, volume ≥0, no duplicate ts, funding only at funding times.

---
