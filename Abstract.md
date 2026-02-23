**Binance Perps Clean Panel – Pro Edition** is a premium, production-grade hourly research dataset for serious crypto quants and systematic traders.

**Abstract**

This product delivers the cleanest, most backtest-ready hourly panel of the top 80 USDT-M perpetual futures contracts on Binance (BTCUSDT, ETHUSDT, SOLUSDT … down to the 80th most liquid pair) covering **January 1, 2022 → today** (4+ years of continuous history).

The single ~900 MB–1.2 GB Parquet file contains perfectly aligned columns:
- `timestamp` (UTC, hourly, zero lookahead)
- `open/high/low/close/volume`
- `funding_rate` (non-NaN **only** at exact payment timestamps — handles dynamic 1h/4h/8h schedule)
- `mark_price`
- `open_interest`
- `is_active` (boolean — eliminates delisting/zombie look-ahead bias)
- Bonus quant columns: `dollar_volume`, `ret_1h`, `fwd_ret_1h`, `next_funding_rate`

Built with Polars for sub-8-second lazy loading on any 2-year slice, the panel meets every professional requirement: no silent forward-fills, full hourly grid, OHLC sanitization, categorical symbols, zstd compression, metadata versioning, and full reproducibility.

**Included deliverables**
- `loader.py` (8–15 lines)
- `process_data.py` (one-click rebuild)
- 3 ready-to-run Jupyter notebooks (momentum + funding factor, realistic L/S quantile backtest with fees+funding, custom factor template)
- Final parquet + README

Designed for quants who already have capital and refuse to waste time on data wrangling. This is the dataset they will happily pay **$49 one-time + $19/mo** for monthly updates — because it is measurably cleaner and more correct than anything they can build themselves in a weekend. 

**The cleanest Binance perps panel money can buy.** 🚀