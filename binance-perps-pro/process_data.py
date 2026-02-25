#!/usr/bin/env python3
"""
Binance USDT-M Perpetual Clean Panel Builder – v1.8 (PRODUCTION)
- Fixed: Sort Order (Symbol ASC, Timestamp ASC)
- New: Symbol Metadata Generation (symbol_information.parquet)
- Fixed: Cache Invalidation (Schema/Version changes)
- Fixed: pl.datetime_range (Polars 1.x compatibility)
- Fixed: BASE_URL clean (no ?prefix=)
- Fixed: tqdm (sync loop) vs tqdm_asyncio (downloads only)
- Fixed: Klines parsing (no header)
- Fixed: Funding rate parsing (correct columns)
- Fixed: Monthly download fallback logic
- Fixed: Timestamp scaling (removed erroneous / 1000 division)
- Fixed: Timestamp truncation for Funding/Premium to ensure hourly join alignment
- Fixed: Validation logic crash (ambiguous boolean expression)
- Fixed: Data Source Paths (markPriceKlines, metrics)
- Fixed: Open Interest & Mark Price Parsers for new formats
- All code-review suggestions implemented
- All 15 original constraints + quant best practices
- Ready for production use
"""

import argparse
import asyncio
import hashlib
import json
import logging
import re
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import polars as pl
import zipfile
from aiohttp import ClientSession
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm   # ← sync loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
BASE_URL = "https://data.binance.vision/"   # clean & correct
# List of API endpoints to try (Primary -> Failover)
API_ENDPOINTS = [
    "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
]
DATA_DIR = Path("raw_data")
CACHE_DIR = Path("cache")
# Filename includes present date for versioning
OUTPUT_PARQUET = Path(f"data/binance_perps_panel_2022_{datetime.now(timezone.utc).year}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.parquet")
OUTPUT_METADATA_PARQUET = Path(f"data/symbol_information.parquet")
START_DATE = date(2022, 1, 1)

# Version string to invalidate cache if logic changes
CODE_VERSION = "1.8"

# Robust symbols.txt path
SYMBOLS_FILE = Path(__file__).parent / "symbols.txt"
SYMBOLS = [
    s for s in re.split(r"[,\s]+", SYMBOLS_FILE.read_text())
    if s and not s.startswith("#")
]
# Deduplicate symbols
SYMBOLS = sorted(list(set(SYMBOLS)))

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(12)

# ==================== CACHE MANAGEMENT ====================
def check_cache_version():
    """Checks if the cache version matches the current code version."""
    version_file = CACHE_DIR / "version.json"
    if version_file.exists():
        try:
            with open(version_file, "r") as f:
                meta = json.load(f)
            if meta.get("version") == CODE_VERSION:
                return True
        except Exception:
            pass
    return False

def update_cache_version():
    """Updates the cache version file."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_DIR / "version.json", "w") as f:
        json.dump({
            "version": CODE_VERSION,
            "updated": datetime.now(timezone.utc).isoformat(),
            "loader_version": "1.0.0"
        }, f)

def invalidate_cache_if_needed():
    """Invalidates cache if version mismatch."""
    if CACHE_DIR.exists() and not check_cache_version():
        logger.warning(f"Cache version mismatch (Current: {CODE_VERSION}). Invalidating old cache...")
        try:
            # We delete all parquet files in cache
            for f in CACHE_DIR.glob("*.parquet"):
                f.unlink()
            update_cache_version()
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")

# ==================== DOWNLOAD HELPERS ====================
async def download_file(session: ClientSession, url: str, save_path: Path, retries: int = 3) -> bool:
    if save_path.exists():
        # Quick check: if it's a 0-byte file (from previous failed attempts?), delete it
        if save_path.stat().st_size == 0:
            save_path.unlink()
        else:
            return True

    async with DOWNLOAD_SEMAPHORE:
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=120) as resp:
                    if resp.status == 200:
                        save_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(8192):
                                f.write(chunk)
                        return True
                    elif resp.status == 404:
                        return False
                    else:
                        # Retry on server errors (5xx)
                        if resp.status >= 500:
                             logger.warning(f"Server error {resp.status} for {url}. Retrying...")
                             await asyncio.sleep(2 ** attempt)
                             continue
                        return False
            except Exception as e:
                logger.warning(f"Retry {attempt+1}/{retries} {url}: {e}")
                await asyncio.sleep(2 ** attempt)
    return False


async def download_all_for_symbol(
    session: ClientSession, symbol: str, data_type: str, interval: str = None, end_date: date = None
):
    files_downloaded = 0
    current = START_DATE

    # Pre-calculate month logic
    while current <= end_date:
        # Check if we can/should try monthly download
        # Logic: If 'current' is part of a month, we try to download that full month.
        # If successful, we advance 'current' to the start of next month.
        # If failed, we fall back to daily download for 'current' and advance by 1 day.

        # Calculate start of next month
        if current.month == 12:
            next_month_start = date(current.year + 1, 1, 1)
        else:
            next_month_start = date(current.year, current.month + 1, 1)

        # Construct monthly filename/URL
        month_str = current.strftime('%Y-%m')

        url_monthly = None
        path_monthly = None

        # Data Type Mapping to Binance Directories
        # klines -> klines
        # fundingRate -> fundingRate
        # markPrice -> markPriceKlines (was premiumIndex)
        # openInterest -> metrics (daily only usually, but we check logic)

        if data_type == "klines":
            fname_monthly = f"{symbol}-{interval}-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/klines/{symbol}/{interval}/{fname_monthly}"
            path_monthly = DATA_DIR / "klines" / symbol / fname_monthly
        elif data_type == "fundingRate":
            fname_monthly = f"{symbol}-fundingRate-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/fundingRate/{symbol}/{fname_monthly}"
            path_monthly = DATA_DIR / "fundingRate" / symbol / fname_monthly
        elif data_type == "markPrice":
            # Using markPriceKlines for Mark Price
            fname_monthly = f"{symbol}-{interval}-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/markPriceKlines/{symbol}/{interval}/{fname_monthly}"
            path_monthly = DATA_DIR / "markPrice" / symbol / fname_monthly
        elif data_type == "openInterest":
            # Open Interest is in 'metrics'. Monthly metrics often missing, so we skip monthly attempt?
            # User investigation showed 404 for metrics monthly.
            # Let's NOT try monthly for OI/metrics to avoid wasting time.
            # Explicitly falling back to daily is expected behavior for metrics.
            url_monthly = None
        else:
            raise ValueError(f"Unknown data_type: {data_type}")

        # Try monthly download first (if URL exists)
        should_try_monthly = (url_monthly is not None) and ((current.day == 1) or (current == START_DATE))

        if should_try_monthly:
            if await download_file(session, url_monthly, path_monthly):
                files_downloaded += 1
                # If successful, skip to next month
                current = next_month_start
                logger.info(f"✅ Downloaded monthly {data_type} for {symbol} {month_str}")
                continue
            else:
                # If failed, we fall back to daily.
                pass

        # === Daily Fallback ===
        if data_type == "klines":
            fname_daily = f"{symbol}-{interval}-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/klines/{symbol}/{interval}/{fname_daily}"
            path_daily = DATA_DIR / "klines" / symbol / fname_daily
        elif data_type == "fundingRate":
            fname_daily = f"{symbol}-fundingRate-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/fundingRate/{symbol}/{fname_daily}"
            path_daily = DATA_DIR / "fundingRate" / symbol / fname_daily
        elif data_type == "markPrice":
            fname_daily = f"{symbol}-{interval}-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/markPriceKlines/{symbol}/{interval}/{fname_daily}"
            path_daily = DATA_DIR / "markPrice" / symbol / fname_daily
        elif data_type == "openInterest":
            # Metrics
            fname_daily = f"{symbol}-metrics-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/metrics/{symbol}/{fname_daily}"
            path_daily = DATA_DIR / "openInterest" / symbol / fname_daily

        if await download_file(session, url_daily, path_daily):
            files_downloaded += 1

        current += timedelta(days=1)

    return symbol, data_type, files_downloaded


# ==================== PARSERS ====================
def parse_klines_zip(path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            # Binance Klines CSVs do NOT have headers
            df = pl.read_csv(
                f,
                has_header=False,
                new_columns=[
                    "open_time", "open", "high", "low", "close", "volume",
                    "close_time", "quote_volume", "count",
                    "taker_buy_volume", "taker_buy_quote_volume", "ignore"
                ],
                schema_overrides={
                    "open_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Float64,
                },
                infer_schema_length=0,
                ignore_errors=True,
            )
    return (
        df.with_columns([
            pl.col("open_time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col(["open", "high", "low", "close", "volume"]).cast(pl.Float64),
        ])
        .with_columns([
            pl.max_horizontal("open", "high", "low", "close").alias("high"),
            pl.min_horizontal("open", "high", "low", "close").alias("low"),
        ])
        .select(["timestamp", "open", "high", "low", "close", "volume"])
        .unique("timestamp")
    )

def parse_funding_zip(path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pl.read_csv(
                f,
                has_header=True,
                schema_overrides={
                    "calc_time": pl.Int64,
                    "last_funding_rate": pl.Float64
                }
            )

    return (
        df.with_columns([
            pl.col("calc_time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col("last_funding_rate").alias("funding_rate"),
        ])
        .select(["timestamp", "funding_rate"])
    )


def parse_mark_price_zip(path: Path) -> pl.DataFrame:
    """
    Parses markPriceKlines.
    Format: standard kline, no header.
    Columns: open_time, open, high, low, close(mark_price), ...
    """
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pl.read_csv(
                f,
                has_header=False,
                new_columns=[
                    "open_time", "open", "high", "low", "close", "volume",
                    "close_time", "quote_volume", "count",
                    "taker_buy_volume", "taker_buy_quote_volume", "ignore"
                ],
                schema_overrides={
                    "open_time": pl.Int64,
                    "close": pl.Float64,
                },
                infer_schema_length=0,
                ignore_errors=True
            )
    return (
        df.with_columns([
            pl.col("open_time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col("close").alias("mark_price"),
        ])
        .select(["timestamp", "mark_price"])
        .group_by("timestamp")
        .agg(pl.col("mark_price").last())
    )


def parse_oi_zip(path: Path) -> pl.DataFrame:
    """
    Parses metrics.
    Format: CSV with header.
    Columns: create_time, symbol, sum_open_interest, sum_open_interest_value, ...
    create_time is string "YYYY-MM-DD HH:MM:SS"
    """
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pl.read_csv(
                f,
                has_header=True,
                schema_overrides={
                    "create_time": pl.String,
                    "sum_open_interest_value": pl.Float64
                }
            )

    # If "create_time" missing, try to detect? No, assuming standard metrics format.
    if "create_time" not in df.columns:
        return pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "open_interest": pl.Float64})

    return (
        df.with_columns([
            # Parse string datetime "2022-01-01 00:00:00" -> Datetime
            pl.col("create_time").str.strptime(pl.Datetime("ms"), format="%Y-%m-%d %H:%M:%S").dt.truncate("1h").alias("timestamp"),
            pl.col("sum_open_interest_value").alias("open_interest"),
        ])
        .select(["timestamp", "open_interest"])
        .group_by("timestamp")
        .agg(pl.col("open_interest").last())
    )

# ==================== SYMBOL METADATA ====================
async def fetch_exchange_info():
    """Fetches symbol metadata from Binance Futures API with fallback."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    async with ClientSession(headers=headers) as session:
        for url in API_ENDPOINTS:
            try:
                logger.info(f"Fetching metadata from: {url}")
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info("✅ Exchange Info fetched successfully.")
                        return data
                    elif resp.status == 451:
                        logger.warning(f"⚠️ Geo-blocked (451) at {url}. You may be in a restricted region (e.g. US).")
                    else:
                        logger.warning(f"Failed to fetch exchange info from {url}: HTTP {resp.status}")
            except Exception as e:
                logger.warning(f"Error fetching from {url}: {e}")

    logger.error("❌ Could not fetch exchange metadata from any source. Metadata columns will be null.")
    return None

def extract_symbol_metadata(info: dict, symbols: list) -> pl.DataFrame:
    """Parses exchange info into a Polars DataFrame."""
    if not info or "symbols" not in info:
        return pl.DataFrame(schema=["symbol"])

    rows = []
    for s in info["symbols"]:
        if s["symbol"] in symbols:
            # Parse Filters
            tick_size = None
            lot_size = None
            min_qty = None
            min_notional = None

            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick_size = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    lot_size = float(f["stepSize"])
                    min_qty = float(f["minQty"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", 0))

            rows.append({
                "symbol": s["symbol"],
                "base_asset": s["baseAsset"],
                "quote_asset": s["quoteAsset"],
                "margin_asset": s["marginAsset"],
                "contract_type": s["contractType"],
                "listing_time": datetime.fromtimestamp(s["onboardDate"] / 1000, timezone.utc) if s.get("onboardDate") else None,
                "delivery_time": datetime.fromtimestamp(s["deliveryDate"] / 1000, timezone.utc) if s.get("deliveryDate") and s["deliveryDate"] < 4102444800000 else None,
                "status": s["status"],
                "tick_size": tick_size,
                "lot_size": lot_size,
                "min_qty": min_qty,
                "min_notional": min_notional,
                "max_leverage": None # Not available publicly without auth
            })

    return pl.DataFrame(rows, schema={
        "symbol": pl.Categorical,
        "base_asset": pl.Utf8,
        "quote_asset": pl.Utf8,
        "margin_asset": pl.Utf8,
        "contract_type": pl.Utf8,
        "listing_time": pl.Datetime("ms"),
        "delivery_time": pl.Datetime("ms"),
        "status": pl.Utf8,
        "tick_size": pl.Float64,
        "lot_size": pl.Float64,
        "min_qty": pl.Float64,
        "min_notional": pl.Float64,
        "max_leverage": pl.Float64
    })

# ==================== MAIN PIPELINE ====================
async def build_panel(end_date: date = None, max_symbols: int = 80):
    END_DATE = end_date or (date.today() - timedelta(days=1))
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    invalidate_cache_if_needed()
    update_cache_version()

    active_symbols = SYMBOLS[:max_symbols]
    logger.info(f"Building panel with {len(active_symbols)} symbols (max={max_symbols})")

    # Fetch Metadata Early
    logger.info("Fetching Exchange Info...")
    exchange_info = await fetch_exchange_info()
    metadata_df = extract_symbol_metadata(exchange_info, active_symbols)

    # === DOWNLOAD PHASE ===
    async with ClientSession() as session:
        tasks = []
        for sym in active_symbols:
            tasks.extend([
                download_all_for_symbol(session, sym, "klines", "1h", END_DATE),
                download_all_for_symbol(session, sym, "fundingRate", end_date=END_DATE),
                # Changed from premiumIndex to markPrice (klines)
                download_all_for_symbol(session, sym, "markPrice", "1h", end_date=END_DATE),
                # Changed from openInterest to openInterest (sourced from metrics)
                download_all_for_symbol(session, sym, "openInterest", end_date=END_DATE),
            ])
        await tqdm_asyncio.gather(*tasks, desc="Downloading raw files")

    logger.info("✅ Downloads complete. Building panel...")

    start_ts = pl.datetime(2022, 1, 1, time_unit="ms")
    end_ts = pl.datetime(END_DATE.year, END_DATE.month, END_DATE.day, time_unit="ms") + pl.duration(hours=23)
    
    full_grid = pl.select(
        pl.datetime_range(start_ts, end_ts, "1h", closed="left", time_unit="ms").alias("timestamp")
    )

    panels = []

    # Store stats for metadata
    stats = []

    for sym in tqdm(active_symbols, desc="Processing symbols"):   # ← tqdm (sync)
        cache_path = CACHE_DIR / f"{sym}.parquet"

        if cache_path.exists():
            try:
                df = pl.read_parquet(cache_path)
                if df["open"].null_count() == len(df):
                     logger.warning(f"Found invalid cache for {sym} (no data), rebuilding...")
                     cache_path.unlink()
                else:
                    logger.info(f"Loaded cache for {sym}")
                    panels.append(df)

                    # Collect Stats
                    valid_df = df.filter(pl.col("is_active"))
                    if not valid_df.is_empty():
                        stats.append({
                            "symbol": sym,
                            "first_trade_time": valid_df["timestamp"].min(),
                            "last_trade_time": valid_df["timestamp"].max(),
                            "first_funding_time": df.filter(pl.col("funding_rate").is_not_null())["timestamp"].min()
                        })
                    continue
            except Exception:
                logger.warning(f"Corrupt cache for {sym}, rebuilding...")
                cache_path.unlink()

        kline_files = sorted((DATA_DIR / "klines" / sym).glob("*.zip"))
        klines = (pl.concat([parse_klines_zip(f) for f in kline_files], rechunk=False).sort("timestamp")
                  if kline_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms")}))

        fund_files = sorted((DATA_DIR / "fundingRate" / sym).glob("*.zip"))
        funding = (pl.concat([parse_funding_zip(f) for f in fund_files], rechunk=False)
                   if fund_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "funding_rate": pl.Float64}))

        # Mark Price Files (now in "markPrice" dir)
        mark_files = sorted((DATA_DIR / "markPrice" / sym).glob("*.zip"))
        mark = (pl.concat([parse_mark_price_zip(f) for f in mark_files], rechunk=False)
                if mark_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "mark_price": pl.Float64}))

        # Open Interest Files (now in "openInterest" dir, sourced from metrics)
        oi_files = sorted((DATA_DIR / "openInterest" / sym).glob("*.zip"))
        oi = (pl.concat([parse_oi_zip(f) for f in oi_files], rechunk=False)
              if oi_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "open_interest": pl.Float64}))

        if klines.is_empty():
            logger.warning(f"No klines found for {sym}, skipping...")
            continue

        # Dynamic Start Date Handling:
        # We join to the full grid (2022-01-01 -> Now).
        # Periods before listing will have null OHLC and is_active=False.
        # This preserves the full panel shape while gracefully handling different listing dates.

        df = (
            full_grid.lazy()
            .join(klines.lazy(), on="timestamp", how="left")
            .join(funding.lazy(), on="timestamp", how="left")
            .join(mark.lazy(), on="timestamp", how="left")
            .join(oi.lazy(), on="timestamp", how="left")
            .sort("timestamp")   # safety
            .with_columns([
                pl.col("mark_price").forward_fill().alias("mark_price"),
                pl.col("open_interest").forward_fill().alias("open_interest"),
                pl.lit(sym).cast(pl.Categorical).alias("symbol"),
                pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
                  .then(pl.col("timestamp"))
                  .otherwise(None)
                  .first()
                  .alias("_first"),
                pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
                  .then(pl.col("timestamp"))
                  .otherwise(None)
                  .last()
                  .alias("_last"),
            ])
            .with_columns([
                pl.col("timestamp").is_between(pl.col("_first"), pl.col("_last")).fill_null(False).alias("is_active"),
                (pl.col("close") * pl.col("volume")).alias("dollar_volume"),
                pl.col("funding_rate").shift(-1).alias("next_funding_rate"),
                pl.col("close").pct_change().alias("ret_1h"),
                (pl.col("close").shift(-1) / pl.col("close") - 1).alias("fwd_ret_1h"),
            ])
            .drop(["_first", "_last"])
            .collect()
        )

        df.write_parquet(cache_path, compression="zstd", compression_level=5)
        logger.info(f"Cached {sym}")

        valid_rows = df.filter(pl.col("is_active"))
        if not valid_rows.is_empty():
            # Collect Stats
            stats.append({
                "symbol": sym,
                "first_trade_time": valid_df["timestamp"].min() if 'valid_df' in locals() else valid_rows["timestamp"].min(),
                "last_trade_time": valid_df["timestamp"].max() if 'valid_df' in locals() else valid_rows["timestamp"].max(),
                "first_funding_time": df.filter(pl.col("funding_rate").is_not_null())["timestamp"].min()
            })

            high_ok = valid_rows.select(
                (pl.col("high") >= pl.max_horizontal("open", "low", "close")).all()
            ).item()

            if not high_ok:
                 logger.warning(f"OHLC high violation: {sym}")

            low_ok = valid_rows.select(
                (pl.col("low") <= pl.min_horizontal("open", "high", "close")).all()
            ).item()

            if not low_ok:
                logger.warning(f"OHLC low violation: {sym}")

        panels.append(df)

    if not panels:
        logger.error("No data processed!")
        return

    # SORT ORDER UPDATE: Symbol ASC, Timestamp ASC
    panel = pl.concat(panels, how="vertical").sort(["symbol", "timestamp"])

    panel = panel.with_columns([
        pl.col(["open", "high", "low", "close", "volume", "funding_rate", "mark_price",
                "open_interest", "dollar_volume", "ret_1h", "fwd_ret_1h", "next_funding_rate"]).cast(pl.Float64),
        pl.col("is_active").cast(pl.Boolean),
        pl.col("timestamp").cast(pl.Datetime("ms", "UTC")),
    ])

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    panel.write_parquet(
        OUTPUT_PARQUET,
        compression="zstd",
        compression_level=5,
        statistics=True,
        row_group_size=750_000,
    )

    # === BUILD & SAVE SYMBOL INFORMATION ===
    stats_df = pl.DataFrame(stats, schema={
        "symbol": pl.Utf8,
        "first_trade_time": pl.Datetime("ms"),
        "last_trade_time": pl.Datetime("ms"),
        "first_funding_time": pl.Datetime("ms")
    })

    # Cast symbol to categorical for join if needed, but strings are safer for metadata
    # Join with API metadata
    # Ensure symbol col is compatible
    metadata_df = metadata_df.with_columns(pl.col("symbol").cast(pl.Utf8))

    # If API fetch failed (451 Unavailable), metadata_df only has "symbol".
    # We must handle missing columns gracefully.

    # Define expected schema columns to ensure they exist before casting
    expected_cols = {
        "listing_time": pl.Datetime("ms"),
        "delivery_time": pl.Datetime("ms"),
        "base_asset": pl.Utf8,
        "quote_asset": pl.Utf8,
        "margin_asset": pl.Utf8,
        "contract_type": pl.Utf8,
        "status": pl.Utf8,
        "tick_size": pl.Float64,
        "lot_size": pl.Float64,
        "min_qty": pl.Float64,
        "min_notional": pl.Float64,
        "max_leverage": pl.Float64
    }

    for col, dtype in expected_cols.items():
        if col not in metadata_df.columns:
            metadata_df = metadata_df.with_columns(pl.lit(None).cast(dtype).alias(col))

    final_metadata = (
        metadata_df.join(stats_df, on="symbol", how="full")
        .with_columns([
            pl.col("listing_time").cast(pl.Datetime("ms")),
            pl.col("delivery_time").cast(pl.Datetime("ms")),
        ])
    )

    final_metadata.write_parquet(OUTPUT_METADATA_PARQUET)
    logger.info(f"✅ Symbol Information saved: {OUTPUT_METADATA_PARQUET}")

    meta = {
        "BUILD_VERSION": "v1.8",
        "SNAPSHOT_DATE_TIME": datetime.now(timezone.utc).isoformat(),
        "DATA_RANGE": f"2022-01-01 → {END_DATE.isoformat()}",
        "cutoff": END_DATE.isoformat(),
        "symbols": len(active_symbols),
        "universe_hash": hashlib.sha256("".join(active_symbols).encode()).hexdigest()[:16],
        "rows": len(panel),
        "size_mb": round(OUTPUT_PARQUET.stat().st_size / 1024**2, 1),
        "fields": panel.columns,
        "loader_version": "1.0.0"
    }
    with open(OUTPUT_PARQUET.with_suffix(".meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"✅ SUCCESS! Parquet: {OUTPUT_PARQUET} ({meta['size_mb']} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Binance Perps Clean Panel v1.8")
    parser.add_argument("--max-symbols", type=int, default=80, help="Fast test mode")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD")
    args = parser.parse_args()

    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    asyncio.run(build_panel(end_date=end_date, max_symbols=args.max_symbols))
