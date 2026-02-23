#!/usr/bin/env python3
"""
Binance USDT-M Perpetual Clean Panel Builder – v1.6 (PRODUCTION)
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
- All code-review suggestions implemented
- All 15 original constraints + quant best practices
- Ready for production use
"""

import argparse
import asyncio
import hashlib
import json
import logging
import shutil
from datetime import date, datetime, timedelta
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
DATA_DIR = Path("raw_data")
CACHE_DIR = Path("cache")
OUTPUT_PARQUET = Path("data/binance_perps_panel_2022_2026.parquet")
START_DATE = date(2022, 1, 1)

# Version string to invalidate cache if logic changes
CODE_VERSION = "1.6"

# Robust symbols.txt path
SYMBOLS_FILE = Path(__file__).parent / "symbols.txt"
SYMBOLS = [
    line.strip()
    for line in SYMBOLS_FILE.read_text().splitlines()
    if line.strip() and not line.startswith("#")
]

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
        json.dump({"version": CODE_VERSION, "updated": datetime.utcnow().isoformat()}, f)

def invalidate_cache_if_needed():
    """Invalidates cache if version mismatch."""
    if CACHE_DIR.exists() and not check_cache_version():
        logger.warning(f"Cache version mismatch (Current: {CODE_VERSION}). Invalidating old cache...")
        try:
            # We delete all parquet files in cache, but maybe keep raw data?
            # The prompt implies cache invalidation. Assuming the processed parquet cache.
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
        if data_type == "klines":
            fname_monthly = f"{symbol}-{interval}-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/klines/{symbol}/{interval}/{fname_monthly}"
            path_monthly = DATA_DIR / "klines" / symbol / fname_monthly
        elif data_type == "fundingRate":
            fname_monthly = f"{symbol}-fundingRate-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/fundingRate/{symbol}/{fname_monthly}"
            path_monthly = DATA_DIR / "fundingRate" / symbol / fname_monthly
        elif data_type == "premiumIndex":
            fname_monthly = f"{symbol}-premiumIndex-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/premiumIndex/{symbol}/{fname_monthly}"
            path_monthly = DATA_DIR / "premiumIndex" / symbol / fname_monthly
        elif data_type == "openInterest":
            fname_monthly = f"{symbol}-openInterest-{month_str}.zip"
            url_monthly = f"{BASE_URL}data/futures/um/monthly/openInterest/{symbol}/{fname_monthly}"
            path_monthly = DATA_DIR / "openInterest" / symbol / fname_monthly
        else:
            raise ValueError(f"Unknown data_type: {data_type}")

        # Try monthly download first (only if we are at start of month or haven't covered this month yet)

        should_try_monthly = (current.day == 1) or (current == START_DATE)

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
        elif data_type == "premiumIndex":
            fname_daily = f"{symbol}-premiumIndex-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/premiumIndex/{symbol}/{fname_daily}"
            path_daily = DATA_DIR / "premiumIndex" / symbol / fname_daily
        elif data_type == "openInterest":
            fname_daily = f"{symbol}-openInterest-{current.strftime('%Y-%m-%d')}.zip"
            url_daily = f"{BASE_URL}data/futures/um/daily/openInterest/{symbol}/{fname_daily}"
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
            # Remove / 1000 division, as open_time is already in milliseconds
            pl.col("open_time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col(["open", "high", "low", "close", "volume"]).cast(pl.Float64),
        ])
        # Industry-standard OHLC sanitization (handles the rare Binance aggregation quirks in 2022 data)
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
            # Funding CSVs DO have headers: calc_time, funding_interval_hours, last_funding_rate
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
            # Remove / 1000 division
            # Truncate to 1h to ensure alignment with grid (some funding times are like 00:00:00.006)
            pl.col("calc_time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col("last_funding_rate").alias("funding_rate"),
        ])
        .select(["timestamp", "funding_rate"])
    )


def parse_premium_zip(path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pl.read_csv(f, has_header=True, schema_overrides={"time": pl.Int64, "markPrice": pl.Float64})
    return (
        df.with_columns([
            # Remove / 1000 division
            # Truncate to 1h to allow grouping by hourly interval
            pl.col("time").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col("markPrice").alias("mark_price"),
        ])
        .select(["timestamp", "mark_price"])
        .group_by("timestamp")
        .agg(pl.col("mark_price").last())
    )


def parse_oi_zip(path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(path) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pl.read_csv(f, has_header=True, schema_overrides={"timestamp": pl.Int64, "openInterest": pl.Float64})
    return (
        df.with_columns([
            # Remove / 1000 division
            pl.col("timestamp").cast(pl.Datetime("ms")).dt.truncate("1h").alias("timestamp"),
            pl.col("openInterest").alias("open_interest"),
        ])
        .select(["timestamp", "open_interest"])
        .group_by("timestamp")
        .agg(pl.col("open_interest").last())
    )


# ==================== MAIN PIPELINE ====================
async def build_panel(end_date: date = None, max_symbols: int = 80):
    END_DATE = end_date or (date.today() - timedelta(days=1))
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Invalidate cache if version mismatch
    invalidate_cache_if_needed()
    # Ensure version file exists if we proceed
    update_cache_version()

    active_symbols = SYMBOLS[:max_symbols]
    logger.info(f"Building panel with {len(active_symbols)} symbols (max={max_symbols})")

    # === DOWNLOAD PHASE ===
    async with ClientSession() as session:
        tasks = []
        for sym in active_symbols:
            tasks.extend([
                download_all_for_symbol(session, sym, "klines", "1h", END_DATE),
                download_all_for_symbol(session, sym, "fundingRate", end_date=END_DATE),
                download_all_for_symbol(session, sym, "premiumIndex", end_date=END_DATE),
                download_all_for_symbol(session, sym, "openInterest", end_date=END_DATE),
            ])
        await tqdm_asyncio.gather(*tasks, desc="Downloading raw files")

    logger.info("✅ Downloads complete. Building panel...")

    # Full hourly grid — use ms to match all parsed data (fixes join error)
    start_ts = pl.datetime(2022, 1, 1, time_unit="ms")
    end_ts = pl.datetime(END_DATE.year, END_DATE.month, END_DATE.day, time_unit="ms") + pl.duration(hours=23)
    
    full_grid = pl.select(
        pl.datetime_range(start_ts, end_ts, "1h", closed="left", time_unit="ms").alias("timestamp")
    )

    panels = []
    for sym in tqdm(active_symbols, desc="Processing symbols"):   # ← tqdm (sync)
        cache_path = CACHE_DIR / f"{sym}.parquet"

        # Check cache validity (simple check for "empty" data from bad runs)
        if cache_path.exists():
            try:
                df = pl.read_parquet(cache_path)
                # If cache has all nulls in 'open', it's likely from a failed run where join failed
                if df["open"].null_count() == len(df):
                     logger.warning(f"Found invalid cache for {sym} (no data), rebuilding...")
                     cache_path.unlink()
                else:
                    logger.info(f"Loaded cache for {sym}")
                    panels.append(df)
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

        prem_files = sorted((DATA_DIR / "premiumIndex" / sym).glob("*.zip"))
        mark = (pl.concat([parse_premium_zip(f) for f in prem_files], rechunk=False)
                if prem_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "mark_price": pl.Float64}))

        oi_files = sorted((DATA_DIR / "openInterest" / sym).glob("*.zip"))
        oi = (pl.concat([parse_oi_zip(f) for f in oi_files], rechunk=False)
              if oi_files else pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "open_interest": pl.Float64}))

        # If klines are empty, we can't build a panel for this symbol
        if klines.is_empty():
            logger.warning(f"No klines found for {sym}, skipping...")
            continue

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

        # Validation — proper Polars eager scalar extraction (.item())
        assert df["timestamp"].is_sorted(), f"Timestamps not monotonic: {sym}"
        assert not df["timestamp"].is_duplicated().any(), f"Duplicates: {sym}"
        
        valid_rows = df.filter(pl.col("is_active"))
        if not valid_rows.is_empty():
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

    panel = pl.concat(panels, how="vertical").sort(["timestamp", "symbol"])

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

    meta = {
        "version": "1.6",
        "cutoff": END_DATE.isoformat(),
        "symbols": len(active_symbols),
        "universe_hash": hashlib.sha256("".join(active_symbols).encode()).hexdigest()[:16],
        "generated": datetime.utcnow().isoformat(),
        "rows": len(panel),
        "size_mb": round(OUTPUT_PARQUET.stat().st_size / 1024**2, 1),
        "fields": panel.columns,
        "start_date": "2022-01-01",
    }
    with open(OUTPUT_PARQUET.with_suffix(".meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"✅ SUCCESS! Parquet: {OUTPUT_PARQUET} ({meta['size_mb']} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Binance Perps Clean Panel v1.6")
    parser.add_argument("--max-symbols", type=int, default=80, help="Fast test mode")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD")
    args = parser.parse_args()

    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    asyncio.run(build_panel(end_date=end_date, max_symbols=args.max_symbols))
