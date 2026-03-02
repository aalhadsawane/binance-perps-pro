import polars as pl
from datetime import datetime, timezone
import logging
import asyncio
import aiohttp
from pathlib import Path

logger = logging.getLogger(__name__)

def build_symbol_metadata(panel_df: pl.DataFrame, exchange_info: dict | None) -> pl.DataFrame:
    """
    Constructs the symbol metadata DataFrame by combining:
    1. Empirical stats from the Panel (first/last trade times).
    2. API data from Binance (exchangeInfo) for active symbols.

    Handles delisted symbols by inferring status and delivery time from the data.
    """

    # 1. Calculate Empirical Stats from Panel
    # group_by is expensive on huge data, but necessary.
    # Optimization: We assume panel_df is already sorted or we rely on Polars speed.
    logger.info("Calculating empirical stats from Panel data...")

    stats = (
        panel_df.lazy()
        .filter(pl.col("is_active"))
        .group_by("symbol")
        .agg([
            pl.col("timestamp").min().alias("first_trade_time"),
            pl.col("timestamp").max().alias("last_trade_time"),
            # We need first funding time too?
            # pl.col("timestamp").filter(pl.col("funding_rate").is_not_null()).min().alias("first_funding_time")
            # -> This is hard in group_by agg for specific filter.
            # Let's do it separately or simplified.
        ])
        .collect()
    )

    # If there are no active rows, return an empty dataframe with correct schema
    if "symbol" not in stats.columns:
        unique_symbols = []
    else:
        # Calculate first funding time separately or via join
        funding_stats = (
            panel_df.lazy()
            .filter(pl.col("funding_rate").is_not_null())
            .group_by("symbol")
            .agg(pl.col("timestamp").min().alias("first_funding_time"))
            .collect()
        )
        if "symbol" in funding_stats.columns:
            stats = stats.join(funding_stats, on="symbol", how="left")
        else:
            stats = stats.with_columns(pl.lit(None).cast(pl.Datetime("ms")).alias("first_funding_time"))

    # 2. Process API Data
    api_map = {}
    if exchange_info and "symbols" in exchange_info:
        for s in exchange_info["symbols"]:
            sym = s["symbol"]

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

            api_map[sym] = {
                "base_asset": s["baseAsset"],
                "quote_asset": s["quoteAsset"],
                "margin_asset": s["marginAsset"],
                "contract_type": s["contractType"],
                "listing_time": datetime.fromtimestamp(s["onboardDate"] / 1000, timezone.utc) if s.get("onboardDate") else None,
                "delivery_time": datetime.fromtimestamp(s["deliveryDate"] / 1000, timezone.utc) if s.get("deliveryDate") else None,
                "status": s["status"],
                "tick_size": tick_size,
                "lot_size": lot_size,
                "min_qty": min_qty,
                "min_notional": min_notional,
                "max_leverage": None # Not available
            }

    # 3. Merge Logic
    rows = []

    # Iterate through all symbols found in the PANEL (our universe)
    # This ensures we cover delisted coins that are in our data but not in API.
    if "symbol" in stats.columns:
        unique_symbols = stats["symbol"].drop_nulls().to_list()
    else:
        unique_symbols = []

    for sym in unique_symbols:
        emp_data = stats.filter(pl.col("symbol") == sym).to_dicts()[0]

        row = {
            "symbol": sym,
            "first_trade_time": emp_data["first_trade_time"],
            "last_trade_time": emp_data["last_trade_time"],
            "first_funding_time": emp_data["first_funding_time"]
        }

        if sym in api_map:
            # ACTIVE (In API)
            api_data = api_map[sym]
            row.update(api_data)
        else:
            # DELISTED (Not in API)
            # Infer details
            base = sym.replace("USDT", "") # Simple heuristic
            row.update({
                "base_asset": base,
                "quote_asset": "USDT",
                "margin_asset": "USDT",
                "contract_type": "PERPETUAL", # Assumption for this dataset
                "listing_time": emp_data["first_trade_time"], # Fallback
                "delivery_time": emp_data["last_trade_time"], # Delisting time = Last Trade
                "status": "DELISTED",
                "tick_size": None,
                "lot_size": None,
                "min_qty": None,
                "min_notional": None,
                "max_leverage": None
            })

        rows.append(row)

    # 4. Create DataFrame
    # Note: Polars schema expects mapping of str to DataType, verified correct.
    # Mypy might complain about dict[str, object] not matching strict TypedDict or similar,
    # but dict[str, pl.DataType] is valid for pl.DataFrame(..., schema=...).
    # We cast explicit types here.

    schema = {
        "symbol": pl.Utf8,
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
        "max_leverage": pl.Float64,
        "first_trade_time": pl.Datetime("ms"),
        "last_trade_time": pl.Datetime("ms"),
        "first_funding_time": pl.Datetime("ms")
    }

    if not rows:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows).with_columns([
        pl.col("symbol").cast(pl.Utf8),
        pl.col("base_asset").cast(pl.Utf8),
        pl.col("quote_asset").cast(pl.Utf8),
        pl.col("margin_asset").cast(pl.Utf8),
        pl.col("contract_type").cast(pl.Utf8),
        pl.col("listing_time").cast(pl.Datetime("ms")),
        pl.col("delivery_time").cast(pl.Datetime("ms")),
        pl.col("status").cast(pl.Utf8),
        pl.col("tick_size").cast(pl.Float64),
        pl.col("lot_size").cast(pl.Float64),
        pl.col("min_qty").cast(pl.Float64),
        pl.col("min_notional").cast(pl.Float64),
        pl.col("max_leverage").cast(pl.Float64),
        pl.col("first_trade_time").cast(pl.Datetime("ms")),
        pl.col("last_trade_time").cast(pl.Datetime("ms")),
        pl.col("first_funding_time").cast(pl.Datetime("ms"))
    ])

async def fetch_exchange_info():
    """Fetches symbol metadata from Binance Futures API."""
    API_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            logger.info(f"Fetching metadata from: {API_URL}")
            async with session.get(API_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info("✅ Exchange Info fetched successfully.")
                    return data
                elif resp.status == 451:
                    logger.warning(f"⚠️ Geo-blocked (451) at {API_URL}. You may be in a restricted region (e.g. US). Metadata will be null.")
                    return None
                else:
                    logger.warning(f"Failed to fetch exchange info: {resp.status}")
                    return None
        except Exception as e:
            logger.warning(f"Error fetching exchange info: {e}")
            return None

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    # Try to load the latest panel
    data_dir = Path(__file__).parent / "data"
    files = sorted(data_dir.glob("binance_perps_panel_*.parquet"))

    if not files:
        logger.error(f"No panel parquet file found in {data_dir}. Cannot build metadata.")
        return

    latest_panel = files[-1]
    logger.info(f"Loading main panel from {latest_panel}...")

    # Load the panel dataframe
    try:
        panel_df = pl.read_parquet(latest_panel)
    except Exception as e:
        logger.error(f"Failed to read parquet file: {e}")
        return

    # Fetch exchange info
    exchange_info = await fetch_exchange_info()

    # Build metadata
    logger.info("Building symbol metadata...")
    try:
        metadata_df = build_symbol_metadata(panel_df, exchange_info)

        output_path = data_dir / "symbol_information.parquet"
        metadata_df.write_parquet(output_path)
        logger.info(f"✅ Successfully wrote symbol metadata to {output_path}")
    except Exception as e:
        logger.error(f"❌ Failed to build symbol metadata: {e}")

if __name__ == "__main__":
    asyncio.run(main())
