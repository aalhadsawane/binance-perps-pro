import polars as pl
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def build_symbol_metadata(panel_df: pl.DataFrame, exchange_info: dict) -> pl.DataFrame:
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

    # Calculate first funding time separately or via join
    funding_stats = (
        panel_df.lazy()
        .filter(pl.col("funding_rate").is_not_null())
        .group_by("symbol")
        .agg(pl.col("timestamp").min().alias("first_funding_time"))
        .collect()
    )

    stats = stats.join(funding_stats, on="symbol", how="left")

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
    unique_symbols = stats["symbol"].to_list()

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

    return pl.DataFrame(rows, schema=schema)
