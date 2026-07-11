import os
import time
from pathlib import Path

import pandas as pd
import requests


# ============================================
# Configuration
# ============================================

API_KEY = os.environ.get("FMP_API_KEY", "").strip()

# Resolve the CSV path relative to this script:
# Calendar Agent/scripts/update_market_cap.py
# Calendar Agent/data/pdufa_master.csv
ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = ROOT / "data" / "pdufa_master.csv"

BATCH_SIZE = 20
REQUEST_DELAY = 0.5
REQUEST_TIMEOUT = 30


# ============================================
# Validate configuration
# ============================================

if not API_KEY:
    raise RuntimeError(
        "FMP_API_KEY is missing. Add it as a GitHub repository secret "
        "and pass it to this workflow step."
    )

if not CSV_FILE.exists():
    raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

print(f"CSV file: {CSV_FILE}")


# ============================================
# Read CSV
# ============================================

df = pd.read_csv(
    CSV_FILE,
    dtype=str,
).fillna("")

required_columns = [
    "ticker",
    "pdufa_date",
    "market_cap",
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}. "
        f"Existing columns: {list(df.columns)}"
    )


# ============================================
# Clean ticker values
# ============================================

df["ticker"] = (
    df["ticker"]
    .astype(str)
    .str.strip()
    .str.upper()
)

tickers = sorted(
    ticker
    for ticker in df["ticker"].unique()
    if ticker
)

print(f"Found {len(tickers)} unique tickers.")

if not tickers:
    print("No tickers found. Nothing to update.")
    raise SystemExit(0)


# ============================================
# Download market caps in batches
# ============================================

market_caps = {}

session = requests.Session()

total_batches = (
    len(tickers) + BATCH_SIZE - 1
) // BATCH_SIZE

for start_index in range(0, len(tickers), BATCH_SIZE):

    batch = tickers[
        start_index:start_index + BATCH_SIZE
    ]

    batch_number = (
        start_index // BATCH_SIZE
    ) + 1

    symbols = ",".join(batch)

    print(
        f"Batch {batch_number}/{total_batches}: "
        f"{symbols}"
    )

    url = (
        "https://financialmodelingprep.com/"
        "stable/market-capitalization-batch"
    )

    params = {
        "symbols": symbols,
        "apikey": API_KEY,
    }

    try:
        response = session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            # Show the actual FMP error without exposing the API key.
            error_text = response.text[:1000]
            error_text = error_text.replace(API_KEY, "***")

            print(
                f"HTTP Error {response.status_code}: "
                f"{error_text}"
            )
            continue

        try:
            results = response.json()
        except ValueError:
            print(
                "FMP returned invalid JSON: "
                f"{response.text[:500]}"
            )
            continue

        # Successful batch responses should be lists.
        # Error responses may be dictionaries.
        if isinstance(results, dict):
            safe_result = str(results).replace(API_KEY, "***")
            print(f"FMP error response: {safe_result}")
            continue

        if not isinstance(results, list):
            print(
                "Unexpected FMP response type: "
                f"{type(results).__name__}"
            )
            continue

        returned_tickers = set()

        for result in results:

            ticker = str(
                result.get("symbol", "")
            ).strip().upper()

            market_cap = result.get("marketCap")

            if not ticker:
                continue

            if market_cap is None:
                print(f"  {ticker:<8} No market-cap data")
                continue

            try:
                market_cap = int(float(market_cap))
            except (TypeError, ValueError):
                print(
                    f"  {ticker:<8} Invalid market cap: "
                    f"{market_cap!r}"
                )
                continue

            market_caps[ticker] = market_cap
            returned_tickers.add(ticker)

            print(
                f"  {ticker:<8} "
                f"{market_cap:,}"
            )

        not_returned = [
            ticker
            for ticker in batch
            if ticker not in returned_tickers
        ]

        if not_returned:
            print(
                "  No valid result returned for: "
                + ", ".join(not_returned)
            )

    except requests.Timeout:
        print(
            f"Batch {batch_number} timed out "
            f"after {REQUEST_TIMEOUT} seconds."
        )

    except requests.RequestException as exc:
        print(
            f"Batch {batch_number} request failed: "
            f"{exc}"
        )

    time.sleep(REQUEST_DELAY)


# ============================================
# Update CSV
# ============================================

old_market_caps = df["market_cap"].copy()

new_market_caps = df["ticker"].map(market_caps)

# Replace the existing value only when FMP returned
# a valid market cap. Existing values are preserved
# when a ticker fails or is not returned.
df["market_cap"] = new_market_caps.where(
    new_market_caps.notna(),
    old_market_caps,
)


# Convert populated values to whole-number strings.
# This avoids CSV values such as 1500000000.0.
def normalize_market_cap(value) -> str:
    value = str(value).strip()

    if not value:
        return ""

    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return value


df["market_cap"] = df["market_cap"].apply(
    normalize_market_cap
)

changed_rows = (
    df["market_cap"] != old_market_caps
).sum()

populated_rows = (
    df["market_cap"]
    .astype(str)
    .str.strip()
    .ne("")
    .sum()
)

print()
print(f"Market caps received: {len(market_caps)}")
print(f"Rows changed: {changed_rows}")
print(f"Rows containing market cap: {populated_rows}")


# ============================================
# Save CSV atomically
# ============================================

temporary_file = CSV_FILE.with_suffix(".tmp.csv")

df.to_csv(
    temporary_file,
    index=False,
)

temporary_file.replace(CSV_FILE)

print(f"Saved CSV: {CSV_FILE}")
print("Done.")
