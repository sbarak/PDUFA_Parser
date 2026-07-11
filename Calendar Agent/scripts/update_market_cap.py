import os
import time
from pathlib import Path

import pandas as pd
import requests


# ============================================
# Configuration
# ============================================

API_KEY = os.environ.get("FMP_API_KEY", "").strip()

# Script:
# Calendar Agent/scripts/update_market_cap.py
#
# CSV:
# Calendar Agent/data/pdufa_master.csv
ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = ROOT / "data" / "pdufa_master.csv"

REQUEST_DELAY = 0.35
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


# ============================================
# Validation
# ============================================

if not API_KEY:
    raise RuntimeError(
        "FMP_API_KEY is missing. Add it as a GitHub repository secret "
        "and pass it to the workflow step."
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
# Normalize tickers
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
# Download one ticker at a time
# ============================================

session = requests.Session()

market_caps: dict[str, int] = {}
failed_tickers: list[str] = []

url = (
    "https://financialmodelingprep.com/"
    "stable/market-capitalization"
)

for index, ticker in enumerate(tickers, start=1):

    print(f"[{index}/{len(tickers)}] {ticker}")

    params = {
        "symbol": ticker,
        "apikey": API_KEY,
    }

    ticker_updated = False

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                error_text = (
                    response.text[:1000]
                    .replace(API_KEY, "***")
                    .replace("\n", " ")
                )

                print(
                    f"  HTTP Error {response.status_code}: "
                    f"{error_text}"
                )

                # Retry temporary server/rate-limit errors only.
                if (
                    response.status_code
                    in {429, 500, 502, 503, 504}
                    and attempt < MAX_RETRIES
                ):
                    wait_seconds = attempt * 2

                    print(
                        f"  Retrying in "
                        f"{wait_seconds} seconds..."
                    )

                    time.sleep(wait_seconds)
                    continue

                break

            try:
                results = response.json()
            except ValueError:
                print(
                    "  Invalid JSON response: "
                    f"{response.text[:500]}"
                )
                break

            # The successful endpoint normally returns a list.
            if isinstance(results, dict):
                safe_response = str(results).replace(
                    API_KEY,
                    "***",
                )

                print(
                    f"  FMP response: {safe_response}"
                )
                break

            if not isinstance(results, list):
                print(
                    "  Unexpected response type: "
                    f"{type(results).__name__}"
                )
                break

            if not results:
                print("  No market-cap data returned.")
                break

            result = results[0]

            returned_ticker = str(
                result.get("symbol", ticker)
            ).strip().upper()

            market_cap = result.get("marketCap")

            if market_cap is None:
                print(
                    "  Response contains no marketCap field: "
                    f"{result}"
                )
                break

            try:
                market_cap_value = int(
                    float(market_cap)
                )
            except (TypeError, ValueError):
                print(
                    f"  Invalid market-cap value: "
                    f"{market_cap!r}"
                )
                break

            market_caps[returned_ticker] = (
                market_cap_value
            )

            print(
                f"  Market cap: "
                f"{market_cap_value:,}"
            )

            ticker_updated = True
            break

        except requests.Timeout:

            print(
                f"  Request timed out after "
                f"{REQUEST_TIMEOUT} seconds."
            )

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 2

                print(
                    f"  Retrying in "
                    f"{wait_seconds} seconds..."
                )

                time.sleep(wait_seconds)
                continue

            break

        except requests.RequestException as exc:

            print(f"  Request failed: {exc}")

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 2

                print(
                    f"  Retrying in "
                    f"{wait_seconds} seconds..."
                )

                time.sleep(wait_seconds)
                continue

            break

    if not ticker_updated:
        failed_tickers.append(ticker)
        print(f"  Failed to update {ticker}")

    time.sleep(REQUEST_DELAY)


# ============================================
# Update CSV
# ============================================

old_market_caps = df["market_cap"].copy()

mapped_market_caps = df["ticker"].map(
    market_caps
)

# Update only tickers successfully returned by FMP.
# Keep the old market cap when a request fails.
df["market_cap"] = mapped_market_caps.where(
    mapped_market_caps.notna(),
    old_market_caps,
)


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

changed_rows = int(
    (
        df["market_cap"].astype(str)
        != old_market_caps.astype(str)
    ).sum()
)

populated_rows = int(
    df["market_cap"]
    .astype(str)
    .str.strip()
    .ne("")
    .sum()
)


# ============================================
# Results
# ============================================

print()
print(f"Market caps received: {len(market_caps)}")
print(f"Rows changed: {changed_rows}")
print(f"Rows containing market cap: {populated_rows}")
print(f"Tickers failed: {len(failed_tickers)}")

if failed_tickers:
    print(
        "Failed ticker list: "
        + ", ".join(failed_tickers)
    )


# ============================================
# Save atomically
# ============================================

temporary_file = CSV_FILE.with_suffix(
    ".tmp.csv"
)

df.to_csv(
    temporary_file,
    index=False,
)

temporary_file.replace(CSV_FILE)

print(f"Saved CSV: {CSV_FILE}")
print("Done.")
