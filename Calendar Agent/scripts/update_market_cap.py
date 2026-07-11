import os
import time
from pathlib import Path

import pandas as pd
import requests


# ============================================
# Configuration
# ============================================

API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()

# Script:
# Calendar Agent/scripts/update_market_cap.py
#
# CSV:
# Calendar Agent/data/pdufa_master.csv
ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = ROOT / "data" / "pdufa_master.csv"

REQUEST_DELAY = 1.1
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


# ============================================
# Validation
# ============================================

if not API_KEY:
    raise RuntimeError(
        "FINNHUB_API_KEY is missing. Add it as a GitHub repository "
        "secret and pass it to this workflow step."
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
# Download market caps
# ============================================

URL = "https://finnhub.io/api/v1/stock/profile2"

session = requests.Session()

market_caps: dict[str, int] = {}
failed_tickers: list[str] = []

for index, ticker in enumerate(tickers, start=1):

    print(f"[{index}/{len(tickers)}] {ticker}")

    params = {
        "symbol": ticker,
        "token": API_KEY,
    }

    ticker_updated = False

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(
                URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                error_text = (
                    response.text[:500]
                    .replace(API_KEY, "***")
                    .replace("\n", " ")
                )

                print(
                    f"  HTTP Error {response.status_code}: "
                    f"{error_text}"
                )

                if (
                    response.status_code
                    in {429, 500, 502, 503, 504}
                    and attempt < MAX_RETRIES
                ):
                    wait_seconds = attempt * 3

                    print(
                        f"  Retrying in {wait_seconds} seconds..."
                    )

                    time.sleep(wait_seconds)
                    continue

                break

            try:
                profile = response.json()
            except ValueError:
                print(
                    "  Invalid JSON response: "
                    f"{response.text[:500]}"
                )
                break

            if not isinstance(profile, dict):
                print(
                    "  Unexpected response type: "
                    f"{type(profile).__name__}"
                )
                break

            if not profile:
                print("  No company profile returned.")
                break

            market_cap_millions = profile.get(
                "marketCapitalization"
            )

            if market_cap_millions in (None, ""):
                print(
                    "  No marketCapitalization field returned."
                )
                break

            try:
                # Finnhub reports market capitalization
                # in millions of the company's currency.
                market_cap = int(
                    round(float(market_cap_millions) * 1_000_000)
                )
            except (TypeError, ValueError):
                print(
                    "  Invalid market-cap value: "
                    f"{market_cap_millions!r}"
                )
                break

            if market_cap <= 0:
                print(
                    f"  Invalid or zero market cap: {market_cap}"
                )
                break

            market_caps[ticker] = market_cap
            ticker_updated = True

            print(f"  Market cap: {market_cap:,}")

            break

        except requests.Timeout:

            print(
                f"  Request timed out after "
                f"{REQUEST_TIMEOUT} seconds."
            )

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 3

                print(
                    f"  Retrying in {wait_seconds} seconds..."
                )

                time.sleep(wait_seconds)
                continue

            break

        except requests.RequestException as exc:

            print(f"  Request failed: {exc}")

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 3

                print(
                    f"  Retrying in {wait_seconds} seconds..."
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

# Only overwrite a CSV value when Finnhub returned
# a valid market cap. Existing values are preserved
# when a request fails.
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
# Save CSV atomically
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
