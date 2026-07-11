import os
import time
from pathlib import Path

import pandas as pd
import requests


# ============================================
# Configuration
# ============================================

API_KEY = os.environ.get("FMP_API_KEY", "").strip()

# Script location:
# Calendar Agent/scripts/update_market_cap.py
#
# CSV location:
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
# Download market cap one ticker at a time
# ============================================

session = requests.Session()
market_caps: dict[str, int] = {}

for index, ticker in enumerate(tickers, start=1):

    print(f"[{index}/{len(tickers)}] {ticker}")

    url = (
        "https://financialmodelingprep.com/api/v3/profile/"
        f"{ticker}"
    )

    params = {
        "apikey": API_KEY,
    }

    success = False

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                error_text = response.text[:1000]
                error_text = error_text.replace(API_KEY, "***")

                print(
                    f"  HTTP Error {response.status_code}: "
                    f"{error_text}"
                )

                if response.status_code in {429, 500, 502, 503, 504}:
                    if attempt < MAX_RETRIES:
                        wait_seconds = attempt * 2
                        print(
                            f"  Retrying in {wait_seconds} seconds..."
                        )
                        time.sleep(wait_seconds)
                        continue

                break

            try:
                profiles = response.json()
            except ValueError:
                print(
                    "  Invalid JSON response: "
                    f"{response.text[:500]}"
                )
                break

            if isinstance(profiles, dict):
                safe_response = str(profiles).replace(API_KEY, "***")
                print(f"  FMP response: {safe_response}")
                break

            if not isinstance(profiles, list):
                print(
                    "  Unexpected response type: "
                    f"{type(profiles).__name__}"
                )
                break

            if not profiles:
                print("  No profile returned.")
                break

            profile = profiles[0]

            returned_ticker = str(
                profile.get("symbol", ticker)
            ).strip().upper()

            market_cap = profile.get("marketCap")

            if market_cap is None:
                print("  No market-cap data returned.")
                break

            try:
                market_cap_value = int(float(market_cap))
            except (TypeError, ValueError):
                print(
                    f"  Invalid market-cap value: "
                    f"{market_cap!r}"
                )
                break

            market_caps[returned_ticker] = market_cap_value

            print(
                f"  Market cap: "
                f"{market_cap_value:,}"
            )

            success = True
            break

        except requests.Timeout:
            print(
                f"  Request timed out after "
                f"{REQUEST_TIMEOUT} seconds."
            )

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 2
                print(
                    f"  Retrying in {wait_seconds} seconds..."
                )
                time.sleep(wait_seconds)
                continue

        except requests.RequestException as exc:
            print(f"  Request failed: {exc}")

            if attempt < MAX_RETRIES:
                wait_seconds = attempt * 2
                print(
                    f"  Retrying in {wait_seconds} seconds..."
                )
                time.sleep(wait_seconds)
                continue

        break

    if not success:
        print(f"  Failed to update {ticker}")

    time.sleep(REQUEST_DELAY)


# ============================================
# Update CSV
# ============================================

old_market_caps = df["market_cap"].copy()

new_market_caps = df["ticker"].map(market_caps)

# Replace only when a valid new value was returned.
# Existing values remain unchanged when a request fails.
df["market_cap"] = new_market_caps.where(
    new_market_caps.notna(),
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
    (df["market_cap"] != old_market_caps).sum()
)

populated_rows = int(
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
