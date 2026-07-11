import pandas as pd
import os
import requests
import time

# ============================================
# Configuration
# ============================================

API_KEY = os.environ["FMP_API_KEY"]
CSV_FILE = "Calendar Agent/data/pdufa_master.csv"

BATCH_SIZE = 20
REQUEST_DELAY = 0.25   # Seconds between requests


# ============================================
# Read CSV
# ============================================

df = pd.read_csv(CSV_FILE)

required_columns = ["ticker", "pdufa_date", "market_cap"]

for col in required_columns:
    if col not in df.columns:
        raise Exception(f"Missing required column: {col}")

# Clean ticker list
tickers = (
    df["ticker"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

print(f"Found {len(tickers)} unique tickers.")

market_caps = {}


# ============================================
# Download Market Caps in batches
# ============================================

for i in range(0, len(tickers), BATCH_SIZE):

    batch = tickers[i:i + BATCH_SIZE]
    symbols = ",".join(batch)

    url = (
        "https://financialmodelingprep.com/api/v3/profile/"
        f"{symbols}?apikey={API_KEY}"
    )

    print(f"Batch {i//BATCH_SIZE + 1}: {symbols}")

    try:

        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            print(f"HTTP Error {response.status_code}")
            continue

        profiles = response.json()

        if not isinstance(profiles, list):
            print("Unexpected response")
            continue

        for profile in profiles:

            ticker = profile.get("symbol")
            market_cap = profile.get("marketCap")

            if ticker:
                market_caps[ticker] = market_cap
                print(f"  {ticker:<6} {market_cap:,}" if market_cap else f"  {ticker:<6} None")

    except Exception as e:
        print(e)

    time.sleep(REQUEST_DELAY)


# ============================================
# Update CSV
# ============================================

updated = 0

for idx, row in df.iterrows():

    ticker = str(row["ticker"]).strip()

    if ticker in market_caps:
        df.at[idx, "market_cap"] = market_caps[ticker]
        updated += 1

print(f"\nUpdated {updated} rows.")


# ============================================
# Save
# ============================================

df.to_csv(CSV_FILE, index=False)

print("Done.")
