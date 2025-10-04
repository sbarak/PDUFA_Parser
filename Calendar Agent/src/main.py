import os
import re
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml
from dateutil import tz as dttz
from dateutil.relativedelta import relativedelta

from adapters.fdt_google_ics import fetch_all

# ===== Paths =====
ROOT   = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DATA   = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA / "pdufa_master.csv"  # resolved only
BLANK_CSV  = DATA / "blank.csv"         # unresolved (no ticker)
STATE_JSON = DATA / "state.json"

# Master CSV schema (2 columns)
CSV_FIELDS = ["ticker", "pdufa_date"]


def read_master_df() -> pd.DataFrame:
    """Load the master CSV (resolved only) or return an empty frame with the right columns."""
    if MASTER_CSV.exists():
        df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
        for c in CSV_FIELDS:
            if c not in df.columns:
                df[c] = ""
        return df[CSV_FIELDS]
    else:
        return pd.DataFrame(columns=CSV_FIELDS)


def upsert(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    """
    Upsert by (ticker, pdufa_date). If pdufa_date is blank, fall back to 'ticker' only.
    Only fills empty fields on an existing row.
    """
    key_cols = ["ticker", "pdufa_date"] if row.get("pdufa_date") else ["ticker"]

    if df.empty:
        return pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    mask = pd.Series([True] * len(df))
    for k in key_cols:
        mask &= (df[k].fillna("") == row.get(k, ""))

    idx = df[mask].index
    if len(idx) == 0:
        return pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        i = idx[0]
        for k, v in row.items():
            if k in df.columns and v and (not str(df.at[i, k]).strip()):
                df.at[i, k] = v
        return df


def _date_key(x: str):
    try:
        return pd.to_datetime(x)
    except Exception:
        return pd.Timestamp.max


def _merge_blanks(new_blanks: pd.DataFrame):
    """
    Save/merge rows with no ticker into data/blank.csv.
    Columns: summary,pdufa_date (no ticker).
    """
    cols = ["summary", "pdufa_date"]
    nb = new_blanks.copy()[["summary", "pdufa_date"]].fillna("")

    # Load existing
    if BLANK_CSV.exists():
        old = pd.read_csv(BLANK_CSV, dtype=str).fillna("")
        for c in cols:
            if c not in old.columns:
                old[c] = ""
        old = old[cols]
        combined = pd.concat([old, nb], ignore_index=True)
    else:
        combined = nb

    # Deduplicate & sort
    combined = combined.drop_duplicates(subset=cols, keep="first")
    combined["__sort"] = combined["pdufa_date"].apply(_date_key)
    combined = combined.sort_values(["__sort", "summary"]).drop(columns="__sort")

    combined.to_csv(BLANK_CSV, index=False)


# -------- Dynamic date resolution --------
def resolve_dynamic_date(expr: str | None, tzname: str | None) -> str | None:
    """
    Supports: None, "@today", "@today±<n><unit>" where unit in {d,w,m,y}.
    Returns ISO 'YYYY-MM-DD' or None if expr is falsy.

    Examples:
      None            -> None
      "@today"        -> today's date in the given tz
      "@today+90d"    -> 90 days from today
      "@today-7d"     -> 7 days ago
      "@today+3m"     -> 3 months from today
      "2026-01-01"    -> passed through as-is
    """
    if not expr:
        return None
    expr = str(expr).strip().lower()
    tzinfo = dttz.gettz(tzname) if tzname else None
    base = datetime.now(tzinfo).date()

    if expr == "@today":
        return base.isoformat()

    m = re.fullmatch(r"@today([+-])(\d+)([dwmy])", expr)
    if m:
        sign, num, unit = m.group(1), int(m.group(2)), m.group(3)
        delta = {
            "d": relativedelta(days=num),
            "w": relativedelta(weeks=num),
            "m": relativedelta(months=num),
            "y": relativedelta(years=num),
        }[unit]
        if sign == "-":
            delta = -delta
        return (base + delta).isoformat()

    # Fallback: pass through unchanged if it's already a concrete date
    return expr


def main():
    # ---- Load config ----
    with open(CONFIG / "calendars.yaml", "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    ics_urls  = y.get("ics_urls", [])
    tzname    = y.get("timezone", None)
    debug     = bool(y.get("debug", False))

    # Resolve dynamic dates
    # If min_date is not provided at all, default to "@today"
    min_expr  = y.get("min_date")
    min_date  = resolve_dynamic_date(min_expr, tzname) or resolve_dynamic_date("@today", tzname)
    max_date  = resolve_dynamic_date(y.get("max_date"), tzname)

    # ---- Fetch events (adapter returns ticker, pdufa_date, summary) ----
    df_all = fetch_all(
        ics_urls,
        tzname=tzname,
        min_date=min_date,
        max_date=max_date,
        debug=debug
    )

    # ---- Split: resolved vs blanks ----
    df_resolved = df_all[df_all["ticker"] != ""].copy()
    df_blanks   = df_all[df_all["ticker"] == ""].copy()

    # ---- Merge resolved into master CSV ----
    df_master = read_master_df()
    for r in df_resolved[["ticker", "pdufa_date"]].fillna("").to_dict(orient="records"):
        df_master = upsert(df_master, r)

    # Sort master by date then ticker
    df_master["__sort"] = df_master["pdufa_date"].apply(_date_key)
    df_master = df_master.sort_values(["__sort", "ticker"]).drop(columns="__sort")
    df_master.to_csv(MASTER_CSV, index=False)

    # ---- Write/merge blanks to blank.csv (summary + date only) ----
    if not df_blanks.empty:
        _merge_blanks(df_blanks)

    # ---- State & logs ----
    if not Path(STATE_JSON).exists():
        Path(STATE_JSON).write_text(json.dumps({"source": "google-ics", "schema": CSV_FIELDS}, indent=2))

    print(
        f"Resolved: {len(df_resolved)} | Blanks: {len(df_blanks)} | "
        f"Master rows: {len(df_master)} | min_date={min_date or 'None'} | max_date={max_date or 'None'}"
    )
    if debug:
        print("[MASTER SAMPLE]")
        print(df_master.head(12).to_string(index=False))
        if not df_blanks.empty:
            print("[BLANK SAMPLE]")
            print(df_blanks[["summary","pdufa_date"]].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
