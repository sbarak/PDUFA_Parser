import os
import re
import requests
import pandas as pd
from typing import Optional
from icalendar import Calendar
from dateutil import tz

# Returned dataframe columns (adds 'summary' so we can store blanks usefully)
SCHEMA_COLUMNS = ["ticker", "pdufa_date", "summary"]

# Rule 1: 3–5 uppercase letters at the START of SUMMARY => ticker (excluding trivial words)
LEADING_TICKER_RE = re.compile(r"^\s*([A-Z]{3,5})\b")
STOPWORDS = {"PDUFA", "ADCOM", "FDA"}

def fetch_ics(url: str) -> Calendar:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return Calendar.from_ical(r.content)

def _to_date_str(dtprop, tzname: Optional[str]) -> str:
    """Coerce VEVENT dtstart (date or datetime) to 'YYYY-MM-DD' in target tz if present."""
    if dtprop is None:
        return ""
    tzinfo = tz.gettz(tzname) if tzname else None
    try:
        d = getattr(dtprop, "dt", dtprop)
        if hasattr(d, "astimezone") and tzinfo is not None and getattr(d, "tzinfo", None) is not None:
            d = d.astimezone(tzinfo)
        d = getattr(d, "date", lambda: d)()
        return str(d)
    except Exception:
        return ""

def _extract_ticker_from_summary(summary: str) -> str:
    """Exact per your rule: if SUMMARY starts with 3–5 caps letters, that's the ticker (excluding STOPWORDS)."""
    if not summary:
        return ""
    m = LEADING_TICKER_RE.match(summary)
    if not m:
        return ""
    cand = m.group(1).upper()
    if cand in STOPWORDS:
        return ""
    return cand

def _lookup_ticker_online(company_text: str, debug: bool = False) -> str:
    """
    Online company->ticker lookup using Alpha Vantage SYMBOL_SEARCH.
    Set ALPHAVANTAGE_API_KEY in env (or GitHub Secret).
    Returns symbol or "" if not found.
    """
    api_key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    if not api_key:
        if debug:
            print("[LOOKUP] Missing ALPHAVANTAGE_API_KEY; cannot resolve:", company_text)
        return ""

    url = "https://www.alphavantage.co/query"
    params = {"function": "SYMBOL_SEARCH", "keywords": company_text, "apikey": api_key}

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        if debug:
            print("[LOOKUP] request error:", e, "| text:", company_text)
        return ""

    matches = data.get("bestMatches") or []
    if not matches:
        if debug:
            print("[LOOKUP] no matches for:", company_text)
        return ""

    # Prefer US results and stronger name match
    company_lower = company_text.lower()
    best = None
    for m in matches:
        sym  = (m.get("1. symbol") or "").strip().upper()
        name = (m.get("2. name")   or "").strip()
        reg  = (m.get("4. region") or "").strip()
        cur  = (m.get("8. currency") or "").strip()
        if not sym:
            continue
        score = 0
        if reg == "United States": score += 3
        if cur == "USD":           score += 2
        if company_lower in name.lower(): score += 3
        if best is None or score > best[0]:
            best = (score, sym)

    return best[1] if best else ""

def _company_text(summary: str) -> str:
    """Use the entire SUMMARY text for online search, as requested."""
    return (summary or "").strip()

def events_to_df(cal: Calendar, tzname: Optional[str], debug: bool = False) -> pd.DataFrame:
    rows = []
    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        summary = str(component.get("summary", "")).strip()
        dtstart = component.get("dtstart")

        # Rule 1: leading 3–5 caps letters in SUMMARY
        ticker = _extract_ticker_from_summary(summary)
        if not ticker:
            # Rule 2: search SUMMARY text online as company name
            ticker = _lookup_ticker_online(_company_text(summary), debug=debug)

        pdufa_date = _to_date_str(dtstart, tzname)
        rows.append({"ticker": ticker, "pdufa_date": pdufa_date, "summary": summary})

        if debug:
            print(f"[EVT] SUMMARY='{summary}' -> ticker='{ticker}' | date='{pdufa_date}'")

    df = pd.DataFrame(rows)
    for c in SCHEMA_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[SCHEMA_COLUMNS]
    # Keep even blank tickers here; main.py will route them to blank.csv
    return df

def fetch_all(ics_urls: list[str],
              tzname: Optional[str] = None,
              min_date: Optional[str] = None,
              max_date: Optional[str] = None,
              debug: bool = False) -> pd.DataFrame:
    frames = []
    for u in ics_urls:
        try:
            cal = fetch_ics(u)
            frames.append(events_to_df(cal, tzname, debug=debug))
        except Exception as e:
            if debug:
                print(f"[ICS]ERROR fetching {u}: {e}")
            continue

    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=SCHEMA_COLUMNS)

    # Optional date filter
    if not df.empty:
        s = pd.to_datetime(df["pdufa_date"], errors="coerce")
        if min_date:
            s_min = pd.to_datetime(min_date, errors="coerce")
            if pd.notna(s_min):
                df = df[s.isna() | (s >= s_min)]
        if max_date:
            s_max = pd.to_datetime(max_date, errors="coerce")
            if pd.notna(s_max):
                df = df[s.isna() | (s <= s_max)]

    # Deduplicate conservatively (include summary so blanks don't collapse)
    df = df.drop_duplicates(subset=["ticker", "pdufa_date", "summary"], keep="first")
    return df
