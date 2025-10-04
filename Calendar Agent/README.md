# PDUFA Watch — Google Calendar (ICS) Source

This version pulls events from **public Google Calendars** (ICS) and merges them into `data/pdufa_master.csv` daily.

## Configure calendars
Edit `config/calendars.yaml`:
```yaml
ics_urls:
  - https://calendar.google.com/calendar/ical/<calendar-id-1>/public/basic.ics
  - https://calendar.google.com/calendar/ical/<calendar-id-2>/public/basic.ics
timezone: America/Los_Angeles
```

> Make sure each Google Calendar is set to **'Make available to public'** so the ICS URL is accessible without auth.

## Run locally
```bash
pip install -r requirements.txt
python src/main.py
```

## GitHub Actions
The workflow runs daily and commits updates if the CSV changes.

## Output CSV schema
```
date_pdufa,company,ticker,drug,indication,priority_review,decision_type,source,announced_at,notes
```

The ICS often contains just a **summary** and a **start date**. The parser uses simple heuristics to fill `company`, `ticker`, `drug`, and `indication` when those appear in the text.
