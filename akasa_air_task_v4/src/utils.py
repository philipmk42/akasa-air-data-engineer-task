from __future__ import annotations
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def parse_ts(ts: str, tz_name: str = "Asia/Kolkata") -> datetime:
    # Accepts ISO-like strings and normalizes to UTC
    local = ZoneInfo(tz_name)
    dt = datetime.fromisoformat(ts.replace("Z","+00:00")) if "T" in ts else datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=local)
    return dt.astimezone(timezone.utc)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)
