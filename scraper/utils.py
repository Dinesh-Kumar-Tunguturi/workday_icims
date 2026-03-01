"""
Utility functions shared across ATS scrapers.
"""
import re
import logging
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from . import config

log = logging.getLogger("scraper.utils")


def strip_html(html_str: str) -> str:
    """Strip HTML tags and normalize whitespace. Truncate to 8000 chars."""
    if not html_str:
        return ""
    text = BeautifulSoup(html_str, "html.parser").get_text(" ", strip=True)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:8000]


def is_us_location(location: str, country: str = "") -> bool:
    """Check if a job location is in the United States."""
    combined = f"{location} {country}".lower()
    combined = re.sub(r'[^a-z0-9 .]', ' ', combined)
    # Remote without foreign indicator = assume US
    if "remote" in combined and not any(
        x in combined for x in ["india", "uk", "canada", "europe", "germany", "ireland", "australia"]
    ):
        return True
    return any(kw in f" {combined} " for kw in config.US_LOCATION_KEYWORDS)


def is_recent(date_str: str) -> bool:
    """Check if a date string represents a job posted within the last 24 hours."""
    if not date_str:
        return False
    dl = date_str.lower().strip()

    # Workday-style relative dates
    if any(x in dl for x in ["today", "hour", "minute", "just now"]):
        return True
    if "yesterday" in dl:
        return True

    # "Posted N Days Ago"
    m = re.search(r'(\d+)\s*day', dl)
    if m:
        return int(m.group(1)) <= 1

    # ISO 8601 date format
    try:
        dt = datetime.fromisoformat(dl.replace("Z", "+00:00"))
        return abs((datetime.now(timezone.utc) - dt).total_seconds()) <= config.LAST_N_HOURS * 3600
    except Exception:
        pass

    return False


def now_iso() -> str:
    """Current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()
