"""
Database layer - Supabase REST API one-by-one inserts.
Maps to exact workday_icims table columns.
"""
import asyncio
import logging
from typing import Dict, Optional

import httpx

from . import config

log = logging.getLogger("scraper.db")

_client: Optional[httpx.AsyncClient] = None
_enabled: bool = False

_lock = asyncio.Lock()
_stats = {"inserted": 0, "duplicates": 0, "failed": 0, "found": 0}


def is_enabled() -> bool:
    return _enabled


def get_stats() -> Dict:
    return dict(_stats)


async def init():
    global _client, _enabled
    if config.SUPABASE_URL and config.SUPABASE_KEY:
        _client = httpx.AsyncClient(timeout=15)
        _enabled = True
        log.info(f"🗄️  DB enabled: {config.SUPABASE_URL}")
    else:
        log.info("📄 No DB credentials → CSV-only mode")


async def close():
    global _client
    if _client:
        await _client.aclose()
        _client = None


def _headers() -> Dict:
    return {
        "apikey": config.SUPABASE_KEY,
        "Authorization": f"Bearer {config.SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }


async def insert_job(job: Dict) -> str:
    """
    Insert ONE job into workday_icims table IMMEDIATELY.
    Returns: 'inserted', 'duplicate', or 'failed'.
    """
    async with _lock:
        _stats["found"] += 1

    if not _enabled:
        return "no_db"

    # Map to exact table columns
    payload = {k: job.get(k, "") for k in config.DB_FIELDS}

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = await _client.post(
                f"{config.SUPABASE_URL}/rest/v1/{config.DB_TABLE}",
                json=payload,
                headers=_headers(),
            )

            if resp.status_code in (200, 201, 204):
                async with _lock:
                    _stats["inserted"] += 1
                return "inserted"

            if resp.status_code == 409:
                async with _lock:
                    _stats["duplicates"] += 1
                return "duplicate"

            if attempt < config.MAX_RETRIES - 1:
                await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))

        except Exception:
            if attempt < config.MAX_RETRIES - 1:
                await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))

    async with _lock:
        _stats["failed"] += 1
    log.warning(f"DB insert failed after {config.MAX_RETRIES} retries: {job.get('job_url','')[:80]}")
    return "failed"
