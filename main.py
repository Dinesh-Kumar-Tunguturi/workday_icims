"""
main.py - Production Job Scraper Entry Point
=============================================
Pipeline:
  1) Fetch ALL domains + roles from API (dynamic, never hardcoded)
  2) Discover Workday + iCIMS portals concurrently
  3) Search ALL keywords across ALL portals concurrently
  4) Insert EACH job into DB immediately (one by one, per record commit)
  5) CSV backup in parallel

Usage:
  python main.py                # All domains, all roles
  python main.py --limit 10     # First 10 domains (testing)

Environment:
  SUPABASE_URL   → Your Supabase project URL
  SUPABASE_KEY   → Your Supabase anon key
"""

import asyncio
import csv
import io
import logging
import os
import sys
import time
from typing import Dict

import httpx

from scraper import config, db
from scraper.roles import fetch_roles, build_search_pairs
from scraper.workday import discover as discover_wd, process_portal as process_wd
from scraper.icims import discover as discover_ic, process_portal as process_ic

# ─── Logging ───

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger("scraper")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ─── CSV Writer (matches your exact DB columns) ───

_csv_lock = asyncio.Lock()
_csv_count = 0


async def csv_writer(job: Dict):
    global _csv_count
    async with _csv_lock:
        try:
            exists = os.path.isfile(config.CSV_OUTPUT) and os.path.getsize(config.CSV_OUTPUT) > 0
            with open(config.CSV_OUTPUT, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=config.DB_FIELDS, extrasaction="ignore")
                if not exists:
                    w.writeheader()
                w.writerow({k: job.get(k, "") for k in config.DB_FIELDS})
            _csv_count += 1
        except Exception:
            pass


# ─── Main Pipeline ───

async def run(limit: int = 0):
    start = time.time()

    # 1. Init DB
    await db.init()

    # 2. Fetch ALL roles from API
    roles = fetch_roles()
    if not roles:
        log.error("No roles from API. Aborting.")
        return
    if limit > 0:
        roles = roles[:limit]

    search_pairs = build_search_pairs(roles)
    log.info(f"🔍 {len(roles)} domains → {len(search_pairs)} keyword combos")

    # 3. Load portal URLs
    if not os.path.isfile(config.PORTALS_FILE):
        log.error(f"Portals file missing: {config.PORTALS_FILE}")
        return

    with open(config.PORTALS_FILE) as f:
        urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    wd_urls = [u for u in urls if "myworkdayjobs.com" in u.lower()]
    ic_urls = [u for u in urls if "icims.com" in u.lower()]
    log.info(f"📋 Portals: {len(wd_urls)} Workday + {len(ic_urls)} iCIMS")

    # 4. Create CSV
    for attempt in range(5):
        fname = config.CSV_OUTPUT if attempt == 0 else config.CSV_OUTPUT.replace(".csv", f"_{attempt}.csv")
        try:
            with open(fname, "w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=config.DB_FIELDS).writeheader()
            config.CSV_OUTPUT = fname
            log.info(f"📄 CSV: {config.CSV_OUTPUT}")
            break
        except PermissionError:
            continue

    # 5. Discover ALL portals concurrently
    log.info("⚡ Phase 1: Discovering portals...")
    async with httpx.AsyncClient(follow_redirects=True, verify=False, timeout=config.REQUEST_TIMEOUT) as client:
        wd_tasks = [discover_wd(u, client) for u in wd_urls]
        ic_tasks = [discover_ic(u, client) for u in ic_urls]
        all_disc = await asyncio.gather(*(wd_tasks + ic_tasks), return_exceptions=True)
        wd_portals = [r for r in all_disc[:len(wd_urls)] if isinstance(r, dict)]
        ic_portals = [r for r in all_disc[len(wd_urls):] if isinstance(r, dict)]

    log.info(f"✅ Active: {len(wd_portals)} Workday + {len(ic_portals)} iCIMS")

    # 6. Search ALL portals concurrently
    log.info(f"⚡ Phase 2: Searching {len(search_pairs)} keywords across {len(wd_portals) + len(ic_portals)} portals...")

    async with httpx.AsyncClient(
        follow_redirects=True, verify=False, timeout=config.REQUEST_TIMEOUT,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
    ) as client:
        portal_sem = asyncio.Semaphore(config.PORTAL_CONCURRENCY)

        async def run_wd(p):
            async with portal_sem:
                await process_wd(p, search_pairs, client, csv_writer)

        async def run_ic(p):
            async with portal_sem:
                await process_ic(p, search_pairs, client, csv_writer)

        all_tasks = [run_wd(p) for p in wd_portals] + [run_ic(p) for p in ic_portals]
        await asyncio.gather(*all_tasks, return_exceptions=True)

    # 7. Done
    elapsed = time.time() - start
    stats = db.get_stats()
    await db.close()

    log.info(f"""
{'='*60}
  ⚡ SCRAPING COMPLETE
{'='*60}
  Portals      : {len(wd_portals)} Workday + {len(ic_portals)} iCIMS
  Domains      : {len(roles)}
  Keywords     : {len(search_pairs)}
  Jobs found   : {stats['found']}
  DB inserted  : {stats['inserted']}
  DB duplicates: {stats['duplicates']}
  DB failed    : {stats['failed']}
  CSV backup   : {_csv_count} rows → {config.CSV_OUTPUT}
  Time         : {elapsed:.0f}s ({elapsed/60:.1f} min)
{'='*60}""")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US Job Scraper - Workday + iCIMS")
    parser.add_argument("--limit", type=int, default=0, help="Limit role domains (0=all)")
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit))


if __name__ == "__main__":
    main()
