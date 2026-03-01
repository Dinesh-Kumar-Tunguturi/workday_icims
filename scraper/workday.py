"""
Workday ATS Scraper
- Discovers portal API endpoints
- Searches keywords via JSON API
- Fetches job detail pages for complete field extraction
- Extracts experience
- Inserts EACH job into DB immediately (one by one)
"""
import asyncio
import re
import logging
from typing import Dict, List, Optional

import httpx

from . import config
from .utils import strip_html, is_us_location, is_recent, now_iso
from .experience import extract_experience
from . import db

log = logging.getLogger("scraper.workday")


async def discover(url: str, client: httpx.AsyncClient) -> Optional[Dict]:
    """Discover Workday portal API endpoint from a career page URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url.rstrip("/"))
    domain = parsed.netloc
    parts = [p for p in parsed.path.split("/") if p and not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
    site = parts[0] if parts else "careers"
    tenant = domain.split(".")[0]

    for page_url in [f"https://{domain}/en-US/{site}", f"https://{domain}/{site}"]:
        try:
            resp = await client.get(page_url, headers={"User-Agent": config.USER_AGENT, "Accept": "text/html"})
            if resp.status_code == 200:
                html = resp.text
                m = re.search(r'"siteId"\s*:\s*"([^"]+)"', html)
                if m:
                    site = m.group(1)
                m = re.search(r'"tenant"\s*:\s*"([^"]+)"', html)
                if m:
                    tenant = m.group(1)
                api = f"https://{domain}/wday/cxs/{tenant}/{site}"
                return {"domain": domain, "tenant": tenant, "site": site, "api": api}
        except Exception:
            pass

    # Fallback
    api = f"https://{domain}/wday/cxs/{tenant}/{site}"
    try:
        resp = await client.post(
            f"{api}/jobs",
            json={"limit": 1, "offset": 0, "searchText": ""},
            headers={"User-Agent": config.USER_AGENT, "Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            return {"domain": domain, "tenant": tenant, "site": site, "api": api}
    except Exception:
        pass
    return None


async def search_and_insert(
    portal: Dict, keyword: str, domain_name: str,
    client: httpx.AsyncClient, sem: asyncio.Semaphore, csv_writer=None,
) -> int:
    """
    Search one keyword on Workday portal.
    For EACH job found:
      1) Fetch detail page → extract ALL fields  
      2) Extract experience
      3) Insert into DB immediately
      4) Append to CSV backup
    """
    async with sem:
        api_url = f"{portal['api']}/jobs"
        count = 0
        offset = 0

        while offset < config.MAX_PAGES_PER_KEYWORD * config.WD_PAGE_SIZE:
            payload = {
                "appliedFacets": {}, "limit": config.WD_PAGE_SIZE,
                "offset": offset, "searchText": keyword,
            }

            resp = None
            for attempt in range(config.MAX_RETRIES):
                try:
                    resp = await client.post(api_url, json=payload, headers={
                        "User-Agent": config.USER_AGENT,
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Referer": f"https://{portal['domain']}/en-US/{portal['site']}",
                    })
                    if resp.status_code == 422:
                        payload["locale"] = "en-US"
                        resp = await client.post(api_url, json=payload, headers={
                            "User-Agent": config.USER_AGENT,
                            "Content-Type": "application/json",
                        })
                    break
                except Exception:
                    if attempt < config.MAX_RETRIES - 1:
                        await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))

            if not resp or resp.status_code != 200:
                break

            postings = resp.json().get("jobPostings", [])
            if not postings:
                break

            old_count = 0
            for item in postings:
                posted = item.get("postedOn", "")
                if not is_recent(posted):
                    old_count += 1
                    continue
                loc = item.get("locationsText", "")

                ext = item.get("externalPath", "")
                job_url = f"https://{portal['domain']}/en-US/{portal['site']}{ext}"

                # ── Build job with YOUR EXACT column names ──
                job = {
                    "domain": domain_name,
                    "role_searched": keyword,
                    "source_ats": "workday",
                    "company": portal["tenant"],
                    "job_id": item.get("bulletinId") or item.get("jobReqId") or "",
                    "title": item.get("title", ""),
                    "job_url": job_url,
                    "location": loc,
                    "posted_date": posted,
                    "employment_type": item.get("timeType", ""),
                    "time_type": item.get("timeType", ""),
                    "experience_level": "Not Specified",
                }

                # ── Fetch detail page for ALL fields ──
                try:
                    dr = await client.get(f"{portal['api']}{ext}",
                        headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"})
                    if dr.status_code == 200:
                        info = dr.json().get("jobPostingInfo", {})
                        desc = strip_html(info.get("jobDescription", ""))
                        job["description"] = desc
                        job["title"] = info.get("title", job["title"])
                        job["location"] = info.get("location", job["location"])
                        job["company"] = info.get("company", job["company"])
                        job["posted_date"] = info.get("postedOn", job["posted_date"])
                        job["time_type"] = info.get("timeType", "")
                        job["employment_type"] = info.get("timeType", "")
                        job["job_id"] = info.get("bulletinId") or info.get("jobReqId") or job["job_id"]

                        # Experience extraction
                        job["experience_level"] = extract_experience(desc)

                        # Country
                        co = info.get("country", {})
                        job["country"] = co.get("descriptor", "") if isinstance(co, dict) else str(co) if co else ""

                        # City / State
                        lt = job.get("location", "")
                        if "," in lt:
                            ps = [p.strip() for p in lt.split(",")]
                            job["city"] = ps[0]
                            job["state"] = ps[1] if len(ps) > 1 else ""

                        # Department
                        cats = info.get("subCategory", [])
                        if cats and isinstance(cats, list):
                            job["department"] = ", ".join(
                                c.get("descriptor", "") if isinstance(c, dict) else str(c) for c in cats[:3]
                            )

                        # Job category
                        fam = info.get("jobFamily", [])
                        if fam and isinstance(fam, list):
                            job["job_category"] = ", ".join(
                                f.get("descriptor", "") if isinstance(f, dict) else str(f) for f in fam[:3]
                            )

                        # Salary
                        sal = info.get("salary", "") or info.get("payRange", "")
                        if sal:
                            job["salary"] = str(sal)

                        # Company logo
                        img = info.get("imageUrl", "")
                        if img:
                            job["company_logo"] = f"https://{portal['domain']}{img}" if not img.startswith("http") else img

                        # Remote
                        job["remote_eligible"] = "Yes" if "remote" in lt.lower() else ""
                except Exception:
                    pass

                # Ensure experience never empty
                if not job.get("experience_level"):
                    job["experience_level"] = "Not Specified"

                # ═══ INSERT INTO DB IMMEDIATELY ═══
                result = await db.insert_job(job)

                # ═══ CSV BACKUP ═══
                if csv_writer and result != "duplicate":
                    await csv_writer(job)

                count += 1

            if old_count >= len(postings): # Only break if ALL jobs are old, ATS often mix orders
                break
            if len(postings) < config.WD_PAGE_SIZE:
                break
            offset += config.WD_PAGE_SIZE

        return count


async def process_portal(portal: Dict, search_pairs: List[tuple],
                          client: httpx.AsyncClient, csv_writer=None):
    """Process all keywords for one Workday portal."""
    tenant = portal["tenant"]
    sem = asyncio.Semaphore(config.SEARCH_PER_PORTAL)
    tasks = [search_and_insert(portal, kw, dn, client, sem, csv_writer) for dn, kw in search_pairs]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total = sum(r for r in results if isinstance(r, int))
    if total > 0:
        log.info(f"  [WD] {tenant}: {total} jobs ✅")
    else:
        log.info(f"  [WD] {tenant}: 0 recent jobs")
