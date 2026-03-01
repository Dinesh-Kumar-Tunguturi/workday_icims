"""
iCIMS ATS Scraper
- Discovers portal type (modern tiles, legacy, jibe API, custom)
- Searches keywords
- Extracts experience + all fields
- Inserts EACH job into DB immediately
"""
import asyncio
import json
import re
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

from . import config
from .utils import strip_html, is_us_location, is_recent, now_iso
from .experience import extract_experience
from . import db

log = logging.getLogger("scraper.icims")


async def discover(url: str, client: httpx.AsyncClient) -> Optional[Dict]:
    """Discover iCIMS portal type."""
    parsed = urlparse(url.rstrip("/"))
    domain = parsed.netloc
    company = domain.split(".")[0].replace("careers-", "").replace("jobs-", "")
    
    # Check if it's a known Jibe API by guessing the root domain
    # Example: careers-amd.icims.com usually means careers.amd.com is the front-end
    root_domain = domain.replace("-", ".").replace("icims.com", company + ".com")
    
    jibe_api_urls = [
        f"https://careers.{company}.com/api/jobs",
        f"https://jobs.{company}.com/api/jobs",
        f"https://careers.{company}.com/search-api/jobs",
    ]
    
    for jibe_url in jibe_api_urls:
        try:
            r = await client.get(jibe_url, headers={"User-Agent": config.USER_AGENT})
            if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                return {"base": jibe_url, "company": company, "type": "jibe_api", "final_url": ""}
        except Exception:
            pass

    # Standard iCIMS checks
    base = f"https://{domain}"
    for try_url in [f"{base}/jobs/search", url]:
        try:
            resp = await client.get(
                try_url, headers={"User-Agent": config.USER_AGENT, "Accept": "text/html"},
                follow_redirects=True,
            )
            if resp.status_code != 200:
                continue
            text = resp.text
            final_domain = urlparse(str(resp.url)).netloc

            if "tile-search-results" in text or "job-tile" in text:
                return {"base": base, "company": company, "type": "modern", "final_url": str(resp.url)}
            if "iCIMS_JobListing" in text or "iCIMS_MainWrapper" in text:
                return {"base": base, "company": company, "type": "legacy", "final_url": str(resp.url)}
            if "application/ld+json" in text:
                return {"base": base, "company": company, "type": "ldjson", "final_url": str(resp.url)}
            if final_domain != domain:
                return {"base": f"https://{final_domain}", "company": company, "type": "custom", "final_url": str(resp.url)}
        except Exception:
            pass
    return None


async def search_and_insert(
    portal: Dict, keyword: str, domain_name: str,
    client: httpx.AsyncClient, sem: asyncio.Semaphore, csv_writer=None,
) -> int:
    async with sem:
        ptype = portal["type"]
        if ptype == "jibe_api":
            return await _search_jibe_api(portal, keyword, domain_name, client, csv_writer)
        elif ptype == "modern":
            return await _search_modern(portal, keyword, domain_name, client, csv_writer)
        elif ptype in ("legacy", "ldjson"):
            return await _search_legacy(portal, keyword, domain_name, client, csv_writer)
        elif ptype == "custom":
            return await _search_custom(portal, keyword, domain_name, client, csv_writer)
        return 0


async def _search_jibe_api(portal, keyword, domain_name, client, csv_writer) -> int:
    count = 0
    api_url = portal["base"]
    company = portal["company"]
    
    offset = 0
    while True:
        page_count = 0
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = await client.get(
                    api_url, 
                    params={"q": keyword, "limit": 100, "offset": offset},
                    headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"}
                )
                if resp.status_code != 200:
                    break

                data = resp.json()
                jobs_list = data.get("jobs", [])
                
                for item in jobs_list:
                    jp = item.get("data", {})
                    if not jp:
                        continue
                        
                    posted = jp.get("posted_date", "") or jp.get("update_date", "") or jp.get("create_date", "")
                    if not is_recent(posted):
                        continue
                        
                    loc = jp.get("full_location", "") or jp.get("location_name", "") or jp.get("city", "")

                    desc = strip_html(jp.get("description", ""))
                    url = jp.get("apply_url", "")
                    if not url:
                        # Construct URL if possible
                        url = api_url.replace("/api/jobs", f"/jobs/{jp.get('slug','')}")
                        
                    job = {
                        "domain": domain_name,
                        "role_searched": keyword,
                        "source_ats": "icims",
                        "company": company,
                        "job_id": jp.get("req_id", ""),
                        "title": jp.get("title", ""),
                        "job_url": url,
                        "description": desc,
                        "location": loc,
                        "city": jp.get("city", ""),
                        "state": jp.get("state", ""),
                        "country": jp.get("country", ""),
                        "department": jp.get("category", ""),
                        "posted_date": posted,
                        "employment_type": jp.get("employment_type", ""),
                        "time_type": jp.get("employment_type", ""),
                        "experience_level": extract_experience(desc) or "Not Specified",
                        "company_logo": jp.get("hiring_organization_logo", ""),
                        "remote_eligible": "Yes" if "remote" in loc.lower() else "",
                    }

                    result = await db.insert_job(job)
                    if csv_writer and result != "duplicate":
                        await csv_writer(job)
                    page_count += 1
                break
            except Exception:
                if attempt < config.MAX_RETRIES - 1:
                    await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))
        
        count += page_count
        if page_count == 0 or page_count < 100:
            break
        offset += 100
        if offset >= 2000:
            break
    return count


async def _search_modern(portal, keyword, domain_name, client, csv_writer) -> int:
    count = 0
    base, company = portal["base"], portal["company"]

    startrow = 0
    while True:
        page_count = 0
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = await client.get(
                    f"{base}/tile-search-results?q={keyword}&sortColumn=referencedate&sortDirection=desc&startrow={startrow}",
                    headers={"User-Agent": config.USER_AGENT, "X-Requested-With": "XMLHttpRequest"},
                )
                if resp.status_code != 200:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                tiles = soup.find_all("li", class_=lambda c: c and "job-tile" in c)
                for tile in tiles:
                    job = _parse_tile(tile, base, domain_name, keyword, company)
                    if not job:
                        continue
                    if not is_recent(job.get("posted_date", "")):
                        continue

                    job["experience_level"] = extract_experience(job.get("description", ""))
                    if not job["experience_level"]:
                        job["experience_level"] = "Not Specified"

                    result = await db.insert_job(job)
                    if csv_writer and result != "duplicate":
                        await csv_writer(job)
                    page_count += 1
                break
            except Exception:
                if attempt < config.MAX_RETRIES - 1:
                    await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))
        
        count += page_count
        if page_count == 0:
            break
        startrow += 20  # iCIMS typically uses 20 or 15. We advance by 20 to be safe
        if startrow >= 1000:  # Safety cap
            break
            
    return count


async def _search_legacy(portal, keyword, domain_name, client, csv_writer) -> int:
    count = 0
    base, company = portal["base"], portal["company"]

    pr = 0
    while True:
        page_count = 0
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = await client.get(
                    f"{base}/jobs/search", params={"q": keyword, "in_iframe": 1, "pr": pr},
                    headers={"User-Agent": config.USER_AGENT},
                )
                if resp.status_code != 200:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        ld = json.loads(script.string)
                        if not isinstance(ld, dict) or ld.get("@type") != "ItemList":
                            continue
                        for item in ld.get("itemListElement", []):
                            jp = item.get("item", item)
                            posted = jp.get("datePosted", "")
                            if not is_recent(posted):
                                continue
                            loc = _ld_location(jp)

                            org = jp.get("hiringOrganization", {})
                            desc = strip_html(jp.get("description", ""))

                            job = {
                                "domain": domain_name,
                                "role_searched": keyword,
                                "source_ats": "icims",
                                "company": org.get("name", company) if isinstance(org, dict) else company,
                                "job_id": _ld_id(jp),
                                "title": jp.get("title", jp.get("name", "")),
                                "job_url": jp.get("url", ""),
                                "description": desc,
                                "location": loc,
                                "posted_date": posted,
                                "employment_type": jp.get("employmentType", ""),
                                "time_type": jp.get("employmentType", ""),
                                "experience_level": extract_experience(desc) or "Not Specified",
                                "company_logo": org.get("logo", "") if isinstance(org, dict) else "",
                                "salary": _ld_salary(jp),
                                "remote_eligible": "Yes" if "remote" in loc.lower() else "",
                            }

                            jl = jp.get("jobLocation", {})
                            if isinstance(jl, dict):
                                a = jl.get("address", {})
                                if isinstance(a, dict):
                                    job["city"] = a.get("addressLocality", "")
                                    job["state"] = a.get("addressRegion", "")
                                    job["country"] = a.get("addressCountry", "")

                            result = await db.insert_job(job)
                            if csv_writer and result != "duplicate":
                                await csv_writer(job)
                            page_count += 1
                    except Exception:
                        pass
                break
            except Exception:
                if attempt < config.MAX_RETRIES - 1:
                    await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))
        
        count += page_count
        if page_count == 0:
            break
        pr += 1
        if pr > 50:
            break
            
    return count


async def _search_custom(portal, keyword, domain_name, client, csv_writer) -> int:
    count = 0
    final_url = portal.get("final_url", "")
    company = portal["company"]

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = await client.get(
                final_url, params={"q": keyword},
                headers={"User-Agent": config.USER_AGENT}, follow_redirects=True,
            )
            if resp.status_code != 200:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string)
                    items = []
                    if isinstance(ld, dict):
                        if ld.get("@type") == "ItemList":
                            items = ld.get("itemListElement", [])
                        elif ld.get("@type") == "JobPosting":
                            items = [ld]
                    elif isinstance(ld, list):
                        items = ld

                    for item in items:
                        jp = item.get("item", item) if isinstance(item, dict) else item
                        if not isinstance(jp, dict):
                            continue
                        posted = jp.get("datePosted", "")
                        if not is_recent(posted):
                            continue
                        loc = _ld_location(jp)

                        org = jp.get("hiringOrganization", {})
                        desc = strip_html(jp.get("description", ""))

                        job = {
                            "domain": domain_name,
                            "role_searched": keyword,
                            "source_ats": "icims",
                            "company": org.get("name", company) if isinstance(org, dict) else company,
                            "job_id": _ld_id(jp),
                            "title": jp.get("title", jp.get("name", "")),
                            "job_url": jp.get("url", ""),
                            "description": desc,
                            "location": loc,
                            "posted_date": posted,
                            "employment_type": jp.get("employmentType", ""),
                            "time_type": jp.get("employmentType", ""),
                            "experience_level": extract_experience(desc) or "Not Specified",
                            "company_logo": org.get("logo", "") if isinstance(org, dict) else "",
                            "salary": _ld_salary(jp),
                            "remote_eligible": "Yes" if "remote" in loc.lower() else "",
                        }

                        jl = jp.get("jobLocation", {})
                        if isinstance(jl, dict):
                            a = jl.get("address", {})
                            if isinstance(a, dict):
                                job["city"] = a.get("addressLocality", "")
                                job["state"] = a.get("addressRegion", "")
                                job["country"] = a.get("addressCountry", "")

                        result = await db.insert_job(job)
                        if csv_writer and result != "duplicate":
                            await csv_writer(job)
                        count += 1
                except Exception:
                    pass
            break
        except Exception:
            if attempt < config.MAX_RETRIES - 1:
                await asyncio.sleep(config.RETRY_BACKOFF * (attempt + 1))
    return count


# ─── Helpers ───

def _parse_tile(tile, base, domain_name, keyword, company) -> Optional[Dict]:
    try:
        cls = " ".join(tile.get("class", []))
        id_m = re.search(r"job-id-(\d+)", cls)
        jid = id_m.group(1) if id_m else ""
        data_url = tile.get("data-url", "")
        url = urljoin(base, data_url) if data_url else ""
        title = ""
        a = tile.find("a", class_="jobTitle-link")
        if a:
            title = a.get_text(strip=True)
        loc = ""
        loc_div = tile.find("div", id=re.compile(r"section-location-value$"))
        if loc_div:
            loc = loc_div.get_text(strip=True)
        posted = ""
        dd = tile.find("div", id=re.compile(r"section.*date.*value"))
        if dd:
            posted = dd.get_text(strip=True)
        return {
            "domain": domain_name, "role_searched": keyword, "source_ats": "icims",
            "company": company, "job_id": jid, "title": title, "job_url": url,
            "location": loc, "posted_date": posted,
        }
    except Exception:
        return None


def _ld_location(jp):
    loc = jp.get("jobLocation", {})
    if isinstance(loc, dict):
        a = loc.get("address", {})
        if isinstance(a, dict):
            return ", ".join(p for p in [a.get("addressLocality", ""), a.get("addressRegion", ""), a.get("addressCountry", "")] if p)
    elif isinstance(loc, list):
        parts = []
        for l in loc[:3]:
            a = l.get("address", {}) if isinstance(l, dict) else {}
            parts.append(", ".join(p for p in [a.get("addressLocality", ""), a.get("addressRegion", "")] if p))
        return " | ".join(parts)
    return ""


def _ld_id(jp):
    ident = jp.get("identifier", "")
    return ident.get("value", "") if isinstance(ident, dict) else str(ident)


def _ld_salary(jp):
    sal = jp.get("baseSalary", {})
    if isinstance(sal, dict):
        val = sal.get("value", {})
        if isinstance(val, dict):
            mn, mx, u = val.get("minValue", ""), val.get("maxValue", ""), val.get("unitText", "")
            if mn and mx: return f"${mn}-${mx}/{u}"
            if mn: return f"${mn}/{u}"
    return ""


async def process_portal(portal: Dict, search_pairs: List[tuple],
                          client: httpx.AsyncClient, csv_writer=None):
    company = portal["company"]
    sem = asyncio.Semaphore(config.SEARCH_PER_PORTAL)
    tasks = [search_and_insert(portal, kw, dn, client, sem, csv_writer) for dn, kw in search_pairs]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total = sum(r for r in results if isinstance(r, int))
    if total > 0:
        log.info(f"  [iCIMS] {company}: {total} jobs ✅")
    else:
        log.info(f"  [iCIMS] {company}: 0 recent jobs")
