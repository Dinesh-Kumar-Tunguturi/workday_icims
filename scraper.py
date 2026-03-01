"""
US Job Scraper - Comprehensive Workday + iCIMS Scraper
=======================================================
Searches ALL role keywords from the Apply-Wizz API across Workday/iCIMS portals.
Extracts every available field (description, company, logo, salary, etc.)
Outputs US-only jobs to us_jobs.csv.

Usage:
  python scraper.py                         # Search all roles, US jobs only
  python scraper.py --file links.txt        # Custom links file
  python scraper.py --limit 50             # Limit to first N role domains
"""

import asyncio, csv, json, logging, os, re, sys, time, random, io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from html import unescape

import httpx
from bs4 import BeautifulSoup

# ═══════════════════════════ CONFIG ═══════════════════════════

ROLES_API = "https://dashboard.apply-wizz.com/api/all-job-roles/"
DEFAULT_LINKS_FILE = "career_links.txt"
OUTPUT_FILE = "us_jobs_recent.csv"
PORTAL_CONCURRENCY = 10
SEARCH_CONCURRENCY = 5
DETAIL_CONCURRENCY = 15
TIMEOUT = 30
SEARCH_LIMIT = 20

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

US_KEYWORDS = [
    "usa", "united states", "u.s.", "california", "texas", "florida",
    "new york", "san francisco", "santa clara", "charlotte", "tampa",
    "austin", "seattle", "atlanta", "chicago", "boston", "denver",
    "portland", "san jose", "los angeles", "washington", "philadelphia",
    "phoenix", "san diego", "dallas", "houston", "minneapolis", "detroit",
    "raleigh", "nashville", "columbus", "pittsburgh", "salt lake",
    "richmond", "indianapolis", "kansas city", "milwaukee", "baltimore",
    "new jersey", "connecticut", "massachusetts", "virginia", "maryland",
    "georgia", "north carolina", "south carolina", "illinois", "ohio",
    "michigan", "pennsylvania", "tennessee", "arizona", "colorado",
    "oregon", "minnesota", "wisconsin", "missouri", "iowa", "kentucky",
    "alabama", "louisiana", "utah", "nevada", "arkansas", "mississippi",
    "nebraska", "idaho", "hawaii", "maine", "montana", "delaware",
    "new hampshire", "rhode island", "vermont", "wyoming", "oklahoma",
    "new mexico", "west virginia", "south dakota", "north dakota",
    "irvine", "irving", "sunnyvale", "cupertino", "mountain view",
    "redmond", "arlington", "plano", "frisco", "scottsdale", "tempe",
    "bellevue", "kirkland", "palo alto", "menlo park", "redwood city",
    "foster city", "san mateo", "fremont", "milpitas", "santa monica",
    "burbank", "pasadena", "long beach", "oakland", "sacramento",
    "san antonio", "fort worth", "jacksonville", "indianapolis",
    "st. louis", "st louis", "cincinnati", "cleveland", "orlando",
    "tampa", "miami", "boca raton", "jersey city", "hoboken", "newark",
    "stamford", "hartford", "wilmington", "morristown", "parsippany",
]

CSV_FIELDS = [
    "domain", "role_searched", "source_ats", "company", "job_id", "title",
    "job_url", "description", "location", "city", "state", "country",
    "department", "job_category", "employment_type", "time_type",
    "posted_date", "salary", "experience_level", "company_logo",
    "remote_eligible", "scraped_at",
]

# ═══════════════════════════ LOGGING ═══════════════════════════

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

# ═══════════════════════════ HELPERS ═══════════════════════════

def strip_html(html_str: str) -> str:
    if not html_str:
        return ""
    text = BeautifulSoup(html_str, "html.parser").get_text(separator=" ", strip=True)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:8000] if len(text) > 8000 else text

def is_us_location(location: str, country: str = "") -> bool:
    combined = f"{location} {country}".lower()
    combined = re.sub(r'[^a-z0-9 .]', ' ', combined)
    if "remote" in combined and not any(x in combined for x in ["india", "uk", "canada", "europe", "germany", "ireland", "australia"]):
        return True
    return any(kw in f" {combined} " or combined.startswith(kw) or combined.endswith(kw) for kw in US_KEYWORDS + ["us"])

def is_recent(date_str: str) -> bool:
    if not date_str:
        return False
    d = str(date_str).lower()
    if any(x in d for x in ["yesterday", "today", "hour", "minute", "second", "just now"]):
        return True
    m = re.search(r'(\d+)\+?\s*day', d)
    if m:
        return int(m.group(1)) <= 1
    if "a day" in d or "one day" in d:
        return True
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo:
            now = datetime.now(dt.tzinfo)
        else:
            now = datetime.utcnow()
        return abs((now - dt).total_seconds()) <= 86400
    except (ValueError, TypeError):
        pass
    return False

def fetch_roles() -> List[Dict]:
    """Fetch all role domains from the API. Returns list of {name, keywords}."""
    log.info("Fetching role domains from API...")
    try:
        resp = httpx.get(ROLES_API, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
        roles = []
        for role in data:
            name = role.get("name", "").strip()
            if not name:
                continue
            search_terms = set()
            search_terms.add(name)
            for alt in role.get("alternateRoles", []):
                alt = alt.strip()
                if alt and len(alt) > 1:
                    search_terms.add(alt)
            roles.append({"name": name, "search_terms": list(search_terms)})
        log.info(f"[OK] Loaded {len(roles)} role domains with {sum(len(r['search_terms']) for r in roles)} total search terms")
        return roles
    except Exception as e:
        log.error(f"[FAIL] {e}")
        return []

# ═══════════════════════════ CSV WRITER ═══════════════════════════

_csv_lock = asyncio.Lock()
_seen_urls: set = set()
_job_count = 0

async def save_job_csv(job: Dict):
    global _job_count
    if not job or not job.get("job_url"):
        return
    async with _csv_lock:
        if job["job_url"] in _seen_urls:
            return
        _seen_urls.add(job["job_url"])
        exists = os.path.isfile(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0
        for attempt in range(3):
            try:
                with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
                    w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
                    if not exists:
                        w.writeheader()
                        exists = True
                    w.writerow({k: job.get(k, "") for k in CSV_FIELDS})
                _job_count += 1
                return
            except PermissionError:
                await asyncio.sleep(1)

# ═══════════════════════════ WORKDAY ═══════════════════════════

async def discover_workday(url: str, client: httpx.AsyncClient) -> Optional[Dict]:
    """Discover Workday portal API info. Returns dict or None."""
    parsed = urlparse(url.rstrip("/"))
    domain = parsed.netloc
    path_parts = [p for p in parsed.path.split("/") if p and not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
    site = path_parts[0] if path_parts else "careers"
    tenant = domain.split(".")[0]

    for page_url in [f"https://{domain}/en-US/{site}", f"https://{domain}/{site}", f"https://{domain}"]:
        try:
            resp = await client.get(page_url, headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html"})
            if resp.status_code == 200:
                html = resp.text
                m = re.search(r'"siteId"\s*:\s*"([^"]+)"', html)
                if m: site = m.group(1)
                m = re.search(r'"tenant"\s*:\s*"([^"]+)"', html)
                if m: tenant = m.group(1)
                api_base = f"https://{domain}/wday/cxs/{tenant}/{site}"
                log.info(f"[WD] {tenant}/{site} discovered [OK]")
                return {"domain": domain, "tenant": tenant, "site": site, "api_base": api_base}
        except Exception:
            pass
    
    # Fallback: try API directly
    api_base = f"https://{domain}/wday/cxs/{tenant}/{site}"
    try:
        test_resp = await client.post(f"{api_base}/jobs", json={"limit": 1, "offset": 0, "searchText": "", "appliedFacets": {}},
                                       headers={"User-Agent": random.choice(USER_AGENTS), "Content-Type": "application/json", "Accept": "application/json"})
        if test_resp.status_code == 200:
            log.info(f"[WD] {tenant}/{site} API direct [OK]")
            return {"domain": domain, "tenant": tenant, "site": site, "api_base": api_base}
    except Exception:
        pass
    return None

async def search_workday_keyword(portal: Dict, keyword: str, domain_name: str,
                                  client: httpx.AsyncClient, sem: asyncio.Semaphore) -> List[Dict]:
    """Search one keyword on a Workday portal. Returns list of US job dicts."""
    async with sem:
        api_url = f"{portal['api_base']}/jobs"
        jobs = []
        offset = 0
        while offset < 500:
            payload = {"appliedFacets": {}, "limit": SEARCH_LIMIT, "offset": offset, "searchText": keyword}
            try:
                resp = await client.post(api_url, json=payload, headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Content-Type": "application/json", "Accept": "application/json",
                    "Referer": f"https://{portal['domain']}/en-US/{portal['site']}",
                    "Origin": f"https://{portal['domain']}"})
                if resp.status_code == 422:
                    payload["locale"] = "en-US"
                    resp = await client.post(api_url, json=payload, headers={
                        "User-Agent": random.choice(USER_AGENTS), "Content-Type": "application/json", "Accept": "application/json"})
                if resp.status_code != 200:
                    break
                data = resp.json()
                postings = data.get("jobPostings", [])
                if not postings:
                    break
                for item in postings:
                    posted_date = item.get("postedOn", "")
                    if not is_recent(posted_date):
                        continue
                    loc = item.get("locationsText", "")
                    if not is_us_location(loc):
                        continue
                    ext_path = item.get("externalPath", "")
                    job_url = f"https://{portal['domain']}/en-US/{portal['site']}{ext_path}"
                    if job_url in _seen_urls:
                        continue
                    jobs.append({
                        "domain": domain_name, "role_searched": keyword,
                        "source_ats": "workday", "company": portal["tenant"],
                        "job_id": item.get("bulletinId") or item.get("jobReqId") or "",
                        "title": item.get("title", ""), "job_url": job_url,
                        "location": loc, "posted_date": item.get("postedOn", ""),
                        "time_type": item.get("timeType", ""),
                        "employment_type": item.get("timeType", ""),
                        "_ext_path": ext_path, "_portal": portal,
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
                if len(postings) < SEARCH_LIMIT:
                    break
                offset += SEARCH_LIMIT
                await asyncio.sleep(0.3)
            except Exception:
                break
        return jobs

async def fetch_workday_detail(job: Dict, client: httpx.AsyncClient, sem: asyncio.Semaphore) -> Dict:
    """Fetch full detail for a Workday job."""
    async with sem:
        portal = job.get("_portal", {})
        ext_path = job.get("_ext_path", "")
        if not portal or not ext_path:
            return job
        detail_url = f"{portal['api_base']}{ext_path}"
        try:
            resp = await client.get(detail_url, headers={
                "User-Agent": random.choice(USER_AGENTS), "Accept": "application/json",
                "Referer": f"https://{portal['domain']}/en-US/{portal['site']}"})
            if resp.status_code != 200:
                return job
            data = resp.json()
            info = data.get("jobPostingInfo", {})
            job["title"] = info.get("title", job.get("title", ""))
            job["description"] = strip_html(info.get("jobDescription", ""))
            job["location"] = info.get("location", job.get("location", ""))
            country_obj = info.get("country", {})
            if isinstance(country_obj, dict):
                job["country"] = country_obj.get("descriptor", "")
            elif isinstance(country_obj, str):
                job["country"] = country_obj
            addl = info.get("additionalLocations", [])
            if addl and isinstance(addl, list):
                if isinstance(addl[0], str):
                    parts = addl[0].split(",")
                    if len(parts) >= 2:
                        job["state"] = parts[-2].strip() if len(parts) >= 3 else ""
                        job["city"] = parts[-3].strip() if len(parts) >= 3 else parts[0].strip()
            loc_text = job.get("location", "")
            if "," in loc_text and not job.get("city"):
                parts = [p.strip() for p in loc_text.split(",")]
                job["city"] = parts[0] if parts else ""
                job["state"] = parts[1] if len(parts) > 1 else ""
            job["company"] = info.get("company", job.get("company", ""))
            job["posted_date"] = info.get("postedOn", job.get("posted_date", ""))
            job["time_type"] = info.get("timeType", "")
            job["employment_type"] = info.get("timeType", "")
            job["job_id"] = info.get("bulletinId") or info.get("jobReqId") or job.get("job_id", "")
            sub_cats = info.get("subCategory", [])
            if sub_cats and isinstance(sub_cats, list):
                job["department"] = ", ".join(s.get("descriptor", "") if isinstance(s, dict) else str(s) for s in sub_cats[:3])
            job_fam = info.get("jobFamily", [])
            if job_fam and isinstance(job_fam, list):
                job["job_category"] = ", ".join(f.get("descriptor", "") if isinstance(f, dict) else str(f) for f in job_fam[:3])
            img = info.get("imageUrl", "")
            if img:
                job["company_logo"] = f"https://{portal['domain']}{img}" if not img.startswith("http") else img
            job["remote_eligible"] = "Yes" if "remote" in loc_text.lower() else ""
            start_date = info.get("startDate", "")
            if start_date:
                job["posted_date"] = job.get("posted_date") or start_date
            await asyncio.sleep(0.2)
        except Exception:
            pass
        job.pop("_ext_path", None)
        job.pop("_portal", None)
        await save_job_csv(job)
        return job

# ═══════════════════════════ iCIMS ═══════════════════════════

async def discover_icims(url: str, client: httpx.AsyncClient) -> Optional[Dict]:
    """Discover iCIMS portal type. Returns dict or None."""
    parsed = urlparse(url.rstrip("/"))
    domain = parsed.netloc
    base = f"{parsed.scheme or 'https'}://{domain}"
    company = domain.split(".")[0].replace("careers-", "").replace("jobs-", "")
    
    for try_url in [f"{base}/jobs/search", url]:
        try:
            resp = await client.get(try_url, headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html"})
            if resp.status_code != 200:
                continue
            text = resp.text
            if "tile-search-results" in text or "job-tile" in text:
                log.info(f"[iCIMS] {company} modern [OK]")
                return {"base": base, "company": company, "type": "modern"}
            if "iCIMS_JobListing" in text or "iCIMS_MainWrapper" in text:
                log.info(f"[iCIMS] {company} legacy [OK]")
                return {"base": base, "company": company, "type": "legacy"}
        except Exception:
            pass
    return None

async def search_icims_keyword(portal: Dict, keyword: str, domain_name: str,
                                client: httpx.AsyncClient, sem: asyncio.Semaphore) -> List[Dict]:
    """Search one keyword on an iCIMS portal."""
    async with sem:
        base = portal["base"]
        company = portal["company"]
        jobs = []
        if portal["type"] == "modern":
            api_url = f"{base}/tile-search-results?q={keyword}&sortColumn=referencedate&sortDirection=desc&startrow=0"
            try:
                resp = await client.get(api_url, headers={
                    "User-Agent": random.choice(USER_AGENTS), "Accept": "text/html, */*",
                    "X-Requested-With": "XMLHttpRequest", "Referer": f"{base}/jobs/search"})
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for tile in soup.find_all("li", class_=lambda c: c and "job-tile" in c):
                        job = _parse_icims_tile(tile, base, domain_name, keyword, company)
                        if job and is_us_location(job.get("location", "")):
                            await save_job_csv(job)
                            jobs.append(job)
            except Exception:
                pass
        elif portal["type"] == "legacy":
            try:
                resp = await client.get(f"{base}/jobs/search", params={"q": keyword, "in_iframe": 1},
                                         headers={"User-Agent": random.choice(USER_AGENTS)})
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            ld = json.loads(script.string)
                            if isinstance(ld, dict) and ld.get("@type") == "ItemList":
                                for item in ld.get("itemListElement", []):
                                    jp = item.get("item", item)
                                    posted_date = jp.get("datePosted", "")
                                    if not is_recent(posted_date):
                                        continue
                                    loc = _extract_ld_location(jp)
                                    if is_us_location(loc):
                                        org = jp.get("hiringOrganization", {})
                                        job_dict = {
                                            "domain": domain_name, "role_searched": keyword,
                                            "source_ats": "icims", "company": org.get("name", company) if isinstance(org, dict) else company,
                                            "job_id": _get_ld_id(jp), "title": jp.get("title", jp.get("name", "")),
                                            "job_url": jp.get("url", ""), "description": strip_html(jp.get("description", "")),
                                            "location": loc, "posted_date": jp.get("datePosted", ""),
                                            "employment_type": jp.get("employmentType", ""),
                                            "company_logo": org.get("logo", "") if isinstance(org, dict) else "",
                                            "salary": _extract_salary(jp),
                                            "scraped_at": datetime.utcnow().isoformat()}
                                        await save_job_csv(job_dict)
                                        jobs.append(job_dict)
                        except Exception:
                            pass
            except Exception:
                pass
        return jobs

def _parse_icims_tile(tile, base, domain_name, keyword, company) -> Optional[Dict]:
    try:
        cls = " ".join(tile.get("class", []))
        id_m = re.search(r"job-id-(\d+)", cls)
        job_id = id_m.group(1) if id_m else ""
        data_url = tile.get("data-url", "")
        full_url = urljoin(base, data_url) if data_url else ""
        title = ""
        a = tile.find("a", class_="jobTitle-link")
        if a: title = a.get_text(strip=True)
        location = ""
        loc_div = tile.find("div", id=re.compile(r"section-location-value$"))
        if loc_div: location = loc_div.get_text(strip=True)
        posted = ""
        dd = tile.find("div", id=re.compile(r"section.*date.*value"))
        if dd: posted = dd.get_text(strip=True)
        if not is_recent(posted):
            return None
        return {
            "domain": domain_name, "role_searched": keyword, "source_ats": "icims",
            "company": company, "job_id": job_id, "title": title, "job_url": full_url,
            "location": location, "posted_date": posted,
            "scraped_at": datetime.utcnow().isoformat()}
    except Exception:
        return None

def _extract_ld_location(jp):
    loc = jp.get("jobLocation", {})
    if isinstance(loc, dict):
        addr = loc.get("address", {})
        if isinstance(addr, dict):
            return ", ".join(p for p in [addr.get("addressLocality", ""), addr.get("addressRegion", ""), addr.get("addressCountry", "")] if p)
    elif isinstance(loc, list):
        parts = []
        for l in loc[:3]:
            a = l.get("address", {}) if isinstance(l, dict) else {}
            parts.append(", ".join(p for p in [a.get("addressLocality", ""), a.get("addressRegion", "")] if p))
        return " | ".join(parts)
    return ""

def _get_ld_id(jp):
    ident = jp.get("identifier", "")
    return ident.get("value", "") if isinstance(ident, dict) else str(ident)

def _extract_salary(jp):
    sal = jp.get("baseSalary", {})
    if isinstance(sal, dict):
        val = sal.get("value", {})
        if isinstance(val, dict):
            mn = val.get("minValue", "")
            mx = val.get("maxValue", "")
            unit = val.get("unitText", "")
            if mn and mx: return f"${mn}-${mx}/{unit}"
            if mn: return f"${mn}/{unit}"
    return ""

# ═══════════════════════════ MAIN ═══════════════════════════

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="US Job Scraper")
    parser.add_argument("--file", default=DEFAULT_LINKS_FILE, help="Career links file")
    parser.add_argument("--limit", type=int, default=0, help="Limit role domains to search (0=all)")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        log.error(f"Links file not found: {args.file}")
        return

    with open(args.file, "r") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    log.info(f"Loaded {len(urls)} portal URLs")

    # 1. Fetch roles
    roles = fetch_roles()
    if not roles:
        log.error("No roles fetched. Exiting.")
        return
    if args.limit > 0:
        roles = roles[:args.limit]
    total_terms = sum(len(r["search_terms"]) for r in roles)
    log.info(f"Using {len(roles)} role domains, {total_terms} search terms")

    # 2. Clear old output and create empty CSV with headers
    if os.path.isfile(OUTPUT_FILE):
        try: os.remove(OUTPUT_FILE)
        except: pass
        
    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
    except Exception as e:
        log.error(f"Could not create output file {OUTPUT_FILE}: {e}")

    # 3. Split URLs by ATS type
    wd_urls = [u for u in urls if "myworkdayjobs.com" in u.lower()]
    ic_urls = [u for u in urls if "icims.com" in u.lower()]
    log.info(f"Portals: {len(wd_urls)} Workday, {len(ic_urls)} iCIMS")

    start = time.time()

    # 4. Discover all portals
    log.info("Phase 1: Discovering portals...")
    wd_portals = []
    ic_portals = []

    async with httpx.AsyncClient(follow_redirects=True, verify=False, timeout=TIMEOUT) as disc_client:
        sem = asyncio.Semaphore(PORTAL_CONCURRENCY)
        async def disc_wd(url):
            async with sem:
                return await discover_workday(url, disc_client)
        async def disc_ic(url):
            async with sem:
                return await discover_icims(url, disc_client)
        wd_results = await asyncio.gather(*[disc_wd(u) for u in wd_urls], return_exceptions=True)
        ic_results = await asyncio.gather(*[disc_ic(u) for u in ic_urls], return_exceptions=True)
        wd_portals = [r for r in wd_results if isinstance(r, dict)]
        ic_portals = [r for r in ic_results if isinstance(r, dict)]

    log.info(f"Active portals: {len(wd_portals)} Workday, {len(ic_portals)} iCIMS")

    # 5. Search phase - per portal, search all keywords
    log.info("Phase 2: Searching roles across portals...")
    all_us_jobs = []

    # Build flat list of (domain_name, keyword)
    search_pairs = []
    for role in roles:
        for term in role["search_terms"]:
            search_pairs.append((role["name"], term))
    log.info(f"Total search pairs: {len(search_pairs)}")

    for i, portal in enumerate(wd_portals):
        log.info(f"[WD] Searching portal {i+1}/{len(wd_portals)}: {portal['tenant']}/{portal['site']} ({len(search_pairs)} keywords)...")
        async with httpx.AsyncClient(follow_redirects=True, verify=False, timeout=TIMEOUT) as client:
            # Warm up cookies
            try:
                await client.get(f"https://{portal['domain']}/en-US/{portal['site']}", headers={"User-Agent": random.choice(USER_AGENTS)})
            except Exception:
                pass
            sem = asyncio.Semaphore(SEARCH_CONCURRENCY)
            batch_size = 100
            portal_jobs = []
            for batch_start in range(0, len(search_pairs), batch_size):
                batch = search_pairs[batch_start:batch_start + batch_size]
                tasks = [search_workday_keyword(portal, kw, dn, client, sem) for dn, kw in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, list):
                        portal_jobs.extend(res)
                if batch_start % 500 == 0 and batch_start > 0:
                    log.info(f"  ... {batch_start}/{len(search_pairs)} keywords searched, {len(portal_jobs)} US jobs found so far")

            # Deduplicate within portal
            seen = set()
            unique = []
            for j in portal_jobs:
                if j["job_url"] not in seen:
                    seen.add(j["job_url"])
                    unique.append(j)
            log.info(f"[WD] {portal['tenant']}: {len(unique)} unique US jobs found")

            # Fetch details for unique jobs
            if unique:
                log.info(f"[WD] {portal['tenant']}: Fetching details for {len(unique)} jobs...")
                detail_sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
                detailed = await asyncio.gather(*[fetch_workday_detail(j, client, detail_sem) for j in unique], return_exceptions=True)
                detailed_jobs = [j for j in detailed if isinstance(j, dict)]
                all_us_jobs.extend(detailed_jobs)

    # iCIMS portals
    for i, portal in enumerate(ic_portals):
        log.info(f"[iCIMS] Searching portal {i+1}/{len(ic_portals)}: {portal['company']} ({len(search_pairs)} keywords)...")
        async with httpx.AsyncClient(follow_redirects=True, verify=False, timeout=TIMEOUT) as client:
            sem = asyncio.Semaphore(SEARCH_CONCURRENCY)
            portal_jobs = []
            for batch_start in range(0, len(search_pairs), batch_size):
                batch = search_pairs[batch_start:batch_start + batch_size]
                tasks = [search_icims_keyword(portal, kw, dn, client, sem) for dn, kw in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, list):
                        portal_jobs.extend(res)

            seen = set()
            unique = []
            for j in portal_jobs:
                if j.get("job_url") and j["job_url"] not in seen:
                    seen.add(j["job_url"])
                    unique.append(j)
            if unique:
                all_us_jobs.extend(unique)
            log.info(f"[iCIMS] {portal['company']}: {len(unique)} unique US jobs found")

    # 6. Summary
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"  SCRAPING COMPLETE - US JOBS ONLY")
    print(f"{'='*60}")
    print(f"  Portals searched    : {len(wd_portals)} Workday + {len(ic_portals)} iCIMS")
    print(f"  Role domains        : {len(roles)}")
    print(f"  Search terms used   : {total_terms}")
    print(f"  Total US jobs saved : {_job_count}")
    print(f"  Output file         : {OUTPUT_FILE}")
    print(f"  Time                : {elapsed:.1f}s")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
