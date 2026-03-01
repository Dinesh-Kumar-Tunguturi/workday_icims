"""
Configuration module - All tunable parameters in one place.
"""
import os

# ─── API ───
ROLES_API = "https://dashboard.apply-wizz.com/api/all-job-roles/"

# ─── Supabase ───
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
DB_TABLE = "workday_icims"

# ─── Concurrency ───
SEMAPHORE_LIMIT = 20
PORTAL_CONCURRENCY = 12
SEARCH_PER_PORTAL = 8
WD_PAGE_SIZE = 20
MAX_PAGES_PER_KEYWORD = 500

# ─── Timeouts & Retries ───
REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0

# ─── Filtering ───
LAST_N_HOURS = 24

# ─── Portal URLs file ───
PORTALS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "career_links.txt")

# ─── CSV Backup ───
CSV_OUTPUT = "all_jobs_recent.csv"

# ─── User Agent ───
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"

# ─── DB Column Names (matches your exact Supabase table) ───
DB_FIELDS = [
    "domain", "role_searched", "source_ats", "company", "job_id", "title",
    "job_url", "description", "location", "city", "state", "country",
    "department", "job_category", "employment_type", "time_type",
    "posted_date", "salary", "experience_level", "company_logo",
    "remote_eligible",
]

# ─── US Location Keywords ───
US_LOCATION_KEYWORDS = [
    "usa", "united states", "u.s.", "california", "texas", "florida",
    "new york", "san francisco", "santa clara", "charlotte", "austin",
    "seattle", "atlanta", "chicago", "boston", "denver", "portland",
    "san jose", "los angeles", "washington", "philadelphia", "phoenix",
    "san diego", "dallas", "houston", "minneapolis", "detroit", "raleigh",
    "nashville", "columbus", "pittsburgh", "salt lake", "richmond",
    "indianapolis", "kansas city", "milwaukee", "baltimore", "new jersey",
    "connecticut", "massachusetts", "virginia", "maryland", "georgia",
    "north carolina", "illinois", "ohio", "michigan", "pennsylvania",
    "tennessee", "arizona", "colorado", "oregon", "minnesota", "wisconsin",
    "missouri", "iowa", "kentucky", "utah", "nevada", "irvine", "irving",
    "sunnyvale", "cupertino", "mountain view", "redmond", "arlington",
    "plano", "bellevue", "palo alto", "menlo park", "redwood city",
    "fremont", "san mateo", "milpitas", "santa monica", "burbank",
    "oakland", "sacramento", "san antonio", "fort worth", "jacksonville",
    "st louis", "cincinnati", "cleveland", "orlando", "tampa", "miami",
    "boca raton", "jersey city", "hoboken", "newark", "stamford",
    "wilmington", "morristown", "remote",
]
