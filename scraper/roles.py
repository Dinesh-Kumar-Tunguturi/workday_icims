"""
Roles API - Fetches all domains + alternate roles dynamically.
Cleans domain names to extract only the JOB ROLE (strips country suffixes).
Skips non-US country-specific domains entirely.
"""
import re
import logging
from typing import Dict, List

import httpx

from . import config

log = logging.getLogger("scraper.roles")

# Countries/regions to strip from domain names or skip entirely
NON_US_COUNTRIES = [
    "ireland", "india", "uk", "canada", "germany", "australia",
    "singapore", "japan", "china", "brazil", "mexico", "france",
    "netherlands", "spain", "italy", "sweden", "switzerland",
    "norway", "denmark", "finland", "belgium", "austria",
    "poland", "czech", "israel", "korea", "dubai", "uae",
    "qatar", "saudi", "south africa", "philippines", "malaysia",
    "thailand", "vietnam", "indonesia", "europe", "apac", "emea",
    "latam", "asia", "middle east",
]


def _clean_role_name(name: str) -> str:
    """
    Strip country/region suffixes from role names.
    
    Examples:
        "Cloud Engineer for Ireland"  → "Cloud Engineer"
        "Data Analyst - India"        → "Data Analyst"
        "SAP Consultant (UK)"         → "SAP Consultant"
        "Software Engineer"           → "Software Engineer"  (unchanged)
    """
    cleaned = name.strip()
    
    # Remove patterns like "for Ireland", "- India", "(UK)", "in Germany"
    for country in NON_US_COUNTRIES:
        # "for Ireland", "for india"
        cleaned = re.sub(rf'\s+for\s+{country}\b', '', cleaned, flags=re.IGNORECASE)
        # "- India", "– UK"
        cleaned = re.sub(rf'\s*[-–]\s*{country}\b', '', cleaned, flags=re.IGNORECASE)
        # "(Ireland)", "(UK)"
        cleaned = re.sub(rf'\s*\(\s*{country}\s*\)', '', cleaned, flags=re.IGNORECASE)
        # "in Ireland", "in India"
        cleaned = re.sub(rf'\s+in\s+{country}\b', '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip()


def _is_non_us_domain(name: str) -> bool:
    """Check if the domain name is EXPLICITLY for a non-US country."""
    nl = name.lower()
    for country in NON_US_COUNTRIES:
        if f"for {country}" in nl or f"in {country}" in nl:
            return True
        if nl.endswith(f"- {country}") or nl.endswith(f"– {country}"):
            return True
        if f"({country})" in nl:
            return True
    return False


def fetch_roles() -> List[Dict]:
    """
    Fetch all role domains from the Apply-Wizz API.
    
    Rules:
    - "name" → DOMAIN (cleaned of country suffixes)
    - "alternateRoles" → search keywords for that domain
    - Domains explicitly for non-US countries are SKIPPED
    - Role names like "Cloud Engineer for Ireland" become just "Cloud Engineer"
    """
    log.info("Fetching domains + roles from API...")
    try:
        resp = httpx.get(config.ROLES_API, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()

        roles = []
        total_terms = 0

        for item in data:
            raw_name = item.get("name", "").strip()
            if not raw_name:
                continue

            # Clean the domain name for searching but keep raw_name for the domain column
            clean_name = _clean_role_name(raw_name)
            if not clean_name or len(clean_name) < 2:
                continue

            # Build search terms: cleaned name + all alternate roles
            terms = {clean_name}
            for alt in item.get("alternateRoles", []):
                alt_clean = _clean_role_name(alt.strip())
                if alt_clean and len(alt_clean) > 1:
                    terms.add(alt_clean)

            roles.append({"name": raw_name, "terms": list(terms)})
            total_terms += len(terms)

        log.info(f"✅ {len(roles)} domains, {total_terms} search keywords loaded")
        return roles

    except Exception as e:
        log.error(f"❌ Failed to fetch roles: {e}")
        return []


def build_search_pairs(roles: List[Dict]) -> List[tuple]:
    """Build flat list of (domain_name, keyword) pairs."""
    pairs = []
    for role in roles:
        domain_name = role["name"]
        for term in role["terms"]:
            pairs.append((domain_name, term))
    return pairs
