"""
Experience extraction module.
Extracts years-of-experience from job descriptions using regex patterns.
Column is NEVER empty - defaults to 'Not Specified'.
"""
import re
from typing import Optional

# Ordered by priority - most specific patterns first
EXPERIENCE_PATTERNS = [
    # "5-7 years of experience"
    re.compile(r'(\d{1,2})\s*[-–to]+\s*(\d{1,2})\s*\+?\s*years?\b', re.IGNORECASE),
    # "5+ years"
    re.compile(r'(\d{1,2})\s*\+\s*years?\b', re.IGNORECASE),
    # "minimum 5 years" / "minimum of 5 years"
    re.compile(r'minimum\s+(?:of\s+)?(\d{1,2})\s*\+?\s*years?\b', re.IGNORECASE),
    # "at least 5 years"
    re.compile(r'at\s+least\s+(\d{1,2})\s*\+?\s*years?\b', re.IGNORECASE),
    # "5 years of experience"
    re.compile(r'(\d{1,2})\s*years?\s+(?:of\s+)?(?:experience|exp)\b', re.IGNORECASE),
    # "experience: 5 years"
    re.compile(r'experience\s*[:=]\s*(\d{1,2})\s*\+?\s*years?\b', re.IGNORECASE),
    # "requires 5 years"
    re.compile(r'requires?\s+(\d{1,2})\s*\+?\s*years?\b', re.IGNORECASE),
    # "over 5 years"
    re.compile(r'over\s+(\d{1,2})\s*years?\b', re.IGNORECASE),
    # "X yrs"
    re.compile(r'(\d{1,2})\s*[-–to]+\s*(\d{1,2})\s*\+?\s*yrs?\b', re.IGNORECASE),
    re.compile(r'(\d{1,2})\s*\+?\s*yrs?\s+(?:of\s+)?(?:experience|exp)\b', re.IGNORECASE),
]


def extract_experience(text: str) -> str:
    """
    Extract experience requirement from job description.
    
    Returns:
        Human-readable string like "5-7 years", "5+ years", "3 years"
        Or "Not Specified" if nothing found.
        
    NEVER returns empty string.
    """
    if not text:
        return "Not Specified"

    for pattern in EXPERIENCE_PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            if len(groups) == 2 and groups[1]:
                return f"{groups[0]}-{groups[1]} years"
            elif len(groups) == 1:
                # Check if the original match contains "+"
                matched_text = match.group(0)
                if "+" in matched_text:
                    return f"{groups[0]}+ years"
                return f"{groups[0]} years"

    return "Not Specified"
