import sys
from scraper.utils import is_us_location

test_cases = [
    ("Austin, TX", ""),
    ("Bangalore", "India"),
    ("Toronto", "Canada"),
    ("San Jose, CA, US", "US"),
    ("Remote - United States", ""),
    ("Remote", "Ireland"),
    ("London", "UK"),
    ("Remote", "United States of America")
]

for loc, country in test_cases:
    print(f"{loc} | {country} -> {is_us_location(loc, country)}")
