import csv
import os
import re
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup


REPORTS = [
    ("2026-03-27", "https://www.graingrowers.com.au/news/fertiliser-report-27-march-2026/"),
    ("2026-03-13", "https://www.graingrowers.com.au/news/fertiliser-report-13-march-2026/"),
    ("2026-02-27", "https://www.graingrowers.com.au/news/fertiliser-report-27-february-2026/"),
    ("2026-02-13", "https://www.graingrowers.com.au/news/fertiliser-report-13-february-2026/"),
    ("2026-01-30", "https://www.graingrowers.com.au/news/fertiliser-report-30-january-2026/"),
    ("2026-01-16", "https://www.graingrowers.com.au/news/fertiliser-report-16-january-2026/"),
    ("2025-12-31", "https://www.graingrowers.com.au/news/fertiliser-report-31-december-2025/"),
    ("2025-12-19", "https://www.graingrowers.com.au/news/fertiliser-report-19-december-2025/"),
    ("2025-12-05", "https://www.graingrowers.com.au/news/fertiliser-report-5-december-2025/"),
    ("2025-11-21", "https://www.graingrowers.com.au/news/fertiliser-report-21-november-2025/"),
    ("2025-11-07", "https://www.graingrowers.com.au/news/fertiliser-report-7-november-2025/"),
    ("2025-10-10", "https://www.graingrowers.com.au/news/fertiliser-report-10-october-2025/"),
]

OUT_CSV = "graingrowers_urea_reports.csv"

UREA_PATTERN = re.compile(
    r"(?:Argus\s+last\s+assessed\s+)?granular\s+urea(?:\s+was)?(?:\s+last)?\s+assessed(?:\s+at|\s+lower\s+at)?\s+A\$(\d{3,4}(?:\.\d+)?)\s*-\s*(\d{3,4}(?:\.\d+)?)\/t(?:\s+fca\s+Geelong|\s+Geelong\s+fca)?",
    re.IGNORECASE,
)


def fetch_text(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Try visible text first
    visible_text = soup.get_text("\n", strip=True)

    # Also include metadata because some of the quote appears in snippets/meta
    meta_parts = []
    for tag in soup.find_all("meta"):
        content = tag.get("content")
        if content:
            meta_parts.append(content)

    return "\n".join([visible_text] + meta_parts)


def parse_urea_range(text: str) -> Optional[tuple[float, float]]:
    match = UREA_PATTERN.search(text)
    if not match:
        return None
    low = float(match.group(1))
    high = float(match.group(2))
    return low, high


def main():
    rows = []

    for report_date, url in REPORTS:
        text = fetch_text(url)
        parsed = parse_urea_range(text)

        if parsed is None:
            rows.append({
                "observed_month": report_date[:7] + "-01",
                "report_date": report_date,
                "commodity_code": "urea",
                "commodity_name": "Urea",
                "region": "au_geelong",
                "price_low": "",
                "price_high": "",
                "price_mid": "",
                "currency": "AUD",
                "unit": "metric_ton",
                "quality_spec": "Argus granular urea, fca Geelong",
                "source_url": url,
                "parse_status": "review_needed",
            })
            continue

        low, high = parsed
        mid = (low + high) / 2

        rows.append({
            "observed_month": report_date[:7] + "-01",
            "report_date": report_date,
            "commodity_code": "urea",
            "commodity_name": "Urea",
            "region": "au_geelong",
            "price_low": low,
            "price_high": high,
            "price_mid": mid,
            "currency": "AUD",
            "unit": "metric_ton",
            "quality_spec": "Argus granular urea, fca Geelong",
            "source_url": url,
            "parse_status": "parsed",
        })

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()