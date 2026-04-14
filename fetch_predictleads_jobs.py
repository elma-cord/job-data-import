import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


PREDICTLEADS_URL = os.getenv(
    "PREDICTLEADS_URL",
    "https://api.predictleads.com/job_openings"
)

API_KEY = os.getenv("PREDICTLEADS_API_KEY", "").strip()
API_TOKEN = os.getenv("PREDICTLEADS_API_TOKEN", "").strip()

DAYS_BACK = int(os.getenv("DAYS_BACK", "7"))
PER_PAGE = int(os.getenv("PER_PAGE", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "1"))

OUTPUT_DIR = Path("output")
RAW_JSON_PATH = OUTPUT_DIR / "raw_predictleads_response.json"
CSV_PATH = OUTPUT_DIR / "predictleads_jobs_last_7_days_english.csv"


def safe_json(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def build_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "predictleads-api-test/1.0",
    }

    if API_KEY:
        headers["X-Api-Key"] = API_KEY

    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"

    return headers


def company_lookup(included: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup = {}

    for item in included:
        if item.get("type") != "company":
            continue

        company_id = item.get("id")
        attrs = item.get("attributes", {}) or {}

        if company_id:
            lookup[company_id] = {
                "company_id": company_id,
                "company_domain": attrs.get("domain"),
                "company_name": attrs.get("company_name"),
                "company_ticker": attrs.get("ticker"),
            }

    return lookup


def flatten_job(job: Dict[str, Any], companies: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    attrs = job.get("attributes", {}) or {}

    company_id = (
        job.get("relationships", {})
        .get("company", {})
        .get("data", {})
        .get("id")
    )

    company = companies.get(company_id, {})

    salary_data = attrs.get("salary_data") or {}
    onet_data = attrs.get("onet_data") or {}
    recruiter_data = attrs.get("recruiter_data") or {}

    return {
        "id": job.get("id"),
        "type": job.get("type"),

        "title": attrs.get("title"),
        "translated_title": attrs.get("translated_title"),
        "normalized_title": attrs.get("normalized_title"),
        "description": attrs.get("description"),
        "url": attrs.get("url"),

        "first_seen_at": attrs.get("first_seen_at"),
        "last_seen_at": attrs.get("last_seen_at"),
        "last_processed_at": attrs.get("last_processed_at"),
        "posted_at": attrs.get("posted_at"),

        "contract_types": safe_json(attrs.get("contract_types")),
        "categories": safe_json(attrs.get("categories")),

        "onet_code": onet_data.get("code"),
        "onet_family": onet_data.get("family"),
        "onet_occupation_name": onet_data.get("occupation_name"),

        "recruiter_name": recruiter_data.get("name"),
        "recruiter_title": recruiter_data.get("title"),
        "recruiter_contact": recruiter_data.get("contact"),

        "salary": attrs.get("salary"),
        "salary_low": salary_data.get("salary_low"),
        "salary_high": salary_data.get("salary_high"),
        "salary_currency": salary_data.get("salary_currency"),
        "salary_low_usd": salary_data.get("salary_low_usd"),
        "salary_high_usd": salary_data.get("salary_high_usd"),
        "salary_time_unit": salary_data.get("salary_time_unit"),

        "seniority": attrs.get("seniority"),
        "status": attrs.get("status"),
        "language": attrs.get("language"),

        "location": attrs.get("location"),
        "location_data": safe_json(attrs.get("location_data")),
        "tags": safe_json(attrs.get("tags")),

        "company_id": company.get("company_id") or company_id,
        "company_name": company.get("company_name"),
        "company_domain": company.get("company_domain"),
        "company_ticker": company.get("company_ticker"),

        "raw_json": safe_json(job),
    }


def fetch_page(page: int) -> Dict[str, Any]:
    params = {
        "page": page,
        "per_page": PER_PAGE,
    }

    response = requests.get(
        PREDICTLEADS_URL,
        headers=build_headers(),
        params=params,
        timeout=60,
    )

    print(f"Page {page}: {response.status_code}")

    if response.status_code >= 400:
        print(response.text[:3000])
        response.raise_for_status()

    return response.json()


def main() -> None:
    if not API_KEY and not API_TOKEN:
        raise RuntimeError("Missing PREDICTLEADS_API_KEY or PREDICTLEADS_API_TOKEN")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    all_payloads = []
    rows = []

    for page in range(1, MAX_PAGES + 1):
        payload = fetch_page(page)
        all_payloads.append(payload)

        companies = company_lookup(payload.get("included", []) or [])

        for job in payload.get("data", []) or []:
            attrs = job.get("attributes", {}) or {}

            if attrs.get("language") != "en":
                continue

            last_seen_at = parse_dt(attrs.get("last_seen_at"))
            if not last_seen_at or last_seen_at < cutoff:
                continue

            rows.append(flatten_job(job, companies))

    RAW_JSON_PATH.write_text(
        json.dumps(all_payloads, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    fieldnames = [
        "id", "type",
        "title", "translated_title", "normalized_title", "description", "url",
        "first_seen_at", "last_seen_at", "last_processed_at", "posted_at",
        "contract_types", "categories",
        "onet_code", "onet_family", "onet_occupation_name",
        "recruiter_name", "recruiter_title", "recruiter_contact",
        "salary", "salary_low", "salary_high", "salary_currency",
        "salary_low_usd", "salary_high_usd", "salary_time_unit",
        "seniority", "status", "language",
        "location", "location_data", "tags",
        "company_id", "company_name", "company_domain", "company_ticker",
        "raw_json",
    ]

    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. Rows saved: {len(rows)}")
    print(f"CSV: {CSV_PATH}")
    print(f"Raw JSON: {RAW_JSON_PATH}")


if __name__ == "__main__":
    main()
