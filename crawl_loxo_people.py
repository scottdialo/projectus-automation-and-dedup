#!/usr/bin/env python3

import json
import os
import sqlite3
import time
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ.get("LOXO_API_TOKEN")
AGENCY = os.environ.get("LOXO_AGENCY_SLUG")

if not TOKEN:
    raise RuntimeError("Set LOXO_API_TOKEN")

if not AGENCY:
    raise RuntimeError("Set LOXO_AGENCY_SLUG")

DB = "loxo_index.sqlite"
STATE_FILE = "crawl_state.json"
BASE_URL = f"https://app.loxo.co/api/{AGENCY}/people"


def norm_email(e):
    if not e:
        return ""
    return str(e).strip().lower()


def norm_linkedin(url):
    if not url:
        return ""
    url = str(url).strip().lower()
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    url = url.split("?")[0].rstrip("/")
    return url


def clean_text(v):
    if v is None:
        return ""
    return str(v).strip()


def extract_primary_email(p):
    emails = p.get("emails")
    if isinstance(emails, list):
        for item in emails:
            if isinstance(item, dict):
                v = norm_email(item.get("value"))
                if v:
                    return v

    return norm_email(p.get("email") or p.get("email_address"))


def extract_company(p):
    # Main field actually present in the Loxo API payload
    company = clean_text(p.get("current_company"))
    if company:
        return company

    # Fallbacks for older / alternate payloads
    company = clean_text(p.get("company"))
    if company:
        return company

    job_profiles = p.get("job_profiles")
    if isinstance(job_profiles, list):
        for jp in job_profiles:
            if not isinstance(jp, dict):
                continue

            company_obj = jp.get("company")
            if isinstance(company_obj, dict):
                name = clean_text(company_obj.get("name"))
                if name:
                    return name

            name = clean_text(jp.get("company"))
            if name:
                return name

    return ""


def extract_job_title(p):
    # Main field actually present in the Loxo API payload
    title = clean_text(p.get("current_title"))
    if title:
        return title

    # Fallbacks for older / alternate payloads
    title = clean_text(p.get("title"))
    if title:
        return title

    job_profiles = p.get("job_profiles")
    if isinstance(job_profiles, list):
        for jp in job_profiles:
            if isinstance(jp, dict):
                title = clean_text(jp.get("title"))
                if title:
                    return title

    return ""


def extract_location(p):
    location = clean_text(p.get("location"))
    if location:
        return location

    city = clean_text(p.get("city"))
    state = clean_text(p.get("state"))
    country = clean_text(p.get("country"))

    parts = [x for x in (city, state, country) if x]
    return ", ".join(parts)


def connect_db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=1000000")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS people_index (
        person_id INTEGER PRIMARY KEY,
        linkedin_norm TEXT,
        email_norm TEXT,
        full_name TEXT,
        company_name TEXT,
        candidate_location TEXT,
        job_title TEXT
    )
    """)

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(people_index)").fetchall()
    }

    wanted_cols = {
        "full_name": "TEXT",
        "company_name": "TEXT",
        "candidate_location": "TEXT",
        "job_title": "TEXT",
    }

    for col, col_type in wanted_cols.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE people_index ADD COLUMN {col} {col_type}")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_li ON people_index(linkedin_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_em ON people_index(email_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_name_company ON people_index(full_name, company_name)")

    return conn


def bulk_upsert(conn, rows):
    conn.executemany(
        """
        INSERT INTO people_index(
            person_id,
            linkedin_norm,
            email_norm,
            full_name,
            company_name,
            candidate_location,
            job_title
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(person_id) DO UPDATE SET
            linkedin_norm=excluded.linkedin_norm,
            email_norm=excluded.email_norm,
            full_name=excluded.full_name,
            company_name=excluded.company_name,
            candidate_location=excluded.candidate_location,
            job_title=excluded.job_title
        """,
        rows,
    )


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"scroll_id": None, "total": 0}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(scroll_id, total):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"scroll_id": scroll_id, "total": total}, f)


def get_page(session, scroll_id=None, max_retries=8):
    params = {"per_page": 200}
    if scroll_id:
        params["scroll_id"] = scroll_id

    for attempt in range(max_retries):
        try:
            r = session.get(BASE_URL, params=params, timeout=60)

            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 2 ** attempt)
                print(f"Retryable status {r.status_code}. Sleeping {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            wait = min(60, 2 ** attempt)
            print(f"Request failed ({e}). Sleeping {wait}s and retrying...")
            time.sleep(wait)

    raise RuntimeError("GET /people failed repeatedly after retries")


def parse_people(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("people", "data", "results"):
            if k in data and isinstance(data[k], list):
                return data[k]
    return []


def extract_scroll(data):
    if not isinstance(data, dict):
        return None
    for k in ("scroll_id", "next_scroll_id", "nextScrollId", "next"):
        if k in data:
            return data[k]
    return None


def main():
    state = load_state()
    scroll = state.get("scroll_id")
    total = int(state.get("total", 0))

    conn = connect_db()

    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "authorization": f"Bearer {TOKEN}",
    })

    batch = []
    start = time.time()

    print(f"Resuming from total={total}, scroll_id={'set' if scroll else 'none'}")

    while True:
        data = get_page(session, scroll)
        people = parse_people(data)

        if not people:
            break

        for p in people:
            pid = p.get("id")
            if not pid:
                continue

            full_name = clean_text(p.get("name"))
            li = norm_linkedin(p.get("linkedin_url"))
            em = extract_primary_email(p)
            company_name = extract_company(p)
            candidate_location = extract_location(p)
            job_title = extract_job_title(p)

            batch.append((
                pid,
                li,
                em,
                full_name,
                company_name,
                candidate_location,
                job_title,
            ))
            total += 1

        if len(batch) >= 2000:
            bulk_upsert(conn, batch)
            conn.commit()
            batch = []

        if total % 5000 == 0:
            rate = total / max(1, time.time() - start)
            print(f"{total} indexed ({rate:.0f}/sec)")

        new_scroll = extract_scroll(data)
        save_state(new_scroll, total)

        if not new_scroll or new_scroll == scroll:
            break

        scroll = new_scroll

    if batch:
        bulk_upsert(conn, batch)
        conn.commit()

    conn.close()

    elapsed = time.time() - start
    print("\nDONE")
    print("Total indexed this state:", total)
    print("Time:", round(elapsed, 1), "seconds")

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)


if __name__ == "__main__":
    main()