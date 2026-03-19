#!/usr/bin/env python3

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
BASE_URL = f"https://app.loxo.co/api/{AGENCY}/people"
STATE_FILE = "backfill_company_title_state.txt"


def clean(v):
    return "" if v is None else str(v).strip()


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


def load_state():
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        value = f.read().strip()
        return value or None


def save_state(scroll_id):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write("" if not scroll_id else str(scroll_id))


def main():
    conn = sqlite3.connect(DB)
    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "authorization": f"Bearer {TOKEN}",
    })

    scroll = load_state()
    page = 0
    updated = 0
    start = time.time()

    print(f"Starting backfill. Resume scroll_id={'set' if scroll else 'none'}")

    while True:
        params = {"per_page": 200}
        if scroll:
            params["scroll_id"] = scroll

        r = session.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()

        data = r.json()
        people = parse_people(data)

        if not people:
            break

        page += 1
        page_updates = 0

        for p in people:
            pid = p.get("id")
            if not pid:
                continue

            company = clean(p.get("current_company"))
            title = clean(p.get("current_title"))

            if company or title:
                cur = conn.execute(
                    """
                    UPDATE people_index
                    SET company_name = CASE
                        WHEN COALESCE(company_name, '') = '' AND ? != '' THEN ?
                        ELSE company_name
                    END,
                    job_title = CASE
                        WHEN COALESCE(job_title, '') = '' AND ? != '' THEN ?
                        ELSE job_title
                    END
                    WHERE person_id = ?
                    """,
                    (company, company, title, title, pid),
                )
                if cur.rowcount:
                    page_updates += 1

        conn.commit()
        updated += page_updates

        scroll = extract_scroll(data)
        save_state(scroll)

        elapsed = time.time() - start
        print(f"Page {page} | updated {updated} | {elapsed:.1f}s")

        if not scroll:
            break

    conn.close()

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    print("\nDONE")
    print("Total updated:", updated)


if __name__ == "__main__":
    main()