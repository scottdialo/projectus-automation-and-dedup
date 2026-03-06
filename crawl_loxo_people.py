#!/usr/bin/env python3

import os
import time
import sqlite3
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


def norm_email(e):
    if not e:
        return ""
    return e.strip().lower()


def norm_linkedin(url):
    if not url:
        return ""

    url = url.strip().lower()

    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    url = url.split("?")[0].rstrip("/")

    return url


def connect_db():

    conn = sqlite3.connect(DB)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS people_index (
        person_id INTEGER PRIMARY KEY,
        linkedin_norm TEXT,
        email_norm TEXT
    )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_li ON people_index(linkedin_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_em ON people_index(email_norm)")

    return conn


def upsert(conn, pid, li, em):

    conn.execute("""
    INSERT INTO people_index(person_id, linkedin_norm, email_norm)
    VALUES (?, ?, ?)
    ON CONFLICT(person_id) DO UPDATE SET
        linkedin_norm=excluded.linkedin_norm,
        email_norm=excluded.email_norm
    """, (pid, li, em))


def get_page(scroll_id=None):

    url = f"https://app.loxo.co/api/{AGENCY}/people"

    params = {"per_page": 200}

    if scroll_id:
        params["scroll_id"] = scroll_id

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {TOKEN}"
    }

    r = requests.get(url, headers=headers, params=params, timeout=60)

    r.raise_for_status()

    return r.json()


def parse_people(data):

    if isinstance(data, list):
        return data

    if isinstance(data, dict):

        for k in ["people", "data", "results"]:
            if k in data and isinstance(data[k], list):
                return data[k]

    return []


def extract_scroll(data):

    if not isinstance(data, dict):
        return None

    for k in ["scroll_id", "next_scroll_id", "nextScrollId", "next"]:
        if k in data:
            return data[k]

    return None


def main():

    conn = connect_db()

    scroll = None

    total = 0

    start = time.time()

    while True:

        data = get_page(scroll)

        people = parse_people(data)

        if not people:
            break

        for p in people:

            pid = p.get("id")

            if not pid:
                continue

            li = norm_linkedin(p.get("linkedin_url"))
            em = norm_email(p.get("email"))

            upsert(conn, pid, li, em)

            total += 1

        conn.commit()

        if total % 5000 == 0:

            rate = total / (time.time() - start)

            print(f"{total} indexed ({rate:.1f}/sec)")

        scroll = extract_scroll(data)

        if not scroll:
            break

    conn.commit()

    conn.close()

    print("DONE")

    print("Total indexed:", total)


if __name__ == "__main__":
    main()

   