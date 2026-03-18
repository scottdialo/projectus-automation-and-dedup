#!/usr/bin/env python3

import os
import sqlite3
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ["LOXO_API_TOKEN"]
AGENCY = os.environ["LOXO_AGENCY_SLUG"]

DB = "loxo_index.sqlite"
BASE = f"https://app.loxo.co/api/{AGENCY}/people"

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {TOKEN}"
}


def get_person(pid):
    try:
        r = requests.get(f"{BASE}/{pid}", headers=headers, timeout=30)

        if r.status_code != 200:
            return {}

        data = r.json()

        if "person" in data:
            return data["person"]

        return data

    except Exception:
        return {}


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
    SELECT linkedin_norm, GROUP_CONCAT(person_id)
    FROM people_index
    WHERE linkedin_norm != ''
    GROUP BY linkedin_norm
    HAVING COUNT(*) > 1
    """)

    rows = cur.fetchall()
    records = []

    print("Duplicate LinkedIn groups:", len(rows))

    total = len(rows)

    for i, (linkedin, ids) in enumerate(rows, start=1):
        print(f"Processing {i}/{total}")

        ids = [int(x) for x in ids.split(",")]
        ids.sort()

        keeper = ids[0]

        for pid in ids:
            print(f"   fetching person {pid}")

            person = get_person(pid)

            records.append({
                "linkedin": linkedin,
                "person_id": pid,
                "name": person.get("name"),
                "email": person.get("email"),
                "company": person.get("company"),
                "keep": pid == keeper
            })

    conn.close()

    df = pd.DataFrame(records)

    df.to_csv("loxo_linkedin_duplicates.csv", index=False)
    df.to_excel("loxo_linkedin_duplicates.xlsx", index=False)

    print()
    print("Export complete:")
    print("loxo_linkedin_duplicates.csv")
    print("loxo_linkedin_duplicates.xlsx")


if __name__ == "__main__":
    main()