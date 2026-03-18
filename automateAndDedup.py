#!/usr/bin/env python3
"""
Fast Loxo index builder for dedupe.

Indexes:
- LinkedIn
- All Emails
- All Phones
"""

import argparse
import os
import re
import sqlite3
import time
from typing import Optional

import pandas as pd

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")


# ---------------------------
# Cleaning
# ---------------------------

def norm_email(email: str) -> str:
    if not isinstance(email, str):
        return ""
    e = email.strip().lower()
    if not EMAIL_RE.match(e):
        return ""
    return e


def clean_phone(phone: str) -> str:
    if not isinstance(phone, str):
        return ""

    s = phone.strip()
    s = re.sub(r"\D+", "", s)

    if len(s) < 7:
        return ""

    return s


def norm_linkedin(url: str) -> str:
    if not isinstance(url, str):
        return ""

    u = url.strip().lower()

    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)

    u = u.split("?")[0].rstrip("/")

    return u


# ---------------------------
# SQLite
# ---------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS people_index (
    person_id INTEGER,
    linkedin_norm TEXT,
    email_norm TEXT,
    phone_norm TEXT,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_people_id ON people_index(person_id);
CREATE INDEX IF NOT EXISTS idx_people_linkedin ON people_index(linkedin_norm);
CREATE INDEX IF NOT EXISTS idx_people_email ON people_index(email_norm);
CREATE INDEX IF NOT EXISTS idx_people_phone ON people_index(phone_norm);
"""


class IndexDB:

    def __init__(self, db_path: str):

        self.db_path = db_path

        self.con = sqlite3.connect(self.db_path)

        self.con.execute("PRAGMA journal_mode=WAL;")
        self.con.execute("PRAGMA synchronous=NORMAL;")

        self._ensure()

    def _ensure(self):

        cur = self.con.cursor()

        cur.executescript(SCHEMA)

        self.con.commit()

    def insert(self, person_id, linkedin="", email="", phone=""):

        li = norm_linkedin(linkedin)
        em = norm_email(email)
        ph = clean_phone(phone)

        cur = self.con.cursor()

        cur.execute(
            """
            INSERT INTO people_index
            (person_id, linkedin_norm, email_norm, phone_norm, updated_at)
            VALUES (?, ?, ?, ?, '')
            """,
            (person_id, li, em, ph),
        )

    def bulk_commit(self):
        self.con.commit()

    def close(self):
        self.con.close()

    def find_by_linkedin(self, linkedin) -> Optional[int]:

        li = norm_linkedin(linkedin)

        if not li:
            return None

        cur = self.con.cursor()

        cur.execute(
            "SELECT person_id FROM people_index WHERE linkedin_norm=? LIMIT 1",
            (li,),
        )

        r = cur.fetchone()

        return r[0] if r else None

    def find_by_email(self, email) -> Optional[int]:

        em = norm_email(email)

        if not em:
            return None

        cur = self.con.cursor()

        cur.execute(
            "SELECT person_id FROM people_index WHERE email_norm=? LIMIT 1",
            (em,),
        )

        r = cur.fetchone()

        return r[0] if r else None

    def find_by_phone(self, phone) -> Optional[int]:

        ph = clean_phone(phone)

        if not ph:
            return None

        cur = self.con.cursor()

        cur.execute(
            "SELECT person_id FROM people_index WHERE phone_norm=? LIMIT 1",
            (ph,),
        )

        r = cur.fetchone()

        return r[0] if r else None


# ---------------------------
# Backfill
# ---------------------------

def backfill_from_csv(db: IndexDB, csv_path: str, id_col: str, linkedin_col: str, list_cols=False):

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    if list_cols:
        print("\nCSV columns:\n")
        for c in df.columns:
            print(" -", c)
        return

    total = len(df)

    start = time.time()

    rows = 0

    for _, r in df.iterrows():

        pid_raw = str(r.get(id_col, "")).strip()

        if not pid_raw:
            continue

        try:
            pid = int(pid_raw)
        except:
            continue

        linkedin = r.get(linkedin_col, "")

        emails = [
            r.get("Email", ""),
            r.get("Personal Email", ""),
            r.get("Work Email", ""),
        ]

        phones = [
            r.get("Phone", ""),
            r.get("Personal Phone", ""),
            r.get("Work Phone", ""),
        ]

        db.insert(pid, linkedin)

        for e in emails:

            if e:
                db.insert(pid, "", e)

        for p in phones:

            if p:
                db.insert(pid, "", "", p)

        rows += 1

        if rows % 10000 == 0:

            db.bulk_commit()

            rate = rows / (time.time() - start)

            print(f"Indexed {rows}/{total} ({rate:.0f}/sec)")

    db.bulk_commit()

    rate = rows / (time.time() - start)

    print(f"\nDONE backfill: {rows} people indexed ({rate:.0f}/sec)")


# ---------------------------
# CLI
# ---------------------------

def main():

    parser = argparse.ArgumentParser()

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db")
    p_init.add_argument("--db", default="loxo_index.sqlite")

    p_backfill = sub.add_parser("backfill-csv")

    p_backfill.add_argument("--db", default="loxo_index.sqlite")

    p_backfill.add_argument("--csv", required=True)

    p_backfill.add_argument("--id-col", default="Id")

    p_backfill.add_argument("--linkedin-col", default="LinkedIN")

    p_backfill.add_argument("--list-cols", action="store_true")

    args = parser.parse_args()

    if args.cmd == "init-db":

        IndexDB(args.db)

        print(f"[OK] initialized {args.db}")

        return

    if args.cmd == "backfill-csv":

        db = IndexDB(args.db)

        backfill_from_csv(db, args.csv, args.id_col, args.linkedin_col, args.list_cols)

        db.close()


if __name__ == "__main__":
    main()