#!/usr/bin/env python3

import argparse
import os
import re
import sqlite3
import time
from typing import Any, Dict, Optional, List

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")


def norm_email(email: str) -> str:
    if not isinstance(email, str):
        return ""
    e = email.strip().lower()
    return e if e and EMAIL_RE.match(e) else ""


def norm_linkedin(url: str) -> str:
    if not isinstance(url, str):
        return ""
    u = url.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    if u.startswith("linkedin.com/search/"):
        return ""
    u = u.split("?")[0].rstrip("/")
    return u


def split_multi_value(value: Any) -> List[str]:
    """
    Convert a stored therapy/device value into a normalized list.

    Handles:
    - comma-separated strings
    - semicolon-separated strings
    - pipe-separated strings
    - lists
    """
    if value is None:
        return []

    if isinstance(value, list):
        raw_items = value
    else:
        text = str(value).strip()
        if not text:
            return []
        raw_items = re.split(r"[;,|]", text)

    cleaned: List[str] = []
    seen = set()

    for item in raw_items:
        s = str(item).strip()
        if not s:
            continue
        key = s.casefold()
        if key not in seen:
            seen.add(key)
            cleaned.append(s)

    return cleaned


def merge_multi_value(existing: Any, new_value: Any) -> str:
    """
    Append new therapy/device values to existing ones without duplicates.
    Returns a comma-separated string suitable for person[custom_text_1].
    """
    existing_items = split_multi_value(existing)
    new_items = split_multi_value(new_value)

    seen = {item.casefold() for item in existing_items}
    for item in new_items:
        key = item.casefold()
        if key not in seen:
            existing_items.append(item)
            seen.add(key)

    return ", ".join(existing_items)


class IndexDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)

    def close(self):
        self.conn.close()

    def has_column(self, table_name: str, column_name: str) -> bool:
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(r[1] == column_name for r in rows)

    def find_by_person_id(self, person_id_value: str) -> Optional[int]:
        if not person_id_value:
            return None
        try:
            return int(str(person_id_value).strip())
        except Exception:
            return None

    def find_by_linkedin(self, linkedin_url: str) -> Optional[int]:
        li = norm_linkedin(linkedin_url)
        if not li:
            return None
        row = self.conn.execute(
            "SELECT person_id FROM people_index WHERE linkedin_norm = ? LIMIT 1",
            (li,),
        ).fetchone()
        return int(row[0]) if row else None

    def find_by_email(self, email: str) -> Optional[int]:
        em = norm_email(email)
        if not em:
            return None
        row = self.conn.execute(
            "SELECT person_id FROM people_index WHERE email_norm = ? LIMIT 1",
            (em,),
        ).fetchone()
        return int(row[0]) if row else None

    def upsert_index(self, person_id: int, linkedin_url: str, email: str):
        li = norm_linkedin(linkedin_url)
        em = norm_email(email)

        cols = {
            r[1]
            for r in self.conn.execute("PRAGMA table_info(people_index)").fetchall()
        }

        if "person_id" not in cols:
            raise RuntimeError("people_index table missing person_id")

        if "linkedin_norm" in cols and "email_norm" in cols:
            self.conn.execute(
                """
                INSERT INTO people_index(person_id, linkedin_norm, email_norm)
                VALUES (?, ?, ?)
                ON CONFLICT(person_id) DO UPDATE SET
                    linkedin_norm=excluded.linkedin_norm,
                    email_norm=excluded.email_norm
                """,
                (person_id, li, em),
            )
        elif "linkedin_norm" in cols:
            self.conn.execute(
                """
                INSERT INTO people_index(person_id, linkedin_norm)
                VALUES (?, ?)
                ON CONFLICT(person_id) DO UPDATE SET
                    linkedin_norm=excluded.linkedin_norm
                """,
                (person_id, li),
            )
        elif "email_norm" in cols:
            self.conn.execute(
                """
                INSERT INTO people_index(person_id, email_norm)
                VALUES (?, ?)
                ON CONFLICT(person_id) DO UPDATE SET
                    email_norm=excluded.email_norm
                """,
                (person_id, em),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO people_index(person_id)
                VALUES (?)
                ON CONFLICT(person_id) DO NOTHING
                """,
                (person_id,),
            )

        self.conn.commit()


class LoxoClient:
    def __init__(self, agency_slug: str, token: str, base_domain: str = "app.loxo.co"):
        self.base_people = f"https://{base_domain}/api/{agency_slug}/people"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "application/json",
                "authorization": f"Bearer {token}",
            }
        )

    def create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(self.base_people, data=payload, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"POST /people failed {r.status_code}: {r.text}")
        return r.json() if r.content else {}

    def get_person(self, person_id: int) -> Dict[str, Any]:
        url = f"{self.base_people}/{person_id}"
        r = self.session.get(url, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"GET /people/{person_id} failed {r.status_code}: {r.text}")
        return r.json() if r.content else {}

    def update(self, person_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_people}/{person_id}"
        r = self.session.patch(url, data=payload, timeout=60)
        if r.status_code == 405:
            r = self.session.put(url, data=payload, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"UPDATE /people/{person_id} failed {r.status_code}: {r.text}")
        return r.json() if r.content else {}

    def get_current_therapy(self, person_id: int) -> str:
        """
        Best-effort extraction of custom_text_1 from a Loxo person response.
        Tries several likely shapes because API responses can vary.
        """
        resp = self.get_person(person_id)

        candidate_objects: List[Dict[str, Any]] = []
        if isinstance(resp, dict):
            candidate_objects.append(resp)

            person_obj = resp.get("person")
            if isinstance(person_obj, dict):
                candidate_objects.append(person_obj)

            data_obj = resp.get("data")
            if isinstance(data_obj, dict):
                candidate_objects.append(data_obj)

        direct_keys = [
            "custom_text_1",
            "person[custom_text_1]",
            "therapy",
            "Therapy/Device",
        ]

        for obj in candidate_objects:
            for key in direct_keys:
                if key in obj and obj.get(key) not in (None, ""):
                    return str(obj.get(key)).strip()

        for obj in candidate_objects:
            custom = obj.get("custom_fields")
            if isinstance(custom, dict):
                for key in ("custom_text_1", "person[custom_text_1]", "Therapy/Device"):
                    if key in custom and custom.get(key) not in (None, ""):
                        return str(custom.get(key)).strip()

        for obj in candidate_objects:
            fields = obj.get("customs") or obj.get("custom_fields_attributes") or obj.get("fields")
            if isinstance(fields, list):
                for field in fields:
                    if not isinstance(field, dict):
                        continue

                    possible_names = [
                        str(field.get("name", "")).strip(),
                        str(field.get("key", "")).strip(),
                        str(field.get("slug", "")).strip(),
                        str(field.get("label", "")).strip(),
                    ]
                    possible_value = (
                        field.get("value")
                        or field.get("text")
                        or field.get("field_value")
                        or field.get("answer")
                    )

                    normalized_names = {name.casefold() for name in possible_names if name}
                    if (
                        "custom_text_1" in normalized_names
                        or "therapy/device" in normalized_names
                        or "therapy" in normalized_names
                    ):
                        if possible_value not in (None, ""):
                            return str(possible_value).strip()

        return ""


def parse_person_id_from_response(resp: Dict[str, Any]) -> Optional[int]:
    if not isinstance(resp, dict):
        return None

    if isinstance(resp.get("person"), dict) and resp["person"].get("id") is not None:
        return int(resp["person"]["id"])

    if resp.get("id") is not None:
        return int(resp["id"])

    if isinstance(resp.get("data"), dict) and resp["data"].get("id") is not None:
        return int(resp["data"]["id"])

    return None


def load_file(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path, dtype=str).fillna("")

    # These columns must be included in the excel / csv file to avoid error
    required = [
        "full_name",
        "Email Address",
        "LinkedIn Contact Profile URL",
        "Therapy/Device",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}\nFound: {list(df.columns)}")

    return df.copy()


def build_create_payload(row: pd.Series) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    full_name = str(row.get("full_name", "")).strip()
    email = norm_email(str(row.get("Email Address", "")).strip())
    linkedin = str(row.get("LinkedIn Contact Profile URL", "")).strip()
    therapy = str(row.get("Therapy/Device", "")).strip()
    job_title = str(row.get("Job Title", "")).strip() if "Job Title" in row.index else ""
    country = str(row.get("Country", "")).strip() if "Country" in row.index else ""
    company = str(row.get("Company Name", "")).strip() if "Company Name" in row.index else ""
    phone = str(row.get("Mobile phone", "")).strip() if "Mobile phone" in row.index else ""

    if full_name:
        payload["person[name]"] = full_name
    if email:
        payload["person[email]"] = email
    if linkedin:
        payload["person[linkedin_url]"] = linkedin
    if therapy:
        payload["person[custom_text_1]"] = therapy
    if job_title:
        payload["person[title]"] = job_title
    if country:
        payload["person[country]"] = country
    if company:
        payload["person[company]"] = company
    if phone:
        payload["person[phone]"] = phone

    return payload


def build_update_payload(row: pd.Series, current_therapy: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    new_therapy = str(row.get("Therapy/Device", "")).strip()
    if not new_therapy:
        return payload

    merged_therapy = merge_multi_value(current_therapy, new_therapy)
    if merged_therapy:
        payload["person[custom_text_1]"] = merged_therapy

    return payload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--agency-slug", required=True)
    parser.add_argument("--db", default="loxo_index.sqlite")
    parser.add_argument("--base-domain", default="app.loxo.co")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    token = os.environ.get("LOXO_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Set LOXO_API_TOKEN")

    df = load_file(args.input)
    db = IndexDB(args.db)
    client = LoxoClient(args.agency_slug, token, base_domain=args.base_domain)

    has_person_id_col = "person_id" in df.columns

    created = 0
    updated = 0
    skipped = 0
    errors = 0

    total_rows = len(df)

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        full_name = str(row.get("full_name", "")).strip()
        therapy = str(row.get("Therapy/Device", "")).strip()
        email = norm_email(str(row.get("Email Address", "")).strip())
        linkedin = str(row.get("LinkedIn Contact Profile URL", "")).strip()
        person_id_raw = str(row.get("person_id", "")).strip() if has_person_id_col else ""

        if not therapy:
            skipped += 1
            print(f"[{idx}/{total_rows}] SKIP {full_name} (no Therapy/Device)")
            continue

        matched_person_id = None
        match_type = ""

        if person_id_raw:
            matched_person_id = db.find_by_person_id(person_id_raw)
            if matched_person_id:
                match_type = "person_id"

        if matched_person_id is None and linkedin:
            matched_person_id = db.find_by_linkedin(linkedin)
            if matched_person_id:
                match_type = "linkedin"

        if matched_person_id is None and email:
            matched_person_id = db.find_by_email(email)
            if matched_person_id:
                match_type = "email"

        try:
            if args.dry_run:
                if matched_person_id:
                    print(f"[{idx}/{total_rows}] WOULD FETCH existing therapy for {full_name}")
                    print(f"[{idx}/{total_rows}] WOULD MERGE new therapy='{therapy}' into existing value")
                    updated += 1
                    print(f"[{idx}/{total_rows}] UPDATE {full_name} via {match_type}")
                else:
                    created += 1
                    print(f"[{idx}/{total_rows}] CREATE {full_name} -> therapy={therapy}")
                continue

            if matched_person_id:
                current_therapy = client.get_current_therapy(matched_person_id)
                payload = build_update_payload(row, current_therapy)

                if not payload:
                    skipped += 1
                    print(f"[{idx}/{total_rows}] SKIP {full_name} (empty update payload)")
                    continue

                client.update(matched_person_id, payload)
                updated += 1
                print(
                    f"[{idx}/{total_rows}] UPDATED {full_name} via {match_type} "
                    f"(person_id={matched_person_id}) "
                    f"| old='{current_therapy}' | new='{payload['person[custom_text_1]']}'"
                )
            else:
                payload = build_create_payload(row)
                resp = client.create(payload)
                new_id = parse_person_id_from_response(resp)
                if new_id:
                    db.upsert_index(new_id, linkedin, email)
                created += 1
                print(f"[{idx}/{total_rows}] CREATED {full_name} (person_id={new_id})")

            time.sleep(0.2)

        except Exception as e:
            errors += 1
            print(f"[{idx}/{total_rows}] ERROR {full_name} - {str(e)[:500]}")

    db.close()

    print("\nDONE")
    print(f"Created: {created} | Updated: {updated} | Skipped: {skipped} | Errors: {errors}")


if __name__ == "__main__":
    main()