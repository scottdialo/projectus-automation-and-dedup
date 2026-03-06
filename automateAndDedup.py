#!/usr/bin/env python3
"""
Fast Loxo import + dedupe without fetching all contacts every run.

Key idea:
- Maintain a local SQLite index of Loxo people keyed by linkedin_norm + email_norm.
- Build it once from a Loxo CSV export (fast) or any backfill method.
- Keep it updated via Loxo webhooks (person create/update/destroy).
- During imports, check SQLite for duplicates instantly; only call Loxo for creates/updates.

Commands:
1) Initialize DB:
   python automateAndDedup.py init-db --db loxo_index.sqlite

2) Backfill from Loxo CSV export (recommended):
   python automateAndDedup.py backfill-csv --db loxo_index.sqlite --csv /path/loxo_people_export.csv \
       --id-col "ID" --linkedin-col "LinkedIn" --email-col "Email"

   (You must map the column names to your export. Run with --list-cols to see them.)

3) Run importer (no full fetch):
   export LOXO_API_TOKEN="..."
   python automateAndDedup.py import \
       --input "/path/sampleUsers.xlsx" \
       --agency-slug "projectus-consulting-ltd" \
       --db loxo_index.sqlite \
       --dry-run

4) Run webhook receiver to keep DB fresh:
   export LOXO_API_TOKEN="..."
   export LOXO_AGENCY_SLUG="projectus-consulting-ltd"
   python automateAndDedup.py webhook-server --db loxo_index.sqlite --host 0.0.0.0 --port 8000

   Then set Loxo webhook endpoint_url to:
   https://YOUR_PUBLIC_DOMAIN/webhooks/loxo

Env vars:
- LOXO_API_TOKEN (required for import + webhook fetch)
- LOXO_AGENCY_SLUG (required for webhook-server fetch)
- LOXO_BASE_DOMAIN (optional, default app.loxo.co)
- LOXO_WEBHOOK_SECRET (optional, if Loxo provides a secret for signature verification; otherwise signature is logged only)

Notes:
- LinkedIn is primary unique identifier.
- Email fallback is allowed only if it's valid AND NOT company-like (per your rule).
- Phone/email invalid errors won't stop the script: it retries create/update without those fields.
"""

import argparse
import csv
import hashlib
import hmac
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

REQUIRED_COLUMNS = [
    "Last Name",
    "First Name",
    "Job Title",
    "Department",
    "Email Address",
    "Mobile phone",
    "LinkedIn Contact Profile URL",
    "Country",
    "Company Name",
    "Website",
]

# Loxo form fields (matches your working examples)
LOXO_FORM_MAPPING = {
    "Job Title": "person[title]",
    "Email Address": "person[email]",
    "Mobile phone": "person[phone]",
    "LinkedIn Contact Profile URL": "person[linkedin_url]",
    "Country": "person[country]",
    "Company Name": "person[company]",
}

# ---------------------------
# Cleaning / normalization
# ---------------------------

def norm_email(email: str) -> str:
    if not isinstance(email, str):
        return ""
    return email.strip().lower()

def clean_email(email: str) -> str:
    e = norm_email(email)
    return e if e and EMAIL_RE.match(e) else ""

def norm_linkedin(url: str) -> str:
    if not isinstance(url, str):
        return ""
    u = url.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    u = u.split("?")[0].rstrip("/")
    return u

def clean_phone(phone: str) -> str:
    if phone is None:
        return ""
    s = str(phone).strip()
    if not s:
        return ""
    # Strip common extensions at end
    s = re.sub(r"(\bext\b|\bx\b|extension)\s*\.?\s*\d+$", "", s, flags=re.IGNORECASE).strip()
    has_plus = s.startswith("+")
    digits = re.sub(r"\D+", "", s)
    # E.164 practical bounds
    if len(digits) < 7 or len(digits) > 15:
        return ""
    if has_plus or len(digits) > 10:
        return "+" + digits
    return digits

def extract_domain(website: str) -> str:
    if not website:
        return ""
    w = website.strip()
    if not w:
        return ""
    if not w.startswith("http"):
        w = "https://" + w
    try:
        host = urlparse(w).hostname or ""
    except Exception:
        return ""
    return host.replace("www.", "").lower()

def email_domain(email: str) -> str:
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].lower()

def normalize_company_tokens(company: str) -> Iterable[str]:
    if not isinstance(company, str):
        return []
    name = company.lower()
    name = re.sub(r"[^a-z0-9]+", " ", name).strip()
    parts = [p for p in name.split() if p]
    stop = {
        "inc","incorporated","llc","ltd","limited","corp","corporation",
        "co","company","plc","gmbh","sa","sas","bv","ag","kg",
        "group","holdings","holding","the"
    }
    toks = [p for p in parts if p not in stop and len(p) >= 3]
    collapsed = "".join(toks)
    if len(collapsed) >= 6:
        toks.append(collapsed)
    # unique preserve order
    out, seen = [], set()
    for t in toks:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def is_company_email(email: str, company: str, website: str) -> bool:
    ed = email_domain(email)
    if not ed:
        return False
    wd = extract_domain(website)
    if wd and ed == wd:
        return True
    for t in normalize_company_tokens(company):
        if t and t in ed:
            return True
    return False

# ---------------------------
# SQLite index (fast local lookup)
# ---------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS people_index (
  person_id     INTEGER PRIMARY KEY,
  linkedin_norm TEXT,
  email_norm    TEXT,
  updated_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_people_linkedin ON people_index(linkedin_norm);
CREATE INDEX IF NOT EXISTS idx_people_email ON people_index(email_norm);
"""

class IndexDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure()

    def _connect(self):
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        return con

    def _ensure(self):
        con = self._connect()
        cur = con.cursor()
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        con.commit()
        con.close()

    def upsert(self, person_id: int, linkedin_url: str, email: str, updated_at: str = ""):
        li = norm_linkedin(linkedin_url or "")
        em = norm_email(email or "")
        con = self._connect()
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO people_index(person_id, linkedin_norm, email_norm, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
              linkedin_norm=excluded.linkedin_norm,
              email_norm=excluded.email_norm,
              updated_at=excluded.updated_at
            """,
            (person_id, li or None, em or None, updated_at or None),
        )
        con.commit()
        con.close()

    def delete(self, person_id: int):
        con = self._connect()
        cur = con.cursor()
        cur.execute("DELETE FROM people_index WHERE person_id=?", (person_id,))
        con.commit()
        con.close()

    def find_by_linkedin(self, linkedin_url: str) -> Optional[int]:
        li = norm_linkedin(linkedin_url or "")
        if not li:
            return None
        con = self._connect()
        cur = con.cursor()
        cur.execute("SELECT person_id FROM people_index WHERE linkedin_norm=? LIMIT 1", (li,))
        row = cur.fetchone()
        con.close()
        return int(row[0]) if row else None

    def find_by_email(self, email: str) -> Optional[int]:
        em = norm_email(email or "")
        if not em:
            return None
        con = self._connect()
        cur = con.cursor()
        cur.execute("SELECT person_id FROM people_index WHERE email_norm=? LIMIT 1", (em,))
        row = cur.fetchone()
        con.close()
        return int(row[0]) if row else None

# ---------------------------
# Loxo API client
# ---------------------------

@dataclass
class LoxoClient:
    agency_slug: str
    token: str
    base_domain: str = "app.loxo.co"

    def __post_init__(self):
        self.base_people = f"https://{self.base_domain}/api/{self.agency_slug}/people"
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "authorization": f"Bearer {self.token}",
        })

    def create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(self.base_people, data=payload, timeout=60)

        # retry without bad fields
        if r.status_code == 400 and r.text and ("Email is invalid" in r.text or "Phone is invalid" in r.text):
            payload2 = dict(payload)
            payload2.pop("person[email]", None)
            payload2.pop("person[phone]", None)
            r = self.session.post(self.base_people, data=payload2, timeout=60)

        if r.status_code >= 400:
            raise RuntimeError(f"POST /people failed {r.status_code}: {r.text}")

        return r.json() if r.content else {}

    def update(self, person_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_people}/{person_id}"
        r = self.session.patch(url, data=payload, timeout=60)

        if r.status_code == 405:
            r = self.session.put(url, data=payload, timeout=60)

        if r.status_code == 400 and r.text and ("Email is invalid" in r.text or "Phone is invalid" in r.text):
            payload2 = dict(payload)
            payload2.pop("person[email]", None)
            payload2.pop("person[phone]", None)
            r = self.session.patch(url, data=payload2, timeout=60)
            if r.status_code == 405:
                r = self.session.put(url, data=payload2, timeout=60)

        if r.status_code >= 400:
            raise RuntimeError(f"UPDATE /people/{person_id} failed {r.status_code}: {r.text}")

        return r.json() if r.content else {}

    def get_person(self, person_id: int) -> Dict[str, Any]:
        url = f"{self.base_people}/{person_id}"
        # try minimal fields first; fallback if server rejects
        params = {"fields": "id,linkedin_url,email,email_address,updated_at"}
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code >= 400:
            r = self.session.get(url, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"GET /people/{person_id} failed {r.status_code}: {r.text}")
        data = r.json() if r.content else {}
        if isinstance(data, dict) and isinstance(data.get("person"), dict):
            return data["person"]
        return data if isinstance(data, dict) else {}

def parse_person_id_from_response(resp: Dict[str, Any]) -> Optional[int]:
    # Loxo often returns {"person": {"id": ...}, "errors": []} or {"id": ...}
    if not isinstance(resp, dict):
        return None
    if isinstance(resp.get("person"), dict) and resp["person"].get("id") is not None:
        try:
            return int(resp["person"]["id"])
        except Exception:
            return None
    if resp.get("id") is not None:
        try:
            return int(resp["id"])
        except Exception:
            return None
    return None

# ---------------------------
# File loading + payload building
# ---------------------------

def load_file(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path, dtype=str).fillna("")
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}\nFound: {list(df.columns)}")
    return df[REQUIRED_COLUMNS].copy()

def build_payload(row: pd.Series) -> Tuple[Dict[str, Any], str, str]:
    first = str(row["First Name"]).strip()
    last = str(row["Last Name"]).strip()
    name = (first + " " + last).strip()

    raw_email = str(row["Email Address"]).strip()
    raw_phone = str(row["Mobile phone"]).strip()
    raw_linkedin = str(row["LinkedIn Contact Profile URL"]).strip()

    email = clean_email(raw_email)
    phone = clean_phone(raw_phone)
    linkedin_norm = norm_linkedin(raw_linkedin)

    payload: Dict[str, Any] = {}
    if name:
        payload["person[name]"] = name
    if email:
        payload["person[email]"] = email
    if phone:
        payload["person[phone]"] = phone
    if raw_linkedin:
        payload["person[linkedin_url]"] = raw_linkedin

    # Map remaining fields (skip empties)
    if str(row["Job Title"]).strip():
        payload["person[title]"] = str(row["Job Title"]).strip()
    if str(row["Country"]).strip():
        payload["person[country]"] = str(row["Country"]).strip()
    if str(row["Company Name"]).strip():
        payload["person[company]"] = str(row["Company Name"]).strip()

    return payload, linkedin_norm, email

# ---------------------------
# Backfill from CSV export
# ---------------------------

def backfill_from_csv(db: IndexDB, csv_path: str, id_col: str, linkedin_col: str, email_col: str, list_cols: bool = False):
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    if list_cols:
        print("CSV columns:")
        for c in df.columns:
            print(" -", c)
        return

    for col in (id_col, linkedin_col, email_col):
        if col not in df.columns:
            raise RuntimeError(f"Column '{col}' not found in CSV. Use --list-cols to inspect columns.")

    total = len(df)
    t0 = time.time()
    done = 0

    for _, r in df.iterrows():
        pid_raw = str(r.get(id_col, "")).strip()
        if not pid_raw:
            continue
        try:
            pid = int(float(pid_raw))  # handles "123.0" exports
        except Exception:
            continue

        li = str(r.get(linkedin_col, "")).strip()
        em = str(r.get(email_col, "")).strip()

        db.upsert(pid, li, em, updated_at="")

        done += 1
        if done % 50000 == 0:
            rate = done / max(1e-9, (time.time() - t0))
            print(f"Backfilled {done}/{total} ({rate:.1f}/s)")

    rate = done / max(1e-9, (time.time() - t0))
    print(f"DONE backfill: {done} rows written ({rate:.1f}/s) into {db.db_path}")

# ---------------------------
# Import (no full fetch)
# ---------------------------

def run_import(args):
    token = os.environ.get("LOXO_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Set LOXO_API_TOKEN env var")

    db = IndexDB(args.db)
    client = LoxoClient(args.agency_slug, token, base_domain=args.base_domain)

    df = load_file(args.input)

    created = updated = skipped = errors = 0

    with open(args.log, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["action","person_id","name","match","linkedin_norm","email","detail"],
        )
        writer.writeheader()

        for _, row in df.iterrows():
            payload, linkedin_norm, email = build_payload(row)
            name = payload.get("person[name]", "")

            company = str(row["Company Name"]).strip()
            website = str(row["Website"]).strip()

            person_id: Optional[int] = None
            match = ""

            # Primary match: linkedin via local DB
            if linkedin_norm:
                person_id = db.find_by_linkedin(row["LinkedIn Contact Profile URL"])
                if person_id:
                    match = "linkedin"

            # Fallback: email via local DB only if allowed
            if person_id is None and email:
                if not is_company_email(email, company, website):
                    person_id = db.find_by_email(email)
                    if person_id:
                        match = "email_fallback"
                else:
                    match = "email_blocked_company_like"

            # If no reliable identifier, skip to avoid creating duplicates
            if not linkedin_norm and not email:
                skipped += 1
                writer.writerow({
                    "action": "SKIP",
                    "person_id": "",
                    "name": name,
                    "match": "no_linkedin_no_email",
                    "linkedin_norm": linkedin_norm,
                    "email": email,
                    "detail": "No identifiers",
                })
                continue

            try:
                if args.dry_run:
                    if person_id:
                        updated += 1
                        writer.writerow({
                            "action": "UPDATE",
                            "person_id": person_id,
                            "name": name,
                            "match": match,
                            "linkedin_norm": linkedin_norm,
                            "email": email,
                            "detail": "DRY_RUN",
                        })
                        print("UPDATE", name)
                    else:
                        created += 1
                        writer.writerow({
                            "action": "CREATE",
                            "person_id": "",
                            "name": name,
                            "match": match or "none",
                            "linkedin_norm": linkedin_norm,
                            "email": email,
                            "detail": "DRY_RUN",
                        })
                        print("CREATE", name)
                    continue

                if person_id:
                    resp = client.update(person_id, payload)
                    updated += 1

                    # keep local index fresh from your input row (fast)
                    li_raw = payload.get("person[linkedin_url]", "")
                    em_raw = payload.get("person[email]", "")
                    db.upsert(person_id, li_raw, em_raw, updated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))

                    writer.writerow({
                        "action": "UPDATE",
                        "person_id": person_id,
                        "name": name,
                        "match": match,
                        "linkedin_norm": linkedin_norm,
                        "email": email,
                        "detail": "",
                    })
                else:
                    resp = client.create(payload)
                    new_id = parse_person_id_from_response(resp)
                    created += 1

                    if new_id:
                        li_raw = payload.get("person[linkedin_url]", "")
                        em_raw = payload.get("person[email]", "")
                        db.upsert(new_id, li_raw, em_raw, updated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))

                    writer.writerow({
                        "action": "CREATE",
                        "person_id": new_id or "",
                        "name": name,
                        "match": match or "none",
                        "linkedin_norm": linkedin_norm,
                        "email": email,
                        "detail": "",
                    })

            except Exception as e:
                errors += 1
                writer.writerow({
                    "action": "ERROR",
                    "person_id": person_id or "",
                    "name": name,
                    "match": match or "none",
                    "linkedin_norm": linkedin_norm,
                    "email": email,
                    "detail": str(e)[:1500],
                })
                print("ERROR", name, "-", str(e)[:200])

    print("DONE")
    print(f"Created: {created} | Updated: {updated} | Skipped: {skipped} | Errors: {errors}")
    print(f"Log: {args.log}")
    print(f"DB: {args.db}")

# ---------------------------
# Webhook server (keeps DB fresh)
# ---------------------------

def verify_signature_if_possible(payload_data: Dict[str, Any], secret: str) -> bool:
    """
    Loxo includes a 'signature' in payload_data.
    Their docs snippet you shared doesn't specify signing algorithm/secret derivation.
    If Loxo provides an explicit webhook secret, you can validate.

    This function uses a conservative guess: HMAC-SHA256 over canonical JSON of data WITHOUT 'signature'.
    If Loxo uses a different scheme, this will fail (so we keep it optional).
    """
    if not secret:
        return True
    sig = str(payload_data.get("signature") or "")
    if not sig:
        return False

    data_copy = dict(payload_data)
    data_copy.pop("signature", None)
    msg = json.dumps(data_copy, separators=(",", ":"), sort_keys=True).encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, sig)

def run_webhook_server(args):
    try:
        from fastapi import FastAPI, Request, Response
        import uvicorn
    except Exception:
        print("Missing deps for webhook server. Install:")
        print("  pip install fastapi uvicorn")
        sys.exit(2)

    token = os.environ.get("LOXO_API_TOKEN", "").strip()
    agency_slug = os.environ.get("LOXO_AGENCY_SLUG", "").strip()
    base_domain = os.environ.get("LOXO_BASE_DOMAIN", "app.loxo.co").strip()
    secret = os.environ.get("LOXO_WEBHOOK_SECRET", "").strip()

    if not token or not agency_slug:
        raise RuntimeError("Set LOXO_API_TOKEN and LOXO_AGENCY_SLUG env vars for webhook-server")

    db = IndexDB(args.db)
    client = LoxoClient(agency_slug, token, base_domain=base_domain)

    app = FastAPI()

    @app.post("/webhooks/loxo")
    async def loxo_webhook(request: Request):
        raw = await request.body()
        try:
            payload = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return Response(status_code=200, content="ok")

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return Response(status_code=200, content="ok")

        # Optional signature verification (only if you have a real secret)
        if secret and not verify_signature_if_possible(data, secret):
            # Return 200 to avoid retry storms; log for investigation
            with open("loxo_webhook_bad_signature.log", "a", encoding="utf-8") as f:
                f.write(raw.decode("utf-8", errors="replace") + "\n")
            return Response(status_code=200, content="ok")

        item_type = str(data.get("item_type") or "")
        action = str(data.get("action") or "")
        item_id = data.get("item_id")

        # Log all events
        with open("loxo_webhook_events.log", "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "data": data}) + "\n")

        if item_type != "person":
            return Response(status_code=200, content="ok")

        try:
            pid = int(item_id)
        except Exception:
            return Response(status_code=200, content="ok")

        try:
            if action == "destroy":
                db.delete(pid)
            elif action in ("create", "update"):
                person = client.get_person(pid)
                li = str(person.get("linkedin_url") or "")
                em = str(person.get("email") or person.get("email_address") or "")
                upd = str(person.get("updated_at") or data.get("timestamp") or "")
                db.upsert(pid, li, em, updated_at=upd)
        except Exception as e:
            with open("loxo_webhook_errors.log", "a", encoding="utf-8") as f:
                f.write(json.dumps({"pid": pid, "action": action, "err": str(e)}) + "\n")

        return Response(status_code=200, content="ok")

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")

# ---------------------------
# CLI
# ---------------------------

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db", help="Initialize SQLite index DB")
    p_init.add_argument("--db", default="loxo_index.sqlite")

    p_backfill = sub.add_parser("backfill-csv", help="Backfill SQLite index from Loxo CSV export")
    p_backfill.add_argument("--db", default="loxo_index.sqlite")
    p_backfill.add_argument("--csv", required=True, help="Path to exported CSV from Loxo")
    p_backfill.add_argument("--id-col", default="id", help="Column name for person id in CSV")
    p_backfill.add_argument("--linkedin-col", default="linkedin_url", help="Column name for linkedin url in CSV")
    p_backfill.add_argument("--email-col", default="email", help="Column name for email in CSV")
    p_backfill.add_argument("--list-cols", action="store_true", help="Print CSV columns and exit")

    p_import = sub.add_parser("import", help="Import file into Loxo using local DB index (no full fetch)")
    p_import.add_argument("--input", required=True, help="XLSX/CSV of candidates to upload")
    p_import.add_argument("--agency-slug", required=True)
    p_import.add_argument("--db", default="loxo_index.sqlite")
    p_import.add_argument("--base-domain", default="app.loxo.co")
    p_import.add_argument("--dry-run", action="store_true")
    p_import.add_argument("--log", default="loxo_upsert_log.csv")

    p_web = sub.add_parser("webhook-server", help="Run webhook receiver to keep DB fresh")
    p_web.add_argument("--db", default="loxo_index.sqlite")
    p_web.add_argument("--host", default="0.0.0.0")
    p_web.add_argument("--port", type=int, default=8000)

    args = p.parse_args()

    if args.cmd == "init-db":
        IndexDB(args.db)
        print(f"[OK] initialized {args.db}")
        return

    if args.cmd == "backfill-csv":
        db = IndexDB(args.db)
        backfill_from_csv(db, args.csv, args.id_col, args.linkedin_col, args.email_col, list_cols=args.list_cols)
        return

    if args.cmd == "import":
        run_import(args)
        return

    if args.cmd == "webhook-server":
        run_webhook_server(args)
        return

if __name__ == "__main__":
    main()