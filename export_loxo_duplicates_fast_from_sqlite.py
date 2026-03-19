#!/usr/bin/env python3

import sqlite3
import pandas as pd

DB = "loxo_index.sqlite"

def main():
    conn = sqlite3.connect(DB)

    df = pd.read_sql_query("""
    SELECT
        linkedin_norm,
        person_id,
        full_name,
        company_name,
        candidate_location,
        job_title,
        email_norm
    FROM people_index
    WHERE linkedin_norm IS NOT NULL
      AND linkedin_norm != ''
    """, conn)

    conn.close()

    dupes = df[df.duplicated("linkedin_norm", keep=False)].copy()

    dupes["keep"] = dupes.groupby("linkedin_norm")["person_id"].transform(
        lambda x: x == x.min()
    )

    dupes = dupes.sort_values(["linkedin_norm", "person_id"])

    dupes.to_csv("loxo_duplicates_rich.csv", index=False)
    dupes.to_excel("loxo_duplicates_rich.xlsx", index=False)

    print()
    print("DONE — rich duplicate export")
    print("loxo_duplicates_rich.csv")
    print("loxo_duplicates_rich.xlsx")
    print()
    print(f"Total duplicate rows: {len(dupes)}")
    print(f"Unique duplicate groups: {dupes['linkedin_norm'].nunique()}")

if __name__ == "__main__":
    main()