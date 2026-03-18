#!/usr/bin/env python3

import sqlite3
import pandas as pd

DB = "loxo_index.sqlite"

def main():

    conn = sqlite3.connect(DB)

    df = pd.read_sql_query("""
    SELECT linkedin_norm, person_id
    FROM people_index
    WHERE linkedin_norm IS NOT NULL
      AND linkedin_norm != ''
    """, conn)

    conn.close()

    # Find duplicates
    dupes = df[df.duplicated("linkedin_norm", keep=False)].copy()

    # Mark keeper (lowest person_id per linkedin)
    dupes["keep"] = dupes.groupby("linkedin_norm")["person_id"].transform(
        lambda x: x == x.min()
    )

    # Sort nicely
    dupes = dupes.sort_values(["linkedin_norm", "person_id"])

    # Export
    dupes.to_csv("loxo_duplicates_fast.csv", index=False)
    dupes.to_excel("loxo_duplicates_fast.xlsx", index=False)

    print()
    print("DONE — instant export")
    print("loxo_duplicates_fast.csv")
    print("loxo_duplicates_fast.xlsx")
    print()
    print(f"Total duplicate rows: {len(dupes)}")
    print(f"Unique duplicate groups: {dupes['linkedin_norm'].nunique()}")


if __name__ == "__main__":
    main()