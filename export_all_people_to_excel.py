#!/usr/bin/env python3

import sqlite3
import pandas as pd

DB = "loxo_index.sqlite"
OUT = "loxo_all_people.xlsx"

def main():
    conn = sqlite3.connect(DB)

    df = pd.read_sql_query("""
        SELECT *
        FROM people_index
        ORDER BY person_id
    """, conn)

    conn.close()

    df.to_excel(OUT, index=False)

    print(f"Done: wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()