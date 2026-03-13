import sqlite3

DB_PATH = "loxo_index.sqlite"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Total rows
    cur.execute("SELECT COUNT(*) FROM people_index")
    total_rows = cur.fetchone()[0]

    # Unique LinkedIn
    cur.execute("""
        SELECT COUNT(DISTINCT linkedin_norm)
        FROM people_index
        WHERE linkedin_norm IS NOT NULL AND linkedin_norm != ''
    """)
    unique_linkedin = cur.fetchone()[0]

    # Duplicate rows
    cur.execute("""
        SELECT IFNULL(SUM(cnt - 1),0)
        FROM (
            SELECT COUNT(*) as cnt
            FROM people_index
            WHERE linkedin_norm IS NOT NULL AND linkedin_norm != ''
            GROUP BY linkedin_norm
            HAVING COUNT(*) > 1
        )
    """)
    duplicate_rows = cur.fetchone()[0]

    # Profiles duplicated
    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT linkedin_norm
            FROM people_index
            WHERE linkedin_norm IS NOT NULL AND linkedin_norm != ''
            GROUP BY linkedin_norm
            HAVING COUNT(*) > 1
        )
    """)
    duplicate_profiles = cur.fetchone()[0]

    conn.close()

    print("\nSQLite Index Health Report")
    print("--------------------------")
    print(f"Total records:        {total_rows}")
    print(f"Unique LinkedIn:      {unique_linkedin}")
    print(f"Duplicate rows:       {duplicate_rows}")
    print(f"Profiles duplicated:  {duplicate_profiles}")
    print()

if __name__ == "__main__":
    main()