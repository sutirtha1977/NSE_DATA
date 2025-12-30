import sqlite3
import sys
from create_db import create_stock_database
from helper import DB_FILE


def main():
    # Create a fresh database
    create_stock_database(drop_existing=True)

    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    # Insert a symbol to satisfy FK
    cur.execute("INSERT INTO symbols (symbol) VALUES (?)", ("TESTSYMB",))
    symbol_id = cur.lastrowid
    conn.commit()

    # Insert a price_data row
    cur.execute(
        """INSERT INTO price_data (symbol_id, timeframe, date, open, high, low, close, adj_close, volume)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (symbol_id, "1d", "2025-01-01", 10.0, 10.0, 10.0, 10.0, 10.0, 1000),
    )
    conn.commit()

    # Attempt to insert a duplicate row (same symbol_id, timeframe, date)
    try:
        cur.execute(
            """INSERT INTO price_data (symbol_id, timeframe, date, open, high, low, close, adj_close, volume)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol_id, "1d", "2025-01-01", 11.0, 11.0, 11.0, 11.0, 11.0, 2000),
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        print("✅ IntegrityError raised on duplicate insert as expected:", e)
        conn.close()
        sys.exit(0)
    else:
        print("❌ Duplicate insert succeeded but should have failed (uniqueness not enforced).")
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
