
### To be Deleted LATER: ###
from data_manager import (
    get_db_connection,
    close_db_connection
)

def check(conn, start_date=None):
    """
    Refresh partial weekly and monthly candles from daily data.
    
    Args:
        conn: SQLite3 or compatible DB connection.
        start_date (str or datetime.date, optional): Start date for calculations (inclusive).
            If None, defaults to last completed weekly/monthly candle date.
            Format: 'YYYY-MM-DD' or datetime.date
    """
    cur = conn.cursor()
    try:
        print("🔍 Fetching last completed weekly & monthly candles...")

        # ---- completed weekly high-water mark (is_final=True) ----
        last_week_str = cur.execute("""
            SELECT MAX(date) FROM equity_price_data 
            WHERE timeframe='1wk' AND is_final=1
        """).fetchone()[0]

        # ---- completed monthly high-water mark (is_final=True) ----
        last_month_str = cur.execute("""
            SELECT MAX(date) FROM equity_price_data 
            WHERE timeframe='1mo' AND is_final=1
        """).fetchone()[0]

        last_week = datetime.strptime(last_week_str, "%Y-%m-%d").date()
        last_month = datetime.strptime(last_month_str, "%Y-%m-%d").date()

        # Use input start_date if provided
        if start_date:
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            print(f"📅 Using start date for calculation: {start_date}")
        else:
            start_date = min(last_week, last_month)
            print(f"📅 Using default start date: {start_date}")

        print(f"📌 Last completed weekly:  {last_week}")
        print(f"📌 Last completed monthly: {last_month}")

        # --------------------------------------------------------
        # Always clear previous partial candles (is_final=False)
        # --------------------------------------------------------
        print("🧹 Clearing previous partial candles...")
        cur.execute("DELETE FROM equity_price_data WHERE is_final=0")
        conn.commit()

        # --------------------------------------------------------
        # Load daily rows from start_date (inclusive)
        # --------------------------------------------------------
        print("📥 Loading daily raw candles from start date...")

        daily_rows = cur.execute("""
            SELECT symbol_id, date, open, high, low, close, adj_close
            FROM equity_price_data
            WHERE timeframe='1d' AND is_final=1
            AND date >= ?
            ORDER BY symbol_id, date
        """, (start_date,)).fetchall()

        # --------------------------------------------------------
        # Aggregate into ONE partial weekly & ONE partial monthly candle
        # --------------------------------------------------------
        weekly = {}
        monthly = {}

        for sid, ds, o, h, l, c, adj in daily_rows:
            d = datetime.strptime(ds, "%Y-%m-%d").date()

            if d >= last_week:
                w = weekly.setdefault(sid, dict(open=o, high=h, low=l, close=c, adj_close=adj))
                w["high"] = max(w["high"], h)
                w["low"]  = min(w["low"], l)
                w["close"] = c
                w["adj_close"] = adj

            if d >= last_month:
                m = monthly.setdefault(sid, dict(open=o, high=h, low=l, close=c, adj_close=adj))
                m["high"] = max(m["high"], h)
                m["low"]  = min(m["low"], l)
                m["close"] = c
                m["adj_close"] = adj

        # --------------------------------------------------------
        # Insert partial candles using **start_date** (is_final=False)
        # --------------------------------------------------------
        rows = []

        # Convert start_date back to string for DB insertion
        start_date_str = start_date.strftime("%Y-%m-%d")

        for sid, v in weekly.items():
            rows.append((sid, "1wk", start_date_str, v["open"], v["high"],
                        v["low"], v["close"], v["adj_close"], 0))  # is_final=False

        for sid, v in monthly.items():
            rows.append((sid, "1mo", start_date_str, v["open"], v["high"],
                        v["low"], v["close"], v["adj_close"], 0))  # is_final=False

        print(f"💾 Partial weekly candles:  {len(weekly)}")
        print(f"💾 Partial monthly candles: {len(monthly)}")

        if rows:
            cur.executemany("""
                INSERT OR REPLACE INTO equity_price_data
                (symbol_id, timeframe, date, open, high, low, close, adj_close, is_final)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()

        print("🎉 Partial price update COMPLETE – one partial candle per symbol!")

    except Exception as e:
        print(f"REFRESH PARTIAL PRICES FAILED | {e}")
        traceback.print_exc()

if __name__ == "__main__":
    conn = get_db_connection()
    check(conn,"2025-12-22")
    close_db_connection(conn)