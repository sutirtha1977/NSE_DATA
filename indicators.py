# =========================================================
# THIS FILE CONTAINS THE FOLLOWING FUNCTIONS:
# 1. calculate_indicators
# 2. update_indicators
# 3. refresh_equity_partial_prices
# 4. refresh_equity_partial_indicators
# =========================================================
import pandas as pd
import numpy as np
import traceback
from datetime import datetime, timedelta, date, timezone
import time
from helper import (
    log, 
    DB_FILE,NSE_INDICES,
    FREQUENCIES,CSV_FILE,
    SKIP_MONTHLY,SKIP_WEEKLY
)
from indicators_helper import (
    calculate_rsi_series,
    calculate_bollinger,
    calculate_atr,
    calculate_macd,
    calculate_supertrend,
    calculate_ema,
    calculate_wma
)

# =========================================================
# calculate_indicators Function
# This function calculates various technical indicators for a given DataFrame
# containing price data. It can compute indicators for the entire DataFrame or
# just the latest row based on the 'latest_only' flag.
# =========================================================
def calculate_indicators(df, latest_only=False):
    try:
        # ---------------- SMA ----------------
        df["sma_20"] = df["adj_close"].rolling(20).mean().round(2)
        df["sma_50"] = df["adj_close"].rolling(50).mean().round(2)
        df["sma_200"] = df["adj_close"].rolling(200).mean().round(2)
        # ---------------- RSI ----------------
        df["rsi_3"] = calculate_rsi_series(df["close"], 3)
        df["rsi_9"] = calculate_rsi_series(df["close"], 9)
        df["rsi_14"] = calculate_rsi_series(df["close"], 14)
        # ---------------- Other Indicators ----------------
        df["ema_rsi_9_3"] = calculate_ema(df["rsi_9"], 3)
        df["wma_rsi_9_21"] = calculate_wma(df["rsi_9"], 21)
        # --------------- Bollinger Bands, ATR, Supertrend, MACD ----------------
        df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger(df["close"])
        df["atr_14"] = calculate_atr(df)
        df["supertrend"], df["supertrend_dir"] = calculate_supertrend(df)
        df["macd"], df["macd_signal"] = calculate_macd(df["close"])
        # --------------- Percentage Price Change ----------------
        df["pct_price_change"] = (df["adj_close"].pct_change() * 100).round(2)

        # ---- Return only last row if requested ----
        if latest_only:
            return df.iloc[[-1]].reset_index(drop=True)

        return df

    except Exception as e:
        log(f"CALCULATE INDICATORS FAILED | {e}")
        traceback.print_exc()
        return df  # return original df on failure

# ======================================================================================
# refresh_indicators Function
# This function calculates and updates technical indicators for equity or index symbols
# incrementally or fully based on the parameters provided.
# ======================================================================================

def refresh_indicators(conn, is_indexs=False, incremental=False, max_lookback=210):

    try:
        cur = conn.cursor()
        TIMEFRAMES = ["1d", "1wk", "1mo"]

        # Fetch symbols once
        table_symbols = "index_symbols" if is_indexs else "equity_symbols"
        cur.execute(f"SELECT {'index_id' if is_indexs else 'symbol_id'} FROM {table_symbols}")
        symbol_ids = [r[0] for r in cur.fetchall()]
        symbol_type = "index" if is_indexs else "equity"
        print(f"🔢 Total {symbol_type} symbols: {len(symbol_ids)}")

        all_records = []

        for timeframe in TIMEFRAMES:
            if SKIP_MONTHLY and timeframe == "1mo":
                log("Skipping 1mo (monthly-skip mode)")
                continue
            if SKIP_WEEKLY and timeframe == "1wk":
                log("Skipping 1wk (weekly-skip mode)")
                continue

            print(f"\n⏳ Processing timeframe: {timeframe}")
            tf_start = time.time()
            symbols_processed, tf_records = 0, 0

            for idx, symbol_id in enumerate(symbol_ids, start=1):
                if idx <= 5 or idx % 200 == 0:
                    print(f"  → {idx}/{len(symbol_ids)} symbols checked", flush=True)

                try:
                    # Determine last indicator date if incremental
                    last_date = None
                    if incremental:
                        indicator_table = "index_indicators" if is_indexs else "equity_indicators"
                        col_id = "index_id" if is_indexs else "symbol_id"
                        cur.execute(f"""
                            SELECT MAX(date) FROM {indicator_table}
                            WHERE {col_id} = ? AND timeframe = ?
                        """, (symbol_id, timeframe))
                        last_date = cur.fetchone()[0]

                    # ---------------- Load price data ----------------
                    price_table = "index_price_data" if is_indexs else "equity_price_data"
                    col_id = "index_id" if is_indexs else "symbol_id"

                    if incremental and last_date:
                        df = pd.read_sql(f"""
                            SELECT date, open, high, low, close, adj_close
                            FROM {price_table}
                            WHERE {col_id} = ? AND timeframe = ?
                              AND date >= (
                                  SELECT date
                                  FROM {price_table}
                                  WHERE {col_id} = ? AND timeframe = ?
                                  AND date <= ?
                                  ORDER BY date DESC
                                  LIMIT ?
                              )
                            ORDER BY date
                        """, conn, params=(symbol_id, timeframe, symbol_id, timeframe, last_date, max_lookback))
                    else:
                        df = pd.read_sql(f"""
                            SELECT date, open, high, low, close, adj_close
                            FROM {price_table}
                            WHERE {col_id} = ? AND timeframe = ?
                            ORDER BY date
                        """, conn, params=(symbol_id, timeframe))

                    if df.empty:
                        continue
                    # ---------------- Calculate indicators ----------------
                    calculate_indicators(df, False)

                    # Keep only new rows if incremental
                    if incremental and last_date:
                        df = df[df["date"] > last_date]

                    if df.empty:
                        continue
                    
                    # ---------------- Prepare records ----------------
                    for _, row in df.iterrows():
                        record = (
                            symbol_id, timeframe, row["date"],
                            row["sma_20"], row["sma_50"], row["sma_200"],
                            row["rsi_3"], row["rsi_9"], row["rsi_14"],
                            row["bb_upper"], row["bb_middle"], row["bb_lower"],
                            row["atr_14"], row["supertrend"], row["supertrend_dir"],
                            row.get("ema_rsi_9_3"), row.get("wma_rsi_9_21"), row.get("pct_price_change"),
                            row.get("macd"), row.get("macd_signal")
                        )
                        all_records.append(record)

                    symbols_processed += 1
                    tf_records += len(df)

                except Exception as e:
                    log(f"INDICATOR ERROR | symbol_id={symbol_id} timeframe={timeframe} | {e}")
                    traceback.print_exc()
                    continue

            elapsed = time.time() - tf_start
            print(f"  ✅ Timeframe {timeframe} done: {symbols_processed} symbols processed, {tf_records} rows prepared in {elapsed:.1f}s", flush=True)

        # # ---------------- Bulk UPSERT ----------------
        # if all_records:
        #     table = "index_indicators" if is_indexs else "equity_indicators"
        #     col_id = "index_id" if is_indexs else "symbol_id"
        #     placeholders = ",".join(["?"] * 20)  # 20 columns
        #     update_cols = ["sma_20","sma_50","sma_200","rsi_3","rsi_9","rsi_14",
        #                    "bb_upper","bb_middle","bb_lower","atr_14","supertrend","supertrend_dir",
        #                    "ema_rsi_9_3","wma_rsi_9_21","pct_price_change","macd","macd_signal"]
        #     update_sql = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

        #     cur.executemany(f"""
        #         INSERT INTO {table} ({col_id}, timeframe, date,
        #             sma_20,sma_50,sma_200,
        #             rsi_3,rsi_9,rsi_14,
        #             bb_upper,bb_middle,bb_lower,
        #             atr_14,supertrend,supertrend_dir,
        #             ema_rsi_9_3,wma_rsi_9_21,pct_price_change,
        #             macd,macd_signal)
        #         VALUES ({placeholders})
        #         ON CONFLICT({col_id}, timeframe, date) DO UPDATE SET {update_sql}
        #     """, all_records)
        #     conn.commit()
        #     print(f"\n✅ Indicators updated | Rows affected: {len(all_records)}")
        # ---------------- UPSERT row-by-row with progress ----------------
        table = "index_indicators" if is_indexs else "equity_indicators"
        col_id = "index_id" if is_indexs else "symbol_id"

        update_cols = ["sma_20","sma_50","sma_200","rsi_3","rsi_9","rsi_14",
                    "bb_upper","bb_middle","bb_lower","atr_14","supertrend","supertrend_dir",
                    "ema_rsi_9_3","wma_rsi_9_21","pct_price_change","macd","macd_signal"]
        update_sql = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

        insert_sql = f"""
            INSERT INTO {table} ({col_id}, timeframe, date,
                sma_20,sma_50,sma_200,
                rsi_3,rsi_9,rsi_14,
                bb_upper,bb_middle,bb_lower,
                atr_14,supertrend,supertrend_dir,
                ema_rsi_9_3,wma_rsi_9_21,pct_price_change,
                macd,macd_signal)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT({col_id}, timeframe, date) DO UPDATE SET {update_sql}
        """

        row_count = 0

        for record in all_records:
            try:
                cur.execute(insert_sql, record)
                row_count += 1
                if row_count % 500 == 0:   # print every 500 inserts, adjustable
                    print(f"   → inserted/upserted {row_count} indicator rows...")
            except Exception as e:
                print(f"   ❌ insert failed for {record[:3]} | {e}")

        conn.commit()
        print(f"\n🔔 Insert/UPSERT complete | Total rows processed: {row_count}")
    except Exception as e:
        log(f"INDICATOR UPDATE FAILED | {e}")
        traceback.print_exc()

# =========================================================
# refresh_equity_partial_prices Function
# This function refreshes partial weekly and monthly equity price data
# based on the latest daily data.
# Refresh partial weekly and monthly equity candles based on daily data.
# Logic:
# 1. Fetch last completed weekly & monthly candles from equity_price_data (is_final=True)
# 2. Clear any previous partial candles (is_final=False)
# 3. Load daily rows after or equal to last completed weekly/monthly (is_final=True)
# 4. Aggregate daily rows into partial weekly/monthly candles
# 5. Insert partial candles using latest daily date with is_final=False
# =========================================================
def refresh_equity_partial_prices(conn):

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

        # ---- latest daily date (is_final=True) -> this becomes partial candle date ----
        latest_daily_str = cur.execute("""
            SELECT MAX(date) FROM equity_price_data 
            WHERE timeframe='1d' AND is_final=1
        """).fetchone()[0]

        last_week = datetime.strptime(last_week_str, "%Y-%m-%d").date()
        last_month = datetime.strptime(last_month_str, "%Y-%m-%d").date()

        print(f"📌 Last completed weekly:  {last_week}")
        print(f"📌 Last completed monthly: {last_month}")
        print(f"📅 Partial candle date:   {latest_daily_str}")

        # --------------------------------------------------------
        # Always clear previous partial candles (is_final=False)
        # --------------------------------------------------------
        print("🧹 Clearing previous partial candles...")
        cur.execute("DELETE FROM equity_price_data WHERE is_final=0")
        conn.commit()

        # --------------------------------------------------------
        # Load daily rows from last completed weekly/monthly (inclusive) (is_final=True)
        # --------------------------------------------------------
        print("📥 Loading daily raw candles from last complete periods...")

        daily_rows = cur.execute("""
            SELECT symbol_id, date, open, high, low, close, adj_close
            FROM equity_price_data
            WHERE timeframe='1d' AND is_final=1
            AND (date >= ? OR date >= ?)
            ORDER BY symbol_id, date
        """, (last_week_str, last_month_str)).fetchall()

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
        # Insert partial candles using **latest daily date** (is_final=False)
        # --------------------------------------------------------
        rows = []

        for sid, v in weekly.items():
            rows.append((sid, "1wk", latest_daily_str, v["open"], v["high"],
                        v["low"], v["close"], v["adj_close"], 0))  # is_final=False

        for sid, v in monthly.items():
            rows.append((sid, "1mo", latest_daily_str, v["open"], v["high"],
                        v["low"], v["close"], v["adj_close"], 0))  # is_final=False

        print(f"💾 Partial weekly candles:  {len(weekly)}")
        print(f"💾 Partial monthly candles: {len(monthly)}")

        if rows:
            cur.executemany("""
                INSERT INTO equity_price_data
                (symbol_id, timeframe, date, open, high, low, close, adj_close, is_final)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()

        print("🎉 Partial price update COMPLETE – one partial candle per symbol!")

    except Exception as e:
        log(f"REFRESH PARTIAL PRICES FAILED | {e}")
        traceback.print_exc()

# =========================================================
# refresh_equity_partial_indicators Function
# This function rebuilds partial weekly & monthly indicators and stores them
# directly in equity_indicators with is_final = 0 (meaning: partial). 
# =========================================================
def refresh_equity_partial_indicators(conn):
    """
    Rebuild partial weekly & monthly indicators and store them directly in equity_indicators
    with is_final = 0 (meaning: partial).
    """
    print("➡️ Refreshing partial indicators...")
    cur = conn.cursor()

    # ---- 1️⃣ Remove old partial indicators ----
    try:
        cur.execute("BEGIN")
        cur.execute("DELETE FROM equity_indicators WHERE is_final = 0")
        conn.commit()
        print("🧹 Cleared old partial indicators from equity_indicators")
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to clear partial indicators | {e}")
        return

    # ---- 2️⃣ Load weekly & monthly prices (full + partial) ----
    df_prices = pd.read_sql("""
        SELECT symbol_id, timeframe, date, open, high, low, close, adj_close, volume
        FROM equity_price_data
        WHERE timeframe IN ('1wk', '1mo')
        ORDER BY symbol_id, timeframe, date
    """, conn)

    if df_prices.empty:
        print("⚠️ No weekly or monthly price data available — nothing to compute.")
        return

    # ---- 3️⃣ Compute partial indicators grouped ----
    calc_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    groups = []

    for (symbol_id, timeframe), group in df_prices.groupby(["symbol_id", "timeframe"]):
        calc_group = group[calc_cols].copy().sort_values("date")

        # Compute indicators but keep only latest row
        ind = calculate_indicators(calc_group, latest_only=True)

        ind["symbol_id"] = symbol_id
        ind["timeframe"] = timeframe
        ind["is_final"] = 0  # <---- IMPORTANT

        groups.append(ind)

    df_ind = pd.concat(groups, ignore_index=True)

    # ---- 4️⃣ Restrict to allowed columns ----
    allowed_columns = [
        "symbol_id", "timeframe", "date", "is_final",
        "sma_20", "sma_50", "sma_200",
        "rsi_3", "rsi_9", "rsi_14",
        "ema_rsi_9_3", "wma_rsi_9_21",
        "pct_price_change",
        "macd", "macd_signal",
        "bb_upper", "bb_middle", "bb_lower",
        "atr_14",
        "supertrend", "supertrend_dir"
    ]
    df_out = df_ind[[c for c in allowed_columns if c in df_ind.columns]]

    print(f"📌 {len(df_out)} latest partial indicator rows generated")
    # ---- 5️⃣ Insert partial indicators into main table ----
    try:
        cur.execute("BEGIN")

        insert_sql = """
            INSERT OR REPLACE INTO equity_indicators (
                symbol_id, timeframe, date, is_final,
                sma_20, sma_50, sma_200,
                rsi_3, rsi_9, rsi_14,
                ema_rsi_9_3, wma_rsi_9_21,
                pct_price_change,
                macd, macd_signal,
                bb_upper, bb_middle, bb_lower,
                atr_14,
                supertrend, supertrend_dir
            ) VALUES (
                :symbol_id, :timeframe, :date, :is_final,
                :sma_20, :sma_50, :sma_200,
                :rsi_3, :rsi_9, :rsi_14,
                :ema_rsi_9_3, :wma_rsi_9_21,
                :pct_price_change,
                :macd, :macd_signal,
                :bb_upper, :bb_middle, :bb_lower,
                :atr_14,
                :supertrend, :supertrend_dir
            )
        """

        cur.executemany(insert_sql, df_out.to_dict(orient="records"))
        conn.commit()

        print(f"✅ {len(df_out)} partial indicator rows inserted/replaced successfully")

    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to insert partial indicators | {e}")
        traceback.print_exc()
        
        

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