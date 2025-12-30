# =========================================================
# THIS FILE CONTAINS THE FOLLOWING FUNCTIONS:
# 1. get_db_connection
# 2. close_db_connection
# 3. get_last_price_date
# 4. retrieve_equity_symbol
# 5. insert_equity_price_data
# 6. refresh_equity
# 7. refresh_indices
# 8. download_equity_price_data_all_timeframes
# 9. download_index_price_data_all_timeframes
# 10. refresh_52week_stats
# =========================================================
import sqlite3
import yfinance as yf
import pandas as pd
import traceback
import time
import socket
import requests
import csv
from datetime import datetime, timedelta, timezone
from urllib3.exceptions import ReadTimeoutError as URLLibReadTimeout
from helper import (
    log, 
    DB_FILE,NSE_INDICES,
    FREQUENCIES,CSV_FILE,MISSING_EQUITY,MISSING_INDEX,
    SKIP_MONTHLY,SKIP_WEEKLY
)
from sql import (
    SQL_MAP
)

# OPEN DATABSE CONNECTION
def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn
    except Exception as e:
        log(f"DB CONNECTION FAILED: {e}")
        raise
    
# CLOSE DATABSE CONNECTION
def close_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        log(f"DB CLOSE FAILED: {e}")
        
# GET LAST STORED DATE (INCREMENTAL)
def get_last_price_date(conn, symbol_id, timeframe):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT MAX(date)
            FROM equity_price_data
            WHERE symbol_id = ? AND timeframe = ?
        """, (symbol_id, timeframe))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        log(f"LAST DATE FETCH FAILED: {e}")
        return None

# FETCH SYMBOLS
def retrieve_equity_symbol(symbol, conn):
    """
    symbol = 'ALL' or 'RELIANCE' or 'RELIANCE,TCS,INFY'
    """
    try:
        if symbol.upper() == "ALL":
            return pd.read_sql(
                "SELECT symbol_id, symbol FROM equity_symbols ORDER BY symbol",
                conn
            )

        symbols = [s.strip().upper() for s in symbol.split(",")]
        placeholders = ",".join("?" * len(symbols))

        query = f"""
            SELECT symbol_id, symbol
            FROM equity_symbols
            WHERE symbol IN ({placeholders})
            ORDER BY symbol
        """
        return pd.read_sql(query, conn, params=symbols)

    except Exception as e:
        log(f"RETRIEVE SYMBOL FAILED: {e}")
        return pd.DataFrame()

# INSERT PRICE DATA
def insert_equity_price_data(df, symbol_id, timeframe, conn):
    try:
        cur = conn.cursor()
        rows = []

        # Ensure DateTimeIndex
        df = df.copy()
        df.index = pd.to_datetime(df.index)

        for idx, r in df.iterrows():
            rows.append((
                symbol_id,
                timeframe,
                idx.strftime("%Y-%m-%d"),
                round(float(r["Open"].iloc[0]),2),
                round(float(r["High"].iloc[0]),2),
                round(float(r["Low"].iloc[0]),2),
                round(float(r["Close"].iloc[0]),2),
                round(float(r["Adj Close"].iloc[0]),2),
                round(float(r["Volume"].iloc[0]),2)
            ))

        cur.executemany("""
            INSERT OR IGNORE INTO equity_price_data (
                symbol_id, timeframe, date,
                open, high, low, close, adj_close, volume
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()

    except Exception as e:
        log("PRICE INSERT FAILED")
        traceback.print_exc()

# REFRESH EQUITY
# https://www.nseindia.com/static/market-data/securities-available-for-trading
def refresh_equity(conn):
    try:
        df = pd.read_csv(CSV_FILE)

        # ---------- Column detection ----------
        symbol_col = next((c for c in df.columns if c.lower() == 'symbol'), 'Symbol')
        name_col = next((c for c in df.columns if c.lower() in ('stock name', 'name')), 'Stock Name')

        series_candidates = [c for c in df.columns if 'series' in c.lower()]
        series_col = series_candidates[0] if series_candidates else None

        listing_candidates = [
            c for c in df.columns
            if 'list' in c.lower() and 'date' in c.lower()
        ]
        listing_col = listing_candidates[0] if listing_candidates else None

        isin_candidates = [c for c in df.columns if 'isin' in c.lower()]
        isin_col = isin_candidates[0] if isin_candidates else None

        # ---------- Build column list ----------
        cols = [symbol_col, name_col]
        if series_col:
            cols.append(series_col)
        if listing_col:
            cols.append(listing_col)
        if isin_col:
            cols.append(isin_col)

        iterable = (
            df[cols]
            .dropna(subset=[symbol_col, name_col])
            .drop_duplicates()
        )

        records = []
        updates_series = []
        updates_listing = []
        updates_isin = []

        # ---------- Row processing ----------
        for _, row in iterable.iterrows():
            symbol = str(row[symbol_col]).strip().upper()
            name = str(row[name_col]).strip()

            # Series
            series = None
            if series_col:
                raw = row.get(series_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        series = s

            # Listing date
            listing_date = None
            if listing_col:
                raw = row.get(listing_col)
                if pd.notna(raw):
                    dt = pd.to_datetime(raw, errors='coerce')
                    if pd.notna(dt):
                        listing_date = dt.date().isoformat()

            # ISIN
            isin = None
            if isin_col:
                raw = row.get(isin_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        isin = s

            records.append(
                (symbol, name, 'NSE', series, listing_date, isin)
            )

            if series:
                updates_series.append((series, symbol))
            if listing_date:
                updates_listing.append((listing_date, symbol))
            if isin:
                updates_isin.append((isin, symbol))

        # ---------- Database write ----------
        if records:
            conn.executemany("""
                INSERT OR IGNORE INTO equity_symbols
                (symbol, name, exchange, series, listing_date, isin)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)

            if updates_series:
                conn.executemany(
                    "UPDATE equity_symbols SET series = ? "
                    "WHERE symbol = ? AND (series IS NULL OR series = '')",
                    updates_series
                )

            if updates_listing:
                conn.executemany(
                    "UPDATE equity_symbols SET listing_date = ? "
                    "WHERE symbol = ? AND listing_date IS NULL",
                    updates_listing
                )

            if updates_isin:
                conn.executemany(
                    "UPDATE equity_symbols SET isin = ? "
                    "WHERE symbol = ? AND (isin IS NULL OR isin = '')",
                    updates_isin
                )

            conn.commit()

            log(
                f"Inserted {len(records)} symbols | "
                f"Updated series:{len(updates_series)}, "
                f"listing_date:{len(updates_listing)}, "
                f"isin:{len(updates_isin)}"
            )
        else:
            log("No symbol records to insert")

    except Exception as e:
        log(f"Error refreshing stock symbols: {e}")
        raise
    
# REFRESH INDICES
def refresh_indices(conn):
    # Safety check: ensure schema matches expectations
    cols = {row[1] for row in conn.execute("PRAGMA table_info(index_symbols)")}
    required = {
        "index_id",
        "index_code",
        "index_name",
        "exchange",
        "yahoo_symbol",
        "category",
        "is_active"
    }

    if not required.issubset(cols):
        raise RuntimeError(
            f"index_symbols table schema mismatch. Found columns: {cols}"
        )
    # Prepare records
    # NSE_INDICES format:
    # (index_code, index_name, exchange, yahoo_symbol, category)
    records = [
        (code, name, exch, yahoo, category, 1)
        for (code, name, exch, yahoo, category) in NSE_INDICES
    ]

    # Insert new indices
    conn.executemany("""
        INSERT OR IGNORE INTO index_symbols
        (index_code, index_name, exchange, yahoo_symbol, category, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
    """, records)

    # Reactivate indices already present
    conn.executemany("""
        UPDATE index_symbols
        SET is_active = 1
        WHERE index_code = ?
    """, [(r[0],) for r in records])

    conn.commit()
    log(f"Index symbols refreshed: {len(records)} total")

# DOWNLOAD EQUITY SYMBOLS FROM YAHOO FINANCE
def download_equity_price_data_all_timeframes(conn, symbol, daily_dt, weekly_dt, monthly_dt):
    try:
        symbols_df = retrieve_equity_symbol(symbol, conn)

        if symbols_df.empty:
            log("NO SYMBOLS FOUND")
            return

        for timeframe in FREQUENCIES:
            # MONTHLY SKIP CONTROL
            if SKIP_MONTHLY and timeframe == "1mo":
                log("Skipping 1mo (monthly-skip mode)")
                continue
            # WEEKLY SKIP CONTROL
            if SKIP_WEEKLY and timeframe == "1wk":
                log("Skipping 1wk (weekly-skip mode)")
                continue
            
            # end_dt = {"1d": daily_dt, "1wk": weekly_dt, "1mo": monthly_dt}.get(timeframe)
            end_dt = (
                        datetime.strptime({"1d": daily_dt, "1wk": weekly_dt, "1mo": monthly_dt}[timeframe], "%Y-%m-%d")
                        + timedelta(days=1)
                    ).strftime("%Y-%m-%d")
            
            log(f"===== FETCHING {timeframe} DATA =====")

            for _, row in symbols_df.iterrows():
                symbol_id = row["symbol_id"]
                symbol_name = row["symbol"]

                try:
                    last_date = get_last_price_date(conn, symbol_id, timeframe)

                    # If incremental run and computed start date is in the future, skip
                    if last_date is not None:
                        try:
                            next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).date()
                            today = datetime.now(timezone.utc).date()
                            if next_date > today:
                                log(f"{symbol_name} | {timeframe} | NO NEW DATA (next_date {next_date} > today {today})")
                                continue
                        except Exception as e:
                            log(f"{symbol_name} | {timeframe} | WARN: could not parse last_date '{last_date}': {e}")

                    # Download data (single attempt)
                    if last_date is None:
                        log(f"{symbol_name} | {timeframe} | FULL DOWNLOAD")
                        try:
                            df = yf.download(
                                f"{symbol_name}.NS",
                                period="max",
                                interval=timeframe,
                                end=end_dt,
                                auto_adjust=False,
                                progress=False
                            )
                        except Exception as e:
                            # fallback; log anything unexpected
                            log(f"{symbol_name} | {timeframe} | FAILED: {e}")
                    else:
                        start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                        log(f"{symbol_name} | {timeframe} | FROM {start_date}")
                        df = yf.download(
                            f"{symbol_name}.NS",
                            interval=timeframe,
                            start=start_date,
                            end=end_dt,
                            auto_adjust=False,
                            progress=False
                        )

                    if df is None or df.empty:
                        log(f"{symbol_name} | {timeframe} | NO NEW DATA")
                        continue

                    insert_equity_price_data(df, symbol_id, timeframe, conn)
                    log(f"{symbol_name} | {timeframe} | UPDATED ({len(df)})")

                except Exception as e:
                    # fallback; log anything unexpected
                    log(f"{symbol_name} | {timeframe} | FAILED: {e}")

        log("✅ PRICE DATA UPDATE COMPLETED")

    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")

    finally:
        close_db_connection(conn)
        
# DOWNLOAD INDEX SYMBOLS FROM YAHOO FINANCE
def download_index_price_data_all_timeframes(conn,daily_dt,weekly_dt,monthly_dt,lookback_years=20):   

    cur = conn.cursor()

    # --------------------------------------------------
    # Active indices
    # --------------------------------------------------
    cur.execute("""
        SELECT index_id, index_code, yahoo_symbol
        FROM index_symbols
        WHERE is_active = 1
    """)
    indices = cur.fetchall()

    if not indices:
        log("No active indices found")
        return

    total_rows = 0

    for index_id, index_code, yahoo_symbol in indices:
        for timeframe in FREQUENCIES:
            end_dt = (
                        datetime.strptime({"1d": daily_dt, "1wk": weekly_dt, "1mo": monthly_dt}[timeframe], "%Y-%m-%d")
                        + timedelta(days=1)
                    ).strftime("%Y-%m-%d")
            # MONTHLY SKIP CONTROL
            if SKIP_MONTHLY and timeframe == "1mo":
                log("Skipping 1mo (monthly-skip mode)")
                continue
            # WEEKLY SKIP CONTROL
            if SKIP_WEEKLY and timeframe == "1wk":
                log("Skipping 1wk (weekly-skip mode)")
                continue
            
            log(f"===== FETCHING {timeframe} DATA =====")
            try:
                # --------------------------------------
                # Last stored date
                # --------------------------------------
                cur.execute("""
                    SELECT MAX(date)
                    FROM index_price_data
                    WHERE index_id = ? AND timeframe = ?
                """, (index_id, timeframe))
                last_date = cur.fetchone()[0]

                if last_date:
                    last_dt = pd.to_datetime(last_date)

                    # Daily → next day
                    if timeframe == "1d":
                        start = last_dt + pd.Timedelta(days=1)
                    # Weekly / Monthly → overlap allowed
                    else:
                        start = last_dt
                else:
                    start = datetime.now() - pd.DateOffset(years=lookback_years)

                # Skip daily if already current
                if timeframe == "1d" and start.date() > datetime.now().date():
                    log(f"{index_code} [{timeframe}] → already up-to-date")
                    continue

                # --------------------------------------
                # Download from Yahoo
                # --------------------------------------
                df = yf.download(
                    yahoo_symbol,
                    start=start.strftime("%Y-%m-%d"),
                    interval=timeframe,
                    end=end_dt,
                    auto_adjust=False,
                    progress=False
                )

                if df.empty:
                    log(f"{index_code} [{timeframe}] → no new data")
                    continue

                # --------------------------------------
                # CRITICAL: Normalize Yahoo output
                # --------------------------------------
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0] for c in df.columns]

                df.reset_index(inplace=True)

                # Normalize date column safely
                date_col = df.columns[0]
                df[date_col] = pd.to_datetime(df[date_col]).dt.date

                # --------------------------------------
                # Normalize & insert (deduplicated)
                # --------------------------------------
                records = []

                for _, row in df.iterrows():
                    trade_date = row[date_col].isoformat()

                    # Deduplicate overlapping candles
                    if last_date is not None and trade_date <= last_date:
                        continue

                    records.append((
                        index_id,
                        timeframe,
                        trade_date,
                        float(row["Open"]) if pd.notna(row["Open"]) else None,
                        float(row["High"]) if pd.notna(row["High"]) else None,
                        float(row["Low"]) if pd.notna(row["Low"]) else None,
                        float(row["Close"]) if pd.notna(row["Close"]) else None,
                        float(row["Adj Close"])
                        if "Adj Close" in row and pd.notna(row["Adj Close"])
                        else None
                    ))

                if not records:
                    log(f"{index_code} [{timeframe}] → nothing new")
                    continue

                # --------------------------------------
                # Insert
                # --------------------------------------
                cur.executemany("""
                    INSERT OR IGNORE INTO index_price_data
                    (index_id, timeframe, date, open, high, low, close, adj_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, records)

                conn.commit()
                total_rows += len(records)

                log(
                    f"{index_code} [{timeframe}] → "
                    f"{len(records)} new rows"
                )

            except Exception as e:
                log(f"❌ {index_code} [{timeframe}] failed: {e}")

    log(f"✅ Index price update complete (incremental). Total rows: {total_rows}")
    
# 52 WEEK HIGH AND LOW REFRESH
def refresh_52week_stats(conn, type_):
    """
    Update 52-week high and low for equity or index symbols.
    
    type_: "equity" or "index"
    """
    cur = conn.cursor()
    try:
        # -----------------------------------------------------
        # Parameterize table/column names
        # -----------------------------------------------------
        if type_ == 'index':
            table_price = "index_price_data"
            table_52w = "index_52week_stats"
            col_id = "index_id"
        else:
            table_price = "equity_price_data"
            table_52w = "equity_52week_stats"
            col_id = "symbol_id"

        # -----------------------------------------------------
        # Detect first run
        # -----------------------------------------------------
        first_run = cur.execute(f"SELECT COUNT(*) FROM {table_52w}").fetchone()[0] == 0

        if first_run:
            log(f"📊 52W STATS ({type_}): FIRST RUN (full rebuild)")
            cur.execute(f"SELECT DISTINCT {col_id} FROM {table_price} WHERE timeframe='1d'")
        else:
            log(f"📊 52W STATS ({type_}): INCREMENTAL UPDATE")
            # incremental = only symbols updated today
            cur.execute(f"SELECT DISTINCT {col_id} FROM {table_price} WHERE timeframe='1d' AND date=date('now')")

        symbol_ids = [r[0] for r in cur.fetchall()]
        if not symbol_ids:
            log("📊 52W STATS: nothing to update")
            return

        # -----------------------------------------------------
        # Compute 52-week high/low for all symbols in one query
        # -----------------------------------------------------
        placeholder = ",".join("?" for _ in symbol_ids)
        cur.execute(f"""
            SELECT {col_id}, MAX(high), MIN(low)
            FROM {table_price}
            WHERE timeframe='1d' AND {col_id} IN ({placeholder})
              AND date >= date('now', '-1 year')
            GROUP BY {col_id}
        """, symbol_ids)

        rows_to_upsert = []
        for symbol_id, high_52, low_52 in cur.fetchall():
            if high_52 is not None:
                rows_to_upsert.append((symbol_id, high_52, low_52))

        if not rows_to_upsert:
            log("📊 52W STATS: no data to update after 1-year filter")
            return

        # -----------------------------------------------------
        # Batch upsert
        # -----------------------------------------------------
        for symbol_id, high_52, low_52 in rows_to_upsert:
            cur.execute(f"""
                INSERT INTO {table_52w} ({col_id}, week52_high, week52_low, as_of_date)
                VALUES (?, ?, ?, date('now'))
                ON CONFLICT({col_id}) DO UPDATE SET
                    week52_high = excluded.week52_high,
                    week52_low  = excluded.week52_low,
                    as_of_date  = excluded.as_of_date
            """, (symbol_id, high_52, low_52))

        conn.commit()
        log(f"✅ 52W STATS UPDATED: {len(rows_to_upsert)} symbols")

    except Exception as e:
        conn.rollback()
        log(f"❌ 52W STATS UPDATE FAILED: {e}")

    finally:
        cur.close()