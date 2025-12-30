"""
scanner_HM.py

Specialized scanner: iterate trading dates backwards from the latest date and collect dates where
there are matches for the HM criteria. Stop when a date has zero matches.

Criteria (same as scanner.py):
1. Current close >= 100
2. Monthly rsi(3) > 60
3. Weekly rsi(3) > 60
4. Daily rsi(3) crossed above 55 (yesterday < 55, today >= 55)
5. Daily rsi(9) >= daily ema(rsi(9),3)
6. Daily ema(rsi(9),3) >= daily wma(rsi(9),21)
7. daily rsi(9) / ema(rsi(9),3) >= 1.2
8. weekly rsi(9) >= weekly ema(rsi(9),3)
9. weekly ema(rsi(9),3) >= weekly wma(rsi(9),21)
10. Daily percentage change less than 10%

Output: Excel file with summary and a sheet per matching date + an aggregate sheet of matches

Usage:
    ./.venv/bin/python scanner_HM.py --excel hm_matches.xlsx

"""
import sqlite3
import argparse
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from helper import DB_FILE, log
# from tabulate import tabulate
from rich.console import Console
from rich.table import Table

try:
    import pandas as pd
except Exception:
    pd = None

console = Console()

def week_start(date_str: str) -> str:
    """Return Monday of the week for given date"""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    monday = d - timedelta(days=d.weekday())
    return monday.strftime("%Y-%m-%d")

def month_start(date_str: str) -> str:
    """Return first day of month for given date"""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    first = d.replace(day=1)
    return first.strftime("%Y-%m-%d")

def fetch_latest_price_adj_close(conn: sqlite3.Connection, symbol_id: int, as_of: Optional[str] = None) -> Optional[float]:
    cur = conn.cursor()
    if as_of:
        cur.execute("""
            SELECT adj_close FROM equity_price_data
            WHERE symbol_id = ? AND timeframe = '1d' AND date <= ?
            ORDER BY date DESC LIMIT 1
        """, (symbol_id, as_of))
    else:
        cur.execute("""
            SELECT adj_close FROM equity_price_data
            WHERE symbol_id = ? AND timeframe = '1d'
            ORDER BY date DESC LIMIT 1
        """, (symbol_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def fetch_latest_indicators(conn: sqlite3.Connection, symbol_id: int, timeframe: str, limit: int = 1, as_of: Optional[str] = None) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    params = [symbol_id, timeframe]
    date_clause = ""
    if as_of:
        date_clause = "AND date <= ?"
        params.append(as_of)
    sql = f"""
        SELECT date, rsi_3, rsi_9, ema_rsi_9_3, wma_rsi_9_21,pct_price_change
        FROM equity_indicators
        WHERE symbol_id = ? AND timeframe = ? {date_clause}
        ORDER BY date DESC
        LIMIT {limit}
    """
    cur.execute(sql, tuple(params))
    cols = ["date", "rsi_3", "rsi_9", "ema_rsi_9_3", "wma_rsi_9_21","pct_price_change"]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def scan_as_of(conn: sqlite3.Connection, as_of: str, limit: int = 0) -> List[Dict[str, Any]]:
    """Run the HM criteria scan as of a particular date and return list of matched records."""
    cur = conn.cursor()
    cur.execute("SELECT symbol_id, symbol, name FROM equity_symbols WHERE series IN ('EQ', 'BE', 'BZ') ORDER BY symbol")
    symbols = cur.fetchall()

    matches = []

    for symbol_id, symbol, name in symbols:
        if limit and len(matches) >= limit:
            break

        price = fetch_latest_price_adj_close(conn, symbol_id, as_of=as_of)
        # Current close >= 100
        if price is None or price < 100:
            continue

        as_of_week = week_start(as_of)
        as_of_month = month_start(as_of)

        monthly = fetch_latest_indicators(
            conn, symbol_id, "1mo", limit=1, as_of=as_of_month
        )
        
        weekly = fetch_latest_indicators(
            conn, symbol_id, "1wk", limit=1, as_of=as_of_week
        )
        daily_rows = fetch_latest_indicators(conn, symbol_id, "1d", limit=2, as_of=as_of)
        # Monthly rsi(3) > 60
        # Weekly rsi(3) > 60
        if not monthly or monthly[0].get("rsi_3") is None or monthly[0]["rsi_3"] <= 60:
            continue
        if not weekly or weekly[0].get("rsi_3") is None or weekly[0]["rsi_3"] <= 60:
            continue
        # Daily rsi(3) crossed above 60 (yesterday < 60, today >= 60)
        if not daily_rows or len(daily_rows) < 1:
            continue
        today = daily_rows[0]
        yesterday = daily_rows[1] if len(daily_rows) > 1 else None

        if today.get("rsi_3") is None or today["rsi_3"] < 55:
            continue
        if yesterday is None or yesterday.get("rsi_3") is None or yesterday["rsi_3"] >= 55:
            continue
        # Daily rsi(9) >= daily ema(rsi(9),3)
        if today.get("rsi_9") is None or today.get("ema_rsi_9_3") is None or today["rsi_9"] < today["ema_rsi_9_3"]:
            continue
        # Daily ema(rsi(9),3) >= daily wma(rsi(9),21)
        if today.get("ema_rsi_9_3") is None or today.get("wma_rsi_9_21") is None or today["ema_rsi_9_3"] < today["wma_rsi_9_21"]:
            continue
        # daily rsi(9) / ema(rsi(9),3) >= 1.2
        if today.get("ema_rsi_9_3") == 0:
            continue
        if float(today.get("rsi_9", 0)) / float(today.get("ema_rsi_9_3", 1)) < 1.2:
            continue
        # weekly rsi(9) >= weekly ema(rsi(9),3)
        w = weekly[0]
        if w.get("rsi_9") is None or w.get("ema_rsi_9_3") is None or w["rsi_9"] < w["ema_rsi_9_3"]:
            continue
        # weekly ema(rsi(9),3) >= weekly wma(rsi(9),21)
        if w.get("wma_rsi_9_21") is None or w["ema_rsi_9_3"] < w["wma_rsi_9_21"]:
            continue

        matches.append({
            "symbol_id": symbol_id,
            "symbol": symbol,
            "name": name,
            "price": price,
            "daily_date": today.get("date"),
            "daily_rsi_3": today.get("rsi_3"),
            "daily_rsi_9": today.get("rsi_9"),
            "daily_ema_rsi_9_3": today.get("ema_rsi_9_3"),
            "daily_wma_rsi_9_21": today.get("wma_rsi_9_21"),
            "weekly_date": w.get("date"),
            "weekly_rsi_3": w.get("rsi_3"),
            "weekly_rsi_9": w.get("rsi_9"),
            "weekly_ema_rsi_9_3": w.get("ema_rsi_9_3"),
            "weekly_wma_rsi_9_21": w.get("wma_rsi_9_21"),
            "monthly_date": monthly[0].get("date"),
            "monthly_rsi_3": monthly[0].get("rsi_3"),
        })

    return matches


def scanner_HM():
    parser = argparse.ArgumentParser(
        description="Scanner HM: start from today and scan backwards"
    )
    parser.add_argument("--excel", help="Write results to Excel file (requires pandas & openpyxl)")
    parser.add_argument("--csv", help="Write results to a single CSV (rows: date,symbol,name)")
    parser.add_argument("--limit", type=int, default=0, help="Limit matches per date (0 = no limit)")
    parser.add_argument("--runs", type=int, default=0, help="Number of runs (days) to scan backwards (overrides --max-days if >0). If 0, you'll be prompted interactively.")
    parser.add_argument(
        "--max-days",
        type=int,
        default=365,
        help="Maximum number of days to scan backwards (safety guard)"
    )
    args = parser.parse_args()

    if args.excel and pd is None:
        print("Pandas (and openpyxl) are required to write Excel output.")
        return

    # Determine number of runs (interactive fallback if --runs not provided)
    runs_limit = args.runs
    if runs_limit <= 0:
        try:
            user_in = input(f"Enter number of runs (days) to scan backwards (press Enter to use default {args.max_days}): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nInput cancelled. Exiting.")
            return
        if user_in:
            try:
                runs_limit = int(user_in)
                if runs_limit <= 0:
                    print("Number of runs must be positive.")
                    return
            except ValueError:
                print("Invalid number format. Use an integer.")
                return
        else:
            runs_limit = args.max_days

    # Safety: never exceed max-days
    runs_limit = min(runs_limit, args.max_days)
    log(f"Configured runs_limit = {runs_limit} (max-days={args.max_days})")

    conn = sqlite3.connect(DB_FILE)
    try:
        cur = conn.cursor()

        # Determine the latest available trading date in DB
        cur.execute("""
            SELECT MAX(date)
            FROM equity_price_data
            WHERE timeframe = '1d'
        """)
        row = cur.fetchone()
        if not row or not row[0]:
            print("No daily price data found in DB. Aborting.")
            return

        latest_trading_date = datetime.strptime(row[0], "%Y-%m-%d").date()
        log(f"Starting HM scan from latest trading date: {latest_trading_date}")

        results_by_date = {}
        agg_matches = []

        # find earliest trading date for stopping condition
        cur.execute("SELECT MIN(date) FROM equity_price_data WHERE timeframe = '1d'")
        row_earliest = cur.fetchone()
        earliest_trading_date = None
        if row_earliest and row_earliest[0]:
            earliest_trading_date = datetime.strptime(row_earliest[0], "%Y-%m-%d").date()

        run_date = latest_trading_date
        scanned_days = 0

        while scanned_days < runs_limit and run_date >= (earliest_trading_date or (latest_trading_date - timedelta(days=runs_limit))):
            scanned_days += 1
            d_str = run_date.strftime("%Y-%m-%d")

            # Check if this date is a trading day (exists in price_data)
            cur.execute("""
                SELECT 1
                FROM equity_price_data
                WHERE timeframe = '1d' AND date = ?
                LIMIT 1
            """, (d_str,))
            is_trading_day = cur.fetchone() is not None

            if not is_trading_day:
                # Skip weekends / holidays silently
                run_date = run_date - timedelta(days=1)
                continue

            matches = scan_as_of(conn, as_of=d_str, limit=args.limit)

            # Record date even if matches is empty
            results_by_date[d_str] = matches

            if matches:
                for m in matches:
                    agg_matches.append({
                        "date": d_str,
                        "symbol": m["symbol"],
                        "name": m.get("name", "")
                    })

            log(f"Date {d_str}: {len(matches)} matches")

            # Go back one calendar day
            run_date = run_date - timedelta(days=1)

        # Summary of dates with matches
        matched_dates = {d: v for d, v in results_by_date.items() if v}

        if not matched_dates:
            console.print("[yellow]No matches found across scanned dates.[/yellow]")
            return

        table = Table(
            title="Matched Symbols by Scan Date",
            show_lines=True
        )

        table.add_column("Scan Date", style="cyan", no_wrap=True)
        table.add_column("Symbol", style="bold white")
        table.add_column("Price", style="green")
        table.add_column("Daily RSI 3", style="magenta")
        table.add_column("Weekly RSI 3", style="magenta")
        table.add_column("Monthly RSI 3", style="magenta")
        table.add_column("Daily RSI 9", style="magenta")
        table.add_column("DAILY EMA(RSI(9),3)", style="magenta")
        table.add_column("DAILY WMA(RSI(9),21)", style="magenta")
        table.add_column("Weekly RSI 9", style="magenta")
        table.add_column("Weekly EMA(RSI(9),3)", style="magenta")
        table.add_column("Weekly WMA(RSI(9),21)", style="magenta")
        

        for scan_date in sorted(matched_dates.keys(), reverse=True):
            for r in matched_dates[scan_date]:
                table.add_row(
                    # str(scan_date),
                    datetime.strptime(scan_date, "%Y-%m-%d").strftime("%d-%b-%Y"),
                    str(r.get("symbol", "")),
                    str(r.get("price", "")),
                    str(r.get("daily_rsi_3", "")),
                    str(r.get("weekly_rsi_3", "")),
                    str(r.get("monthly_rsi_3", "")),
                    str(r.get("daily_rsi_9", "")),
                    str(r.get("daily_ema_rsi_9_3", "")),
                    str(r.get("daily_wma_rsi_9_21", "")),
                    str(r.get("weekly_rsi_9", "")),
                    str(r.get("weekly_ema_rsi_9_3", "")),
                    str(r.get("weekly_wma_rsi_9_21", ""))
                )

        console.print(table)
    finally:
        conn.close()


if __name__ == "__main__":
    scanner_HM()
