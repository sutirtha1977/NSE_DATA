
from helper import (
    log, LOG_FILE,
    SCANNER_FOLDER
)
from data_manager import (
    get_db_connection,
    close_db_connection
)
import traceback
import os
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from sql import (
    SQL_MAP
)

console = Console()

def export_to_csv(df, filename_prefix):
    try:
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"Scanner_{filename_prefix}_{ts}.csv"
        filepath = os.path.join(SCANNER_FOLDER, filename)
        df.to_csv(filepath, index=False)
        return filepath
    except Exception as e:
        log(f"I❌ CSV Failed | {e}")
        traceback.print_exc()

def create_scanner_menu():
    console = Console()

    menu = Text()
    
    menu.append("1. ", style="bold cyan")
    menu.append("SCANNER 1\n", style="bold yellow")
    menu.append("   ▸ DAILY rsi(9)/ DAILY ema(rsi(9),3) greater than 1.1\n")
    menu.append("   ▸ DAILY ema(rsi(9),3)/ DAILY wma(rsi(9),21) greater than 1.1\n")
    menu.append("   ▸ DAILY rsi(3) crossed over 60\n")
    menu.append("   ▸ MONTHLY rsi(3) greater than 50\n")
    menu.append("   ▸ WEEKLY rsi(3) greater than 50\n")
    menu.append("   ▸ DAILY PRICE CHANGE is less than equal to 5\n")
    menu.append("   ▸ DAILY CLOSE >= 100\n\n")

    menu.append("2. ", style="bold cyan")
    menu.append("SCANNER 2\n", style="bold yellow")
    menu.append("   ▸ DAILY CLOSE >= 100\n")
    menu.append("   ▸ MONTHLY rsi(3) > 60\n")
    menu.append("   ▸ WEEKLY rsi(3) > 60\n")
    menu.append("   ▸ DAILY rsi(3) crossed above 55 (yesterday < 55, today >= 55\n")
    menu.append("   ▸ DAILY rsi(9) >= daily ema(rsi(9),3)\n")
    menu.append("   ▸ DAILY ema(rsi(9),3) >= daily wma(rsi(9),21)\n")
    menu.append("   ▸ DAILY rsi(9) / ema(rsi(9),3) >= 1.2\n")
    menu.append("   ▸ WEEKLY rsi(9) >= WEEKLY ema(rsi(9),3)\n")
    menu.append("   ▸ WEEKLY ema(rsi(9),3) >= WEEKLY wma(rsi(9),21)\n")
    menu.append("   ▸ Daily percentage change less than 10%\n\n")
    
    console.print(Panel(menu, title="[bold]SCANNER[/bold]", border_style="blue"))
            
def scanner(conn,choice):
    try:
        cur = conn.cursor()
        sql = SQL_MAP.get(int(choice))

        print(f"✅ Scanner Started (SQL_SCANNER_{choice})")
        cur.execute(sql)

        # DEFINE COLUMNS HERE
        columns = [desc[0] for desc in cur.description]
        
        rows = cur.fetchall()
        results = []

        for row in rows:
            row = dict(zip(columns, row))
            results.append(row)
            
        print(f"✅ Scanner finished | Matches: {len(results)}")
        return results
    except Exception as e:
        log(f"I❌ Scanner failed | {e}")
        traceback.print_exc()
        
if __name__ == "__main__":
    try:
        with open(LOG_FILE, "w") as f:
            f.write("SYMBOL | TIMEFRAME | STATUS\n")
            f.write("-" * 40 + "\n")

        while True:
            create_scanner_menu()

            try:
                choice = input("Enter choice ('0', 'q', 'quit', 'exit' to exit): ").strip()
            except EOFError:
                print("\nInput closed. Exiting.")
                break
            except KeyboardInterrupt:
                print("\nInterrupted by user. Exiting.")
                break

            # allow several ways to exit
            if choice in ("0", "q", "quit", "exit"):
                break
            conn = get_db_connection()
            matches = scanner(conn,choice)
            try:
                if not matches:
                    console.print("[bold yellow]No backtest matches found.[/bold yellow]")
                else:
                    df = pd.DataFrame(matches)

                    # Optional: sort
                    df.sort_values(["date", "symbol"], inplace=True)
                    # ✅ EXPORT TO CSV
                    csv_file = export_to_csv(df,choice)
                    console.print(f"[bold cyan]📁 Backtest data exported to:[/bold cyan] {csv_file}")
                    
                    df = pd.DataFrame(matches)

                    # Win definitions
                    df["win_5d"] = df["ret_5d"] > 0
                    df["win_10d"] = df["ret_10d"] > 0

                    win_rate_5d = df["win_5d"].mean() * 100
                    win_rate_10d = df["win_10d"].mean() * 100

                    print(f"📈 Win Rate (5D):  {win_rate_5d:.2f}%")
                    print(f"📈 Win Rate (10D): {win_rate_10d:.2f}%")
                    max_loss_5d = df["ret_5d"].min()
                    max_loss_10d = df["ret_10d"].min()

                    print(f"❌ Max Loss (5D):  {max_loss_5d:.2f}%")
                    print(f"❌ Max Loss (10D): {max_loss_10d:.2f}%")

                    console.print(f"[bold green]Backtest Matches: {len(df)}[/bold green]")

                    table = Table(
                        title=f"Scanner_{choice} Backtest Results",
                        show_lines=True,
                        header_style="bold cyan"
                    )

                    for col in ["symbol", "date", "ret_5d", "ret_10d"]:
                        table.add_column(col.upper())

                    # Show last N signals only
                    for _, row in df.head(10).iterrows():
                        table.add_row(
                            row["symbol"],
                            str(row["date"]),
                            f"{row['ret_5d']:.2f}",
                            f"{row['ret_10d']:.2f}",
                        )

                    console.print(table)
            finally:
                close_db_connection(conn)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
    finally:
        # Ensure any leftover DB connection is closed
        try:
            if 'conn' in locals() and conn:
                close_db_connection(conn)
        except Exception:
            pass
    
