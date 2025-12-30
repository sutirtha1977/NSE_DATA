# =========================================================
# THIS IS THE MAIN FILE CONTAINS THE FOLLOWING FUNCTIONS:
# stock_data_user_input()
# =========================================================
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

try:
    from urllib3.exceptions import ReadTimeoutError as URLLibReadTimeout
except Exception:
    URLLibReadTimeout = None

from helper import (
    log,LOG_FILE
)
from data_manager import (
    get_db_connection,
    close_db_connection,
    refresh_equity,
    refresh_indices,
    download_equity_price_data_all_timeframes,
    download_index_price_data_all_timeframes,
    update_52week_stats
)
from create_db import create_stock_database
# from indicators import (
#     update_indicators,
#     update_indicators_incremental
# )
from indicators import (
    update_indicators
)

def create_main_menu():
    console = Console()

    menu = Text()
    
    menu.append("1. ", style="bold cyan")
    menu.append("Create Database (", style="white")
    menu.append("RUN ONCE", style="bold red")
    menu.append(")\n", style="white")

    menu.append("2. ", style="bold cyan")
    menu.append("Update Equity Symbols (", style="white")
    menu.append("RUN ONCE", style="bold red")
    menu.append(")\n", style="white")            

    menu.append("3. ", style="bold cyan")
    menu.append("Update Index Symbols (", style="white")
    menu.append("RUN ONCE", style="bold red")
    menu.append(")\n", style="white")  
    
    menu.append("4. ", style="bold cyan")
    menu.append("Update all Equity Indicators (", style="white")
    menu.append("RUN ONCE", style="bold red")
    menu.append(")\n", style="white")  

    menu.append("5. ", style="bold cyan")
    menu.append("Update all Index Indicators (", style="white")
    menu.append("RUN ONCE", style="bold red")
    menu.append(")\n", style="white")  

    menu.append("6. ", style="bold cyan")
    menu.append("Fetch all Equity Price Data (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white")  

    menu.append("7. ", style="bold cyan")
    menu.append("Fetch one/multi Equity Price Data (", style="white")
    menu.append("RUN AS REQUIRED", style="bold yellow")
    menu.append(")\n", style="white")  

    menu.append("8. ", style="bold cyan")
    menu.append("Fetch all Index Price Data (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white")  

    menu.append("9. ", style="bold cyan")
    menu.append("Update Incremental Equity Indicators (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white")  

    menu.append("10. ", style="bold cyan")
    menu.append("Update Incremental Index Indicators (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white")  

    menu.append("11. ", style="bold cyan")
    menu.append("Update Equity 52 Week High Low (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white")  
                
    menu.append("12. ", style="bold cyan")
    menu.append("Update Index 52 Week High Low (", style="white")
    menu.append("RUN DAILY", style="bold yellow")
    menu.append(")\n", style="white") 
    
    console.print(Panel(menu, title="[bold]DATA MANAGER[/bold]", border_style="blue"))

# -----------------------------0
# CLI MENU
# -----------------------------
if __name__ == "__main__":
    try:
        with open(LOG_FILE, "w") as f:
            f.write("SYMBOL | TIMEFRAME | STATUS\n")
            f.write("-" * 40 + "\n")

        while True:
            create_main_menu()
 
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

            try:
                if choice == "1":
                    create_stock_database(drop_existing=True)
                    
                elif choice == "2":
                    refresh_equity(conn)
                     
                elif choice == "3":  
                    refresh_indices(conn)

                elif choice == "4":
                    update_indicators(conn, is_indexs=False)

                elif choice == "5":
                    update_indicators(conn, is_indexs=True)

                elif choice == "6":
                    download_equity_price_data_all_timeframes(conn,"ALL")

                elif choice == "7":
                    syms = input("Enter symbols (RELIANCE,TCS): ")
                    download_equity_price_data_all_timeframes(conn,syms)
                    
                elif choice == "8":
                    download_index_price_data_all_timeframes(conn,lookback_years=20)  

                elif choice == "9":
                    update_indicators(conn, is_indexs=False, incremental=True)

                elif choice == "10":
                    update_indicators(conn, is_indexs=True, incremental=True)

                elif choice == "11":
                    update_52week_stats(conn, "equity")

                elif choice == "12":
                    update_52week_stats(conn, "index")

                else:
                    print("Invalid choice")

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