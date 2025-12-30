from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from helper import (
    LOG_FILE, MAIN_MENU_ITEMS,
    FREQ_COLORS
)
from data_manager import (
    get_db_connection,
    close_db_connection,
    refresh_equity,
    refresh_indices,
    download_equity_price_data_all_timeframes,
    download_index_price_data_all_timeframes,
    refresh_52week_stats
    # check_export_csv_missing_data
)
from create_db import create_stock_database
from indicators import (
    refresh_indicators, 
    refresh_equity_partial_prices,
    refresh_equity_partial_indicators,
    refresh_equity_partial_prices_datewise
)

console = Console()

def display_menu():
    table = Table.grid(padding=(0, 3))   # no spacing between rows
    table.add_column("Press")
    table.add_column("Action", style="white")
    table.add_column("Frequency", justify="center")

    for opt, action, freq, _ in MAIN_MENU_ITEMS:
        row_color = FREQ_COLORS.get(freq, "white")
        freq_text = f"[{row_color}]{freq.upper()}[/{row_color}]" if freq else ""

        # 👉 option number follows FREQUENCY color
        press_text  = f"👉 [bold {row_color}]{opt}[/bold {row_color}]"
        action_text = f"[bold]{action.upper()}[/bold]"

        # entire row styled using frequency color
        table.add_row(press_text, action_text, freq_text, style=row_color)

    panel = Panel(
        table,
        title="[bold blue]DATA MANAGER[/bold blue]",
        border_style="bright_blue"
    )
    console.print(panel)
    console.print("\n[bold green]Enter an option and press [yellow]ENTER[/yellow]:[/bold green] ", end="")
    
def data_manager_user_input():
    try:
        with open(LOG_FILE, "w") as f:
            f.write("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")

        while True:
            display_menu()
            # choice = Prompt.ask("Enter choice ('0' to exit)").strip()
            choice = Prompt.ask("[bold green]👉[/bold green]").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting...[/bold green]")
                break

            conn = get_db_connection()
            try:
                if choice == "1":
                    # Create Database
                    create_stock_database(drop_existing=True)
                elif choice == "2":
                    # Update Equity Symbols
                    refresh_equity(conn)
                elif choice == "3":
                    # Update Index Symbols
                    refresh_indices(conn)
                elif choice == "4":
                    # Fetch all Equity Price Data
                    daily_dt = Prompt.ask("Enter Daily End Date (YYYY-MM-DD)")
                    weekly_dt = Prompt.ask("Enter Weekly End Date (YYYY-MM-DD)")
                    monthly_dt = Prompt.ask("Enter Monthly End Date (YYYY-MM-DD)")
                    download_equity_price_data_all_timeframes(conn, "ALL",daily_dt,weekly_dt,monthly_dt)
                elif choice == "5":
                    # Fetch one/multi Equity Price Data
                    syms = Prompt.ask("Enter symbols (comma separated, e.g., RELIANCE,TCS)")
                    daily_dt = Prompt.ask("Enter Daily End Date (YYYY-MM-DD)")
                    weekly_dt = Prompt.ask("Enter Weekly End Date (YYYY-MM-DD)")
                    monthly_dt = Prompt.ask("Enter Monthly End Date (YYYY-MM-DD)")
                    download_equity_price_data_all_timeframes(conn, syms,daily_dt,weekly_dt,monthly_dt)
                elif choice == "6":
                    # Fetch all Index Price Data
                    daily_dt = Prompt.ask("Enter Daily End Date (YYYY-MM-DD)")
                    weekly_dt = Prompt.ask("Enter Weekly End Date (YYYY-MM-DD)")
                    monthly_dt = Prompt.ask("Enter Monthly End Date (YYYY-MM-DD)")
                    download_index_price_data_all_timeframes(conn, daily_dt,weekly_dt,monthly_dt,lookback_years=20)
                elif choice == "7":
                    console.print("\n[bold green]Start 52 weeks stat run for equity...[/bold green]")
                    # Update Equity 52 Week High Low
                    refresh_52week_stats(conn, "equity")
                    console.print("\n[bold green]End 52 weeks stat run for equity...[/bold green]")
                    console.print("\n[bold green]Start 52 weeks stat run for index...[/bold green]")
                    # Update Index 52 Week High Low
                    refresh_52week_stats(conn, "index")
                    console.print("\n[bold green]End 52 weeks stat run for index...[/bold green]")
                elif choice == "8":
                    # Update all Equity Indicators
                    refresh_indicators(conn, is_indexs=False)
                elif choice == "9":
                    # Update all Index Indicators
                    refresh_indicators(conn, is_indexs=True)
                elif choice == "10":
                    # Update Incremental Equity Indicators
                    refresh_indicators(conn, is_indexs=False, incremental=True)
                elif choice == "11":
                    # Update Incremental Index Indicators
                    refresh_indicators(conn, is_indexs=True, incremental=True)
                elif choice == "12":
                    # Update partial equity prices for weekly and monthly
                    refresh_equity_partial_prices(conn)
                elif choice == "13":
                    # Update partial equity prices for weekly and monthly datewise
                    run_dt = Prompt.ask("Enter Date (YYYY-MM-DD)")
                    refresh_equity_partial_prices_datewise(conn,run_dt)
                elif choice == "14":
                    # Update Partial Equity Indicators for weekly and monthly
                    refresh_equity_partial_indicators(conn)
                else:
                    console.print("[bold red]❌ Invalid choice![/bold red]")
            finally:
                close_db_connection(conn)

    except KeyboardInterrupt:
        console.print("\n[bold green]Interrupted by user. Exiting...[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")

if __name__ == "__main__":
    data_manager_user_input()