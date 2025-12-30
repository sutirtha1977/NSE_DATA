from datetime import datetime, date, timezone
LOG_FILE = "price_loader.log"
DB_FILE = "./database/stocks.db"
FREQUENCIES = ["1d", "1wk", "1mo"]
CSV_FILE = "data.csv"
SCANNER_FOLDER = "./scanner_files/"
MISSING_EQUITY = "./yahoo_failure/missing_equity_symbols.csv"
MISSING_INDEX = "./yahoo_failure/missing_index_symbols.csv"
FREQ_COLORS = {
    "Run Once": "bold blue",
    "Run Daily": "bold white",
    "Run As Required": "bold green",
    "": "white"
}
MAIN_MENU_ITEMS = [
    ("1", "Create Database", "Run Once", "blue"),
    ("2", "Update Equity Symbols", "Run Once", "blue"),
    ("3", "Update Index Symbols", "Run Once", "blue"),
    ("4", "Fetch all Equity Price Data", "Run Daily", "yellow"),
    ("5", "Fetch one/multi Equity Price Data", "Run As Required", "yellow"),
    ("6", "Fetch all Index Price Data", "Run Daily", "yellow"),
    ("7", "Update52 Week High Low", "Run Daily", "yellow"),
    ("8", "Update all Equity Indicators", "Run Once", "blue"),
    ("9", "Update all Index Indicators", "Run Once", "blue"),
    ("10", "Update Incremental Equity Indicators", "Run Daily", "yellow"),
    ("11", "Update Incremental Index Indicators", "Run Daily", "yellow"),
    ("12", "Update Partial Week and Month Data", "Run Daily", "yellow"),
    ("13", "Update Partial Week and Month Data Based on Date supplied", "Run As Required", "yellow"),
    ("14", "Update Partial Week and Month Indicators", "Run Daily", "yellow"),
    ("0", "Exit", "", "white"),
]
NSE_INDICES = [
    # Broad market
    ("NIFTY50", "NIFTY 50", "NSE", "^NSEI", "Broad"),
    ("NIFTYNEXT50", "NIFTY Next 50", "NSE", "^NSMIDCP", "Broad"),
    ("NIFTY100", "NIFTY 100", "NSE", "^CNX100", "Broad"),
    ("NIFTY200", "NIFTY 200", "NSE", "^CNX200", "Broad"),
    ("NIFTY500", "NIFTY 500", "NSE", "^CRSLDX", "Broad"),

    # Sectoral
    ("BANKNIFTY", "NIFTY Bank", "NSE", "^NSEBANK", "Sectoral"),
    ("FINNIFTY", "NIFTY Financial Services", "NSE", "^CNXFIN", "Sectoral"),
    ("ITNIFTY", "NIFTY IT", "NSE", "^CNXIT", "Sectoral"),
    ("PHARMANIFTY", "NIFTY Pharma", "NSE", "^CNXPHARMA", "Sectoral"),
    ("FMCGNIFTY", "NIFTY FMCG", "NSE", "^CNXFMCG", "Sectoral"),
    ("AUTONIFTY", "NIFTY Auto", "NSE", "^CNXAUTO", "Sectoral"),
    ("METALNIFTY", "NIFTY Metal", "NSE", "^CNXMETAL", "Sectoral"),
    ("REALTYNIFTY", "NIFTY Realty", "NSE", "^CNXREALTY", "Sectoral"),
    ("PSUBANKNIFTY", "NIFTY PSU Bank", "NSE", "^CNXPSUBANK", "Sectoral"),

    # Market cap
    ("SMALLCAP100", "NIFTY Smallcap 100", "NSE", "^CNXSC", "Smallcap"),

    # Volatility
    ("INDIAVIX", "India VIX", "NSE", "^INDIAVIX", "Volatility"),
]
# NSE_INDICES = [
#     # Broad market
#     ("NIFTY50", "NIFTY 50", "NSE", "^NSEI", "Broad"),
#     ("BANKNIFTY", "NIFTY Bank", "NSE", "^NSEBANK", "Sectoral"),
#     ("FINNIFTY", "NIFTY Financial Services", "NSE", "^CNXFIN", "Sectoral"),
#     ("INDIAVIX", "India VIX", "NSE", "^INDIAVIX", "Volatility"),
# ]

SKIP_MONTHLY = True
SKIP_MONTHLY = (date.today().day != 1)
today_weekday = datetime.now(timezone.utc).weekday()
SKIP_WEEKLY = today_weekday in (0, 1, 2, 3)  # NOT Fri, Sat, Sun
SKIP_MONTHLY = False
SKIP_WEEKLY = False


# =========================================================
# LOGGING
# =========================================================
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")