import yfinance as yf
import pandas as pd
from datetime import datetime
import os
# https://ranaroussi.github.io/yfinance/reference/index.html
# -----------------------------
# Configuration
# -----------------------------
symbol = "TCS.NS"  # NSE Reliance
timeframes = {
    "1d": "daily",
    "1wk": "weekly",
    "1mo": "monthly"
}

# Maximum period
period = "1y"

# -----------------------------
# Download and save CSVs
# -----------------------------
for interval, label in timeframes.items():
    print(f"Downloading {label} data for {symbol}...")
    
    try:
        df = yf.download(
            symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=True
        )
        
        if df.empty:
            print(f"No data found for {label} timeframe.")
            continue
        
        # Reset index to have 'Date' column
        df.reset_index(inplace=True)
        
        # Optional: format Date as YYYY-MM-DD
        df['Date'] = df['Date'].dt.strftime("%Y-%m-%d")
        # Folder where CSVs will be saved
        output_folder = "test_python_scripts"

        # Create folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # CSV filename with folder path
        csv_file = os.path.join(output_folder, f"{symbol}_{label}_data.csv")

        # CSV filename
        # CSV filename with folder path
        csv_file = os.path.join(output_folder, f"{symbol}_{label}_data.csv")

        df.to_csv(csv_file, index=False)
        
        print(f"✅ Saved {label} data to {csv_file} ({len(df)} rows)")
    
    except Exception as e:
        print(f"❌ Failed to download {label} data: {e}")

print("All downloads completed.")