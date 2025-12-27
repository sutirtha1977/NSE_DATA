import pandas as pd
import numpy as np
import traceback
import time
import sys
from helper import (
    log
)
# ---------------------------------------------
# Indicator Calculations
# ---------------------------------------------
# ---------------------------------------------
# RSI Calculations
# ---------------------------------------------
def calculate_rsi_series(close, period):
    try:
        # Use Wilder's smoothing (adjust=False) for RSI as commonly expected
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        # avoid division by zero
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        # where avg_loss == 0, RSI should be 100 (all gains)
        rsi = rsi.fillna(100)
        return rsi.round(2)
    except Exception as e:
        log(f"RSI CALC FAILED | period={period} | {e}")
        traceback.print_exc()
        return pd.Series(index=close.index, dtype=float)
# ---------------------------------------------
# Bollinger Calculations
# ---------------------------------------------
def calculate_bollinger(close, period=20, std_mult=2):
    try:
        mid = close.rolling(period).mean()
        std = close.rolling(period).std()
        upper = mid + std_mult * std
        lower = mid - std_mult * std
        return upper.round(2), mid.round(2), lower.round(2)
    except Exception as e:
        log(f"BOLLINGER CALC FAILED | {e}")
        traceback.print_exc()
        return (pd.Series(index=close.index, dtype=float),) * 3

# ---------------------------------------------
# ATR Calculations
# ---------------------------------------------
def calculate_atr(df, period=14):
    try:
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        # Use Wilder's smoothing (EMA with adjust=False) for ATR
        atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        return atr.round(2)
    except Exception as e:
        log(f"ATR CALC FAILED | period={period} | {e}")
        traceback.print_exc()
        return pd.Series(index=df.index, dtype=float)
# ---------------------------------------------
# MACD Calculations
# ---------------------------------------------
def calculate_macd(close):
    try:
        ema_12 = close.ewm(span=12, adjust=False).mean()
        ema_26 = close.ewm(span=26, adjust=False).mean()
        macd = ema_12 - ema_26
        signal = macd.ewm(span=9, adjust=False).mean()
        return macd.round(2), signal.round(2)
    except Exception as e:
        log(f"MACD CALC FAILED | {e}")
        traceback.print_exc()
        return (
            pd.Series(index=close.index, dtype=float),
            pd.Series(index=close.index, dtype=float),
        )
# ---------------------------------------------
# SuperTrend Calculations
# ---------------------------------------------
def calculate_supertrend(df, atr_period=10, multiplier=3):
    try:
        atr = calculate_atr(df, atr_period)
        hl2 = (df["high"] + df["low"]) / 2

        basic_ub = hl2 + multiplier * atr
        basic_lb = hl2 - multiplier * atr

        final_ub = basic_ub.copy()
        final_lb = basic_lb.copy()

        # ---- ADJUST BANDS (correct as-is) ----
        for i in range(1, len(df)):
            if basic_ub.iloc[i] < final_ub.iloc[i - 1] or df["close"].iloc[i - 1] > final_ub.iloc[i - 1]:
                final_ub.iloc[i] = basic_ub.iloc[i]
            else:
                final_ub.iloc[i] = final_ub.iloc[i - 1]

            if basic_lb.iloc[i] > final_lb.iloc[i - 1] or df["close"].iloc[i - 1] < final_lb.iloc[i - 1]:
                final_lb.iloc[i] = basic_lb.iloc[i]
            else:
                final_lb.iloc[i] = final_lb.iloc[i - 1]


        # ---- CORRECT SUPER TREND SELECTION ----
        supertrend = pd.Series(index=df.index, dtype=float)
        direction = pd.Series(index=df.index, dtype=int)

        # initialize
        supertrend.iloc[0] = final_ub.iloc[0]
        direction.iloc[0] = -1   # initial trend is down

        for i in range(1, len(df)):
            prev_st = supertrend.iloc[i - 1]

            # Determine direction based on prev supertrend
            if df["close"].iloc[i] > prev_st:
                direction.iloc[i] = 1      # uptrend
                supertrend.iloc[i] = final_lb.iloc[i]
            else:
                direction.iloc[i] = -1     # downtrend
                supertrend.iloc[i] = final_ub.iloc[i]

        return supertrend.round(2), direction

    except Exception as e:
        log(f"SUPERTREND CALC FAILED | {e}")
        traceback.print_exc()
        return (
            pd.Series(index=df.index, dtype=float),
            pd.Series(index=df.index, dtype=int),
        )
# ---------------------------------------------
# EMA Calculations
# ---------------------------------------------
def calculate_ema(series, period):
    try:
        # Keep as-is (Wilder-style EMA behavior with adjust=False)
        return series.ewm(span=period, adjust=False).mean().round(2)
    except Exception as e:
        log(f"EMA CALC FAILED | period={period} | {e}")
        traceback.print_exc()
        return pd.Series(index=series.index, dtype=float)
# ---------------------------------------------
# WMA Calculations
# ---------------------------------------------
def calculate_wma(series, period):
    try:
        weights = np.arange(1, period + 1)
        wma = series.rolling(period).apply(lambda x: np.dot(x, weights)/weights.sum(), raw=True)
        return wma.round(2)
    except Exception as e:
        log(f"WMA CALC FAILED | period={period} | {e}")
        traceback.print_exc()
        return pd.Series(index=series.index, dtype=float)