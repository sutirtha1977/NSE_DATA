##################### CRITERIA #####################
# 1. DAILY rsi(9)/ DAILY ema(rsi(9),3) greater than 1.1
# 2. DAILY ema(rsi(9),3)/ DAILY wma(rsi(9),21) greater than 1.1
# 3. DAILY rsi(3) crossed over 60
# 4. MONTHLY rsi(3) greater than 50
# 5. WEEKLY rsi(3) greater than 50
# 6. DAILY PRICE CHANGE is less than equal to 5
# 7. DAILY CLOSE >= 100
##################### CRITERIA #####################
SQL_SCANNER_1 = """
WITH daily AS (
    SELECT
        i.symbol_id,
        i.date,
        p.close,
        i.rsi_3,
        i.rsi_9,
        i.ema_rsi_9_3,
        i.wma_rsi_9_21,
        i.pct_price_change,

        LAG(i.rsi_3) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS prev_rsi_3,

        LEAD(p.close, 5) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS close_5d,

        LEAD(p.close, 10) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS close_10d

    FROM equity_indicators i
    JOIN equity_price_data p
      ON p.symbol_id = i.symbol_id
     AND p.timeframe = i.timeframe
     AND p.date = i.date
    WHERE i.timeframe = '1d'
),
weekly AS (
    SELECT
        w.symbol_id,
        w.date,
        w.rsi_3
    FROM equity_indicators w
    WHERE w.timeframe = '1wk'
),
monthly AS (
    SELECT
        m.symbol_id,
        m.date,
        m.rsi_3
    FROM equity_indicators m
    WHERE m.timeframe = '1mo'
)
SELECT
    s.symbol,
    d.symbol_id,
    d.date,
    d.rsi_3  AS daily_rsi_3,
    d.rsi_9  AS daily_rsi_9,
    w.rsi_3  AS weekly_rsi_3,
    m.rsi_3  AS monthly_rsi_3,
    ROUND((d.close_5d  - d.close) / d.close * 100, 2) AS ret_5d,
    ROUND((d.close_10d - d.close) / d.close * 100, 2) AS ret_10d
FROM daily d
JOIN weekly w
  ON w.symbol_id = d.symbol_id
 AND w.date = (
     SELECT MAX(w2.date)
     FROM weekly w2
     WHERE w2.symbol_id = d.symbol_id
       AND w2.date <= d.date
 )
JOIN monthly m
  ON m.symbol_id = d.symbol_id
 AND m.date = (
     SELECT MAX(m2.date)
     FROM monthly m2
     WHERE m2.symbol_id = d.symbol_id
       AND m2.date <= d.date
 )
JOIN equity_symbols s ON s.symbol_id = d.symbol_id
WHERE
    (d.rsi_9 / d.ema_rsi_9_3) > 1.1
    AND (d.ema_rsi_9_3 / d.wma_rsi_9_21) > 1.1
    AND d.prev_rsi_3 <= 60
    AND d.rsi_3 > 60
    AND w.rsi_3 > 50
    AND m.rsi_3 > 50
    AND d.pct_price_change <= 5
	AND d.close >= 100
	AND d.close_5d IS NOT NULL
	AND d.close_10d IS NOT NULL
ORDER BY d.date, s.symbol;
"""

##################### CRITERIA #####################
# 1. DAILY CLOSE >= 100
# 2. MONTHLY rsi(3) > 60
# 3. WEEKLY rsi(3) > 60
# 4. DAILY rsi(3) crossed above 55 (yesterday < 55, today >= 55)
# 5. DAILY rsi(9) >= daily ema(rsi(9),3)
# 6. DAILY ema(rsi(9),3) >= daily wma(rsi(9),21)
# 7. DAILY rsi(9) / ema(rsi(9),3) >= 1.2
# 8. WEEKLY rsi(9) >= WEEKLY ema(rsi(9),3)
# 9. WEEKLY ema(rsi(9),3) >= WEEKLY wma(rsi(9),21)
# 10. Daily percentage change less than 10%
##################### CRITERIA #####################

SQL_SCANNER_2 = """
WITH daily AS (
    SELECT
        i.symbol_id,
        i.date,
        p.close,
        i.rsi_3,
        i.rsi_9,
        i.ema_rsi_9_3,
        i.wma_rsi_9_21,
        i.pct_price_change,
        LAG(i.rsi_3) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS prev_rsi_3,
        LEAD(p.close, 5) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS close_5d,
        LEAD(p.close, 10) OVER (
            PARTITION BY i.symbol_id
            ORDER BY i.date
        ) AS close_10d
    FROM equity_indicators i
    JOIN equity_price_data p
      ON p.symbol_id = i.symbol_id
     AND p.timeframe = i.timeframe
     AND p.date = i.date
    WHERE i.timeframe = '1d'
),
weekly AS (
    SELECT
        symbol_id,
        date,
        rsi_3,
        rsi_9,
        ema_rsi_9_3,
        wma_rsi_9_21
    FROM equity_indicators
    WHERE timeframe = '1wk'
),
monthly AS (
    SELECT
        symbol_id,
        date,
        rsi_3
    FROM equity_indicators
    WHERE timeframe = '1mo'
)
SELECT
    s.symbol,
    d.date,

    d.close,
    d.rsi_3  AS daily_rsi_3,
    d.rsi_9  AS daily_rsi_9,
    w.rsi_3  AS weekly_rsi_3,
    m.rsi_3  AS monthly_rsi_3,
    ROUND((d.close_5d  - d.close) / d.close * 100, 2) AS ret_5d,
    ROUND((d.close_10d - d.close) / d.close * 100, 2) AS ret_10d
FROM daily d
JOIN weekly w
  ON w.symbol_id = d.symbol_id
 AND w.date = (
     SELECT MAX(w2.date)
     FROM weekly w2
     WHERE w2.symbol_id = d.symbol_id
       AND w2.date <= d.date
 )
JOIN monthly m
  ON m.symbol_id = d.symbol_id
 AND m.date = (
     SELECT MAX(m2.date)
     FROM monthly m2
     WHERE m2.symbol_id = d.symbol_id
       AND m2.date <= d.date
 )
JOIN equity_symbols s
  ON s.symbol_id = d.symbol_id
WHERE
    -- Price filter
    d.close >= 100
    -- Monthly RSI(3)
    AND m.rsi_3 > 60
    -- Weekly RSI(3)
    AND w.rsi_3 > 60
    -- Daily RSI(3) crossover above 55
    AND d.prev_rsi_3 < 55
    AND d.rsi_3 >= 55
    -- Daily RSI9 ≥ EMA
    AND d.rsi_9 >= d.ema_rsi_9_3
    -- Daily EMA ≥ WMA
    AND d.ema_rsi_9_3 >= d.wma_rsi_9_21
    -- Daily RSI9 / EMA ≥ 1.2
    AND (d.rsi_9 / d.ema_rsi_9_3) >= 1.2
    -- Weekly RSI9 ≥ EMA
    AND w.rsi_9 >= w.ema_rsi_9_3
    -- Weekly EMA ≥ WMA
    AND w.ema_rsi_9_3 >= w.wma_rsi_9_21
    -- Daily change
    AND d.pct_price_change < 10
    AND d.close_5d IS NOT NULL
	AND d.close_10d IS NOT NULL
ORDER BY d.date DESC, s.symbol;
"""
SQL_MISSING_TEMPLATE = """
    SELECT symbol
    FROM (
        SELECT
            s.symbol
        FROM {symbols_table} s
        LEFT JOIN {price_table} p
            ON s.{id_col} = p.{id_col}
        GROUP BY s.symbol
    )
    WHERE d1 IS NULL OR wk1 IS NULL OR mo1 IS NULL;
"""
SQL_MAP = {
    1: SQL_SCANNER_1,
    2: SQL_SCANNER_2,
    3: SQL_MISSING_TEMPLATE,
}