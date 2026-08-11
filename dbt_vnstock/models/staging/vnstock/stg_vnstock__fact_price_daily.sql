-- Cast đúng kiểu dữ liệu + dedup theo (symbol, trade_date).
-- Cần dedup ở đây vì extract_vnstock.py chạy incremental lấy dư 5 ngày mỗi lần
-- để tự vá ngày bị miss -> có thể trùng lặp giữa các lần load, xử lý ở đây
-- thay vì ở load_to_motherduck.py (giữ đúng nguyên tắc "landing tối giản").

WITH source AS (
    SELECT * FROM {{ source('vnstock_raw', 'fact_price_daily') }}
),

typed AS (
    SELECT
        symbol,
        CAST(time AS DATE)          AS trade_date,
        CAST(open AS DECIMAL(18,2))  AS open_price,
        CAST(high AS DECIMAL(18,2))  AS high_price,
        CAST(low AS DECIMAL(18,2))   AS low_price,
        CAST(close AS DECIMAL(18,2)) AS close_price,
        CAST(volume AS BIGINT)       AS volume
    FROM source
    WHERE symbol IS NOT NULL AND time IS NOT NULL
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, trade_date
            ORDER BY trade_date DESC
        ) AS rn
    FROM typed
)

SELECT
    symbol, trade_date, open_price, high_price, low_price, close_price, volume
FROM deduped
WHERE rn = 1
