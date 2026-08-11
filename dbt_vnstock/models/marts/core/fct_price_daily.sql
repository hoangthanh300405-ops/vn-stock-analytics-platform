-- Fact table: 1 dòng / (mã, ngày). Incremental để không rebuild toàn bộ lịch sử mỗi lần chạy.
-- Denormalize sector_name thẳng vào fact (theo khuyến nghị OLAP wide-table của MotherDuck)
-- để dashboard query xu hướng theo ngành không cần join thêm.

{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'date_key']
    )
}}

WITH prices AS (
    SELECT * FROM {{ ref('stg_vnstock__fact_price_daily') }}
    {% if is_incremental() %}
    WHERE trade_date >= (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
),

enriched AS (
    SELECT
        p.symbol,
        p.trade_date AS date_key,
        s.sector_name,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.volume,
        ROUND(
            (p.close_price - p.open_price) / NULLIF(p.open_price, 0) * 100, 2
        ) AS price_change_pct
    FROM prices p
    LEFT JOIN {{ ref('dim_stock') }} s ON p.symbol = s.symbol
)

SELECT * FROM enriched
