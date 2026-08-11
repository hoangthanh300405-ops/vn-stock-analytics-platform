-- Dimension: lịch 2020-2030. is_trading_day = không phải cuối tuần VÀ không
-- phải ngày nghỉ lễ (theo seed vn_public_holidays.csv - fix #7).
-- LƯU Ý: seed chỉ xác nhận đầy đủ ngày nghỉ âm lịch (Tết, Giỗ Tổ) cho 2024-2026;
-- các năm ngoài khoảng này sẽ THIẾU ngày nghỉ âm lịch cho tới khi seed được
-- cập nhật thủ công khi lịch nghỉ chính thức được công bố.

{{ config(materialized='table') }}

WITH days AS (
    SELECT date_key::DATE AS date_key
    FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY) AS t(date_key)
),

holidays AS (
    SELECT holiday_date::DATE AS holiday_date FROM {{ ref('vn_public_holidays') }}
)

SELECT
    d.date_key,
    EXTRACT(YEAR FROM d.date_key)    AS year,
    EXTRACT(QUARTER FROM d.date_key) AS quarter,
    EXTRACT(MONTH FROM d.date_key)   AS month,
    dayname(d.date_key)              AS day_name,
    dayofweek(d.date_key) IN (0, 6)  AS is_weekend,
    (dayofweek(d.date_key) NOT IN (0, 6) AND h.holiday_date IS NULL) AS is_trading_day
FROM days d
LEFT JOIN holidays h ON d.date_key = h.holiday_date
