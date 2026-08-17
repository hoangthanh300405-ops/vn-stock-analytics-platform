-- Làm sạch thông tin công ty phát hành.
--
-- Fix 2026-08-16: business_model/founded_date/charter_capital/number_of_employees
-- KHÔNG tồn tại trong dữ liệu thật trả về từ Company.overview() (nguồn VCI) — đây
-- là các tên cột ĐOÁN theo tài liệu cũ trước khi chạy thử API thật (comment gốc ở
-- đầu file này đã cảnh báo đúng điều này). Đối chiếu trực tiếp với CSV thật đã chạy
-- (company_profile.csv), cột thật liên quan tới dashboard là: market_cap,
-- company_profile (mô tả doanh nghiệp dạng text — đổi tên business_description cho
-- rõ nghĩa, tránh trùng tên bảng), foreigner_percentage, maximum_foreign_percentage,
-- listing_date, issue_share, highest_price1_year, lowest_price1_year, rating,
-- target_price, analyst.
--
-- LƯU Ý ĐƠN VỊ TIỀN TỆ: market_cap/target_price/highest_price1_year/lowest_price1_year
-- ở NGUỒN NÀY (Company.overview()) trả về FULL VNĐ — KHÁC với Quote().history()
-- (nguồn OHLCV cho fct_price_daily) trả về đơn vị NGHÌN đồng. Xử lý format ở tầng
-- Streamlit (xem format_vnd vs format_vnd_full trong pages/2_Chi_tiet_ma.py),
-- KHÔNG quy đổi lại đơn vị ở đây để giữ đúng giá trị gốc từ nguồn.

WITH source AS (
    SELECT * FROM {{ source('vnstock_raw', 'company_profile') }}
),

renamed AS (
    SELECT
        symbol,
        market_cap,
        company_profile AS business_description,
        foreigner_percentage,
        maximum_foreign_percentage,
        listing_date,
        issue_share,
        highest_price1_year AS highest_price_1y,
        lowest_price1_year AS lowest_price_1y,
        rating,
        target_price,
        analyst
    FROM source
    WHERE symbol IS NOT NULL
)

SELECT * FROM renamed
