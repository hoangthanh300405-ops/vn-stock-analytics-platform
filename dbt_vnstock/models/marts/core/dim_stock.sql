-- Dimension: 1 dòng / 1 mã cổ phiếu, có thêm thông tin công ty (left join,
-- company_profile có thể chưa có nếu chưa chạy weekly_company_profile.yml
-- lần nào — stg_vnstock__company_profile luôn tồn tại dù rỗng nhờ bootstrap
-- trong build_duckdb_file.py, nên LEFT JOIN này luôn an toàn).
--
-- Fix 2026-08-14: đổi các cột lấy từ company_profile cho khớp dữ liệu THẬT
-- (xem giải thích đầy đủ ở đầu file stg_vnstock__company_profile.sql) —
-- business_model -> business_description, charter_capital -> market_cap,
-- number_of_employees -> foreigner_percentage (dữ liệu gốc không có
-- charter_capital/number_of_employees, không tự bịa).

SELECT
    d.symbol AS stock_key,
    d.symbol,
    d.company_name,
    d.exchange,
    d.sector_name,
    p.business_description,
    p.listing_date,
    p.market_cap,
    p.foreigner_percentage
FROM {{ ref('stg_vnstock__dim_stock') }} d
LEFT JOIN {{ ref('stg_vnstock__company_profile') }} p ON d.symbol = p.symbol
