-- Dimension: 1 dòng / 1 mã cổ phiếu, có thêm thông tin công ty (left join,
-- company_profile có thể chưa có nếu chưa chạy extract weekly lần nào)

SELECT
    d.symbol AS stock_key,
    d.symbol,
    d.company_name,
    d.exchange,
    d.sector_name,
    p.business_model,
    p.founded_date,
    p.listing_date,
    p.charter_capital,
    p.number_of_employees
FROM {{ ref('stg_vnstock__dim_stock') }} d
LEFT JOIN {{ ref('stg_vnstock__company_profile') }} p ON d.symbol = p.symbol
