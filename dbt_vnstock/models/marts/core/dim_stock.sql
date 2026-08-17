-- Dimension: 1 dòng / 1 mã cổ phiếu, có thêm thông tin công ty (left join,
-- company_profile có thể chưa có nếu chưa chạy extract_company_profile.py lần nào)

SELECT
    d.symbol AS stock_key,
    d.symbol,
    d.company_name,
    d.exchange,
    d.sector_name,
    p.business_description,
    p.market_cap,
    p.foreigner_percentage,
    p.maximum_foreign_percentage,
    p.listing_date,
    p.issue_share,
    p.highest_price_1y,
    p.lowest_price_1y,
    p.rating,
    p.target_price,
    p.analyst
FROM {{ ref('stg_vnstock__dim_stock') }} d
LEFT JOIN {{ ref('stg_vnstock__company_profile') }} p ON d.symbol = p.symbol
