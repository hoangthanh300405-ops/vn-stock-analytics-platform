-- Làm sạch bảng danh mục mã: chuẩn hoá tên cột, loại bỏ dòng thiếu symbol
-- LƯU Ý: tên cột organ_name/exchange/industry cần đối chiếu lại với cột thật
-- trả về từ list_by_exchange()/list_by_industry() sau khi chạy extract lần đầu.

WITH source AS (
    SELECT * FROM {{ source('vnstock_raw', 'dim_stock') }}
),

renamed AS (
    SELECT
        symbol,
        organ_name          AS company_name,
        exchange,
        -- Fix #6: TRIM + NULLIF chuỗi rỗng ngay ở staging để dim_stock, dim_sector
        -- và fct_price_daily (join qua dim_stock) đều đồng bộ, tránh 2 dòng ngành
        -- trùng nhau do khoảng trắng thừa/khác hoa-thường ở nguồn.
        NULLIF(TRIM(industry), '') AS sector_name,
        current_timestamp    AS _loaded_at
    FROM source
    WHERE symbol IS NOT NULL
)

SELECT * FROM renamed
