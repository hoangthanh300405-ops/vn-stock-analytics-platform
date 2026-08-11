-- Làm sạch thông tin công ty phát hành.
-- LƯU Ý: tên cột dựa theo tài liệu Company.overview() nguồn KBS, cần đối chiếu
-- lại với dữ liệu thật sau khi chạy extract_company_profile.py lần đầu.

WITH source AS (
    SELECT * FROM {{ source('vnstock_raw', 'company_profile') }}
),

renamed AS (
    SELECT
        symbol,
        business_model,
        founded_date,
        listing_date,
        charter_capital,
        number_of_employees
    FROM source
    WHERE symbol IS NOT NULL
)

SELECT * FROM renamed
