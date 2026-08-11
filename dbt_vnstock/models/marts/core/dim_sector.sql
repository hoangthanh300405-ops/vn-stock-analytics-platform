-- Dimension: danh sách ngành duy nhất, dùng natural key (sector_name)
-- cho phù hợp quy mô dự án cá nhân; có thể đổi sang surrogate key hash nếu cần sau.

SELECT DISTINCT
    sector_name AS sector_key,
    sector_name
FROM {{ ref('stg_vnstock__dim_stock') }}
WHERE sector_name IS NOT NULL
