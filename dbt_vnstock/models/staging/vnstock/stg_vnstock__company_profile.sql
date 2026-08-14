-- Làm sạch thông tin công ty phát hành.
--
-- ĐÃ ĐỐI CHIẾU VỚI DỮ LIỆU THẬT (lần chạy đầu tiên của weekly_company_profile.yml,
-- 2026-08-14) — Company.overview() của vnstock v4.0.5 (nguồn VCI) trả về danh
-- sách cột KHÁC HẲN so với giả định ban đầu ghi trong file này. Cột thật đầy
-- đủ (in ra bởi extract_company_profile.py, xem log step "Extract company
-- profile"):
--   symbol, organ_code, current_price, market_cap, issue_share, tag, rating,
--   rating_as_of, organ_name, organ_short_name, com_type_code,
--   com_group_code, sector, average_match_value1_month,
--   average_match_volume1_month, highest_price1_year, lowest_price1_year,
--   foreigner_percentage, maximum_foreign_percentage, state_percentage,
--   analyst, upside_to_target_percent, dividend_per_share_tsr,
--   projected_tsr_percentage, target_price, company_profile, in_cu,
--   icb_code_lv2, icb_code_lv4, free_float, free_float_percentage,
--   listing_date, prev_insight, fund_info, is_bank, listing, bank
-- Cột thật sự giữ lại, dùng được cho dashboard:
--   - company_profile -> đổi tên business_description: đây là cột mô tả
--     doanh nghiệp dạng text (chính là field "profile" gốc mà thư viện tự
--     rename thành "company_profile", xem comment gốc ở
--     extract_company_profile.py) -> thay thế cho business_model cũ trong UI.
--   - listing_date: giữ nguyên, đúng như giả định ban đầu.
--   - market_cap: vốn hoá thị trường -> thay thế charter_capital trong UI
--     (đổi nhãn hiển thị cho đúng bản chất, không dùng lại tên charter_capital).
--   - foreigner_percentage: room ngoại hiện tại -> thay thế
--     number_of_employees trong UI (dữ liệu này không có trong nguồn thật).

WITH source AS (
    SELECT * FROM {{ source('vnstock_raw', 'company_profile') }}
),

renamed AS (
    SELECT
        symbol,
        company_profile        AS business_description,
        listing_date,
        market_cap,
        foreigner_percentage
    FROM source
    WHERE symbol IS NOT NULL
)

SELECT * FROM renamed
