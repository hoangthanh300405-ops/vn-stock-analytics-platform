-- Fact table: 1 dòng / (mã, ngày). Incremental để không rebuild toàn bộ lịch sử mỗi lần chạy.
-- Denormalize sector_name + exchange thẳng vào fact (theo khuyến nghị OLAP wide-table)
-- để dashboard query xu hướng theo ngành/tính biên độ giá không cần join thêm.

-- Fact table: 1 dòng / (mã, ngày). Incremental để không rebuild toàn bộ lịch sử mỗi lần chạy.
-- Denormalize sector_name + exchange thẳng vào fact (theo khuyến nghị OLAP wide-table)
-- để dashboard query xu hướng theo ngành/tính biên độ giá không cần join thêm.

-- Fix (phát hiện qua lỗi thật: BinderException "reference_price" not found
-- khi Streamlit query, dù dbt run luôn báo "Completed successfully"): mặc
-- định on_schema_change='ignore' của dbt khiến incremental model CHỈ insert
-- dữ liệu khớp với cột đã có sẵn trong bảng đích, âm thầm bỏ qua bất kỳ cột
-- mới nào thêm vào SELECT sau khi bảng đã tồn tại (như
-- reference_price/ceiling_price/floor_price ở fix #9) — không có cảnh báo
-- nào, rất khó phát hiện. Đặt 'append_new_columns' để dbt tự ALTER TABLE
-- thêm cột mới ở lần incremental run tiếp theo thay vì âm thầm bỏ qua.
{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'date_key'],
        on_schema_change='append_new_columns'
    )
}}

WITH new_prices AS (
    SELECT * FROM {{ ref('stg_vnstock__fact_price_daily') }}
    {% if is_incremental() %}
    WHERE trade_date >= (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
),
-- Fix #9: reference_price/ceiling_price/floor_price KHÔNG có trong dữ liệu lịch sử
-- vnstock trả về (đó là thông tin của phiên giao dịch, chỉ price-board realtime mới
-- có) -> ước tính bằng công thức chuẩn VN: reference = giá đóng cửa phiên liền trước,
-- ceiling/floor = reference +/- biên độ theo sàn. KHÔNG đúng cho phiên đầu sau niêm
-- yết hoặc sau chia tách/trả cổ tức bằng cổ phiếu (biên độ tính khác) — chấp nhận sai
-- số nhỏ ở các phiên hiếm gặp này cho mục đích hiển thị dashboard cá nhân.
--
-- Để LAG ra đúng reference_price ở NGÀY ĐẦU TIÊN của mỗi lần chạy incremental, phải
-- gộp thêm lịch sử đã có sẵn trong chính bảng này (nếu đã tồn tại) rồi mới LAG — nếu
-- chỉ LAG trong batch mới thì ngày đầu batch sẽ bị NULL dù thực ra đã có giá hôm trước.
-- Đơn giản hoá: lấy nguyên phần lịch sử trước ngày batch mới nhất thay vì chỉ 1 dòng
-- gần nhất/mã — chấp nhận đánh đổi hiệu năng để giữ SQL dễ đọc, phù hợp quy mô cá nhân.
price_history_for_lag AS (
    SELECT symbol, trade_date AS date_key, close_price FROM new_prices
    {% if is_incremental() %}
    UNION ALL
    SELECT symbol, date_key, close_price
    FROM {{ this }}
    WHERE date_key < (SELECT MIN(trade_date) FROM new_prices)
    {% endif %}
),

with_reference AS (
    SELECT
        symbol,
        date_key,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY date_key) AS reference_price
    FROM price_history_for_lag
),

enriched AS (
    SELECT
        p.symbol,
        p.trade_date AS date_key,
        s.sector_name,
        s.exchange,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.volume,
        ROUND(
            (p.close_price - p.open_price) / NULLIF(p.open_price, 0) * 100, 2
        ) AS price_change_pct,
        r.reference_price,
        -- LƯU Ý: giá trị exchange cần đối chiếu lại với dữ liệu thật trả về từ
        -- list_by_exchange() (có thể là "HOSE"/"HSX", "HNX", "UPCOM" tuỳ nguồn) —
        -- sửa lại các nhãn CASE bên dưới cho khớp sau khi chạy extract lần đầu.
        CASE s.exchange
            WHEN 'HOSE'  THEN ROUND(r.reference_price * 1.07, 2)
            WHEN 'HNX'   THEN ROUND(r.reference_price * 1.10, 2)
            WHEN 'UPCOM' THEN ROUND(r.reference_price * 1.15, 2)
        END AS ceiling_price,
        CASE s.exchange
            WHEN 'HOSE'  THEN ROUND(r.reference_price * 0.93, 2)
            WHEN 'HNX'   THEN ROUND(r.reference_price * 0.90, 2)
            WHEN 'UPCOM' THEN ROUND(r.reference_price * 0.85, 2)
        END AS floor_price
    FROM new_prices p
    LEFT JOIN {{ ref('dim_stock') }} s ON p.symbol = s.symbol
    LEFT JOIN with_reference r ON r.symbol = p.symbol AND r.date_key = p.trade_date
)

SELECT * FROM enriched
