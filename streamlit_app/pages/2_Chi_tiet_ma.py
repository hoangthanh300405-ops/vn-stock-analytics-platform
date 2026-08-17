"""
pages/2_Chi_tiet_ma.py - Chọn 1 mã, xem giá tham chiếu/trần/sàn của phiên gần
nhất + biểu đồ giá/khối lượng theo thời gian + thông tin công ty phát hành
(từ dim_stock đã join company_profile).

Fix 2026-08-16: business_model/founded_date/charter_capital/number_of_employees
KHÔNG tồn tại trong dữ liệu thật trả về từ Company.overview() (nguồn VCI) — đã
đối chiếu company_profile.csv chạy thật. Đổi sang cột thật: market_cap,
business_description (đổi tên từ company_profile), foreigner_percentage,
maximum_foreign_percentage, issue_share, highest_price_1y, lowest_price_1y,
rating, target_price, analyst — xem chi tiết ở
dbt_vnstock/models/staging/vnstock/stg_vnstock__company_profile.sql.

LƯU Ý ĐƠN VỊ TIỀN TỆ (dễ nhầm nhất trong file này — 2 nguồn vnstock khác đơn vị):
  - Cột từ company_profile (market_cap, target_price, highest_price_1y,
    lowest_price_1y) -> nguồn Company.overview() trả FULL VNĐ.
  - Cột từ fct_price_daily (close_price, reference_price, ceiling_price,
    floor_price...) -> nguồn Quote().history() trả NGHÌN đồng.
  Dùng ĐÚNG hàm format tương ứng bên dưới (format_vnd_full vs format_vnd),
  không dùng lẫn.

Thêm 2026-08-16: khối giá tham chiếu/trần/sàn của phiên gần nhất
(reference_price/ceiling_price/floor_price, tính sẵn ở dbt trong
fct_price_daily.sql — xem "Fix #9" ở model đó).
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from db import run_query
from theme import (
    apply_global_style,
    page_header,
    metric_card,
    change_tone,
    format_pct,
    COLOR_UP,
    COLOR_DOWN,
)


def format_vnd(value) -> str:
    """fct_price_daily lưu giá theo đơn vị NGHÌN đồng (convention của
    Quote().history()) -> quy đổi sang VNĐ đầy đủ, có dấu phẩy ngăn cách."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value * 1000:,.0f} ₫"


def format_vnd_full(value) -> str:
    """Cột giá từ company_profile (Company.overview()) đã là FULL VNĐ sẵn
    rồi -> chỉ thêm dấu phẩy, KHÔNG nhân 1000 (khác format_vnd ở trên)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f} ₫"


def format_ty_dong(value) -> str:
    """Rút gọn số VNĐ nguyên (VD 1393079000000) về dạng '1,393 tỷ' cho dễ đọc."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value / 1e9:,.0f} tỷ"


def format_shares(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f} CP"


def format_ratio_pct(value) -> str:
    """foreigner_percentage/maximum_foreign_percentage là số thập phân (0.15 = 15%)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.2%}"


st.set_page_config(page_title="Chi tiết mã cổ phiếu", page_icon="🏢", layout="wide")
apply_global_style()
page_header("🏢", "Chi tiết mã cổ phiếu")

all_symbols = run_query("SELECT symbol FROM dim_stock ORDER BY symbol")["symbol"].tolist()
if not all_symbols:
    st.warning("Chưa có dữ liệu mã cổ phiếu.")
    st.stop()

symbol = st.selectbox("Chọn mã", all_symbols)

# ── Thông tin công ty phát hành ──────────────────────────────────────────
info = run_query(
    """
    SELECT company_name, exchange, sector_name, business_description,
           market_cap, foreigner_percentage, maximum_foreign_percentage,
           listing_date, issue_share, highest_price_1y, lowest_price_1y,
           rating, target_price, analyst
    FROM dim_stock WHERE symbol = ?
    """,
    (symbol,),
)

if not info.empty:
    row = info.iloc[0]
    st.subheader(f"{row['company_name'] or symbol} ({symbol})")

    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Sàn", row["exchange"] or "—")
    metric_card(c2, "Ngành", row["sector_name"] or "—")
    metric_card(c3, "Vốn hoá", format_ty_dong(row["market_cap"]))
    metric_card(c4, "Số CP lưu hành", format_shares(row["issue_share"]))

    c5, c6, c7, c8 = st.columns(4)
    metric_card(c5, "Room ngoại hiện tại", format_ratio_pct(row["foreigner_percentage"]))
    metric_card(c6, "Room ngoại tối đa", format_ratio_pct(row["maximum_foreign_percentage"]))
    metric_card(
        c7, "Ngày niêm yết",
        str(row["listing_date"])[:10] if pd.notna(row["listing_date"]) else "—",
    )
    target_label = format_vnd_full(row["target_price"])
    if row["rating"]:
        target_label += f" · {row['rating']}"
    metric_card(c8, "Giá mục tiêu (chuyên gia)", target_label)

    if row["analyst"]:
        st.caption(f"Khuyến nghị bởi: {row['analyst']}")
    if row["business_description"]:
        with st.expander("📄 Giới thiệu công ty"):
            st.write(row["business_description"])
else:
    st.info("Chưa có thông tin công ty cho mã này — có thể chưa chạy extract_company_profile.py")

st.divider()

# ── Giá phiên gần nhất: đóng cửa / tham chiếu / trần / sàn ────────────────
latest = run_query(
    """
    SELECT date_key, close_price, reference_price, ceiling_price, floor_price
    FROM fct_price_daily
    WHERE symbol = ?
    ORDER BY date_key DESC
    LIMIT 1
    """,
    (symbol,),
)

if not latest.empty:
    lr = latest.iloc[0]
    st.caption(f"Phiên gần nhất: {lr['date_key']}")

    delta_vs_ref = None
    if pd.notna(lr["close_price"]) and pd.notna(lr["reference_price"]) and lr["reference_price"]:
        delta_vs_ref = round((lr["close_price"] - lr["reference_price"]) / lr["reference_price"] * 100, 2)

    p1, p2, p3, p4 = st.columns(4)
    close_label = format_vnd(lr["close_price"])
    if delta_vs_ref is not None:
        close_label += f"  ({format_pct(delta_vs_ref)})"
    metric_card(p1, "Giá đóng cửa", close_label, tone=change_tone(delta_vs_ref))
    metric_card(p2, "Giá tham chiếu", format_vnd(lr["reference_price"]), tone="flat")
    metric_card(p3, "Giá trần", format_vnd(lr["ceiling_price"]))
    metric_card(p4, "Giá sàn", format_vnd(lr["floor_price"]))

    st.caption(
        "Giá trần/sàn là ước tính từ giá tham chiếu theo biên độ dao động của từng sàn "
        "(HOSE ±7%, HNX ±10%, UPCOM ±15%) — có thể lệch với số liệu thực tế trong các "
        "phiên đặc biệt (ngày đầu niêm yết, sau chia tách/trả cổ tức bằng cổ phiếu)."
    )

    # Cao/thấp 52 tuần: dùng thẳng số liệu THẬT từ Company.overview() (nếu có)
    # thay vì tự tính từ fct_price_daily — chính xác hơn vì không phụ thuộc
    # vào việc lịch sử giá trong warehouse đã backfill đủ 52 tuần hay chưa.
    h1y = info["highest_price_1y"].iloc[0] if not info.empty else None
    l1y = info["lowest_price_1y"].iloc[0] if not info.empty else None
    if pd.notna(h1y) or pd.notna(l1y):
        w1, w2 = st.columns(2)
        metric_card(w1, "Cao nhất 52 tuần", format_vnd_full(h1y))
        metric_card(w2, "Thấp nhất 52 tuần", format_vnd_full(l1y))

    st.divider()

# ── Biểu đồ giá + khối lượng ──────────────────────────────────────────────
date_range = st.slider("Số ngày gần nhất", min_value=30, max_value=365, value=90, step=30)

price_df = run_query(
    f"""
    SELECT date_key, open_price, high_price, low_price, close_price, volume
    FROM fct_price_daily
    WHERE symbol = ?
    ORDER BY date_key DESC
    LIMIT {date_range}
    """,
    (symbol,),
).sort_values("date_key")

if price_df.empty:
    st.warning("Chưa có dữ liệu giá cho mã này.")
    st.stop()

# Quy ước màu VN: nến tăng (đóng cửa > mở cửa) -> đỏ, nến giảm -> xanh lá
# (mặc định go.Candlestick dùng xanh=tăng/đỏ=giảm kiểu phương Tây, SAI cho
# người đọc quen bảng giá chứng khoán Việt Nam)
fig = go.Figure(data=[go.Candlestick(
    x=price_df["date_key"],
    open=price_df["open_price"],
    high=price_df["high_price"],
    low=price_df["low_price"],
    close=price_df["close_price"],
    name=symbol,
    increasing_line_color=COLOR_UP, increasing_fillcolor=COLOR_UP,
    decreasing_line_color=COLOR_DOWN, decreasing_fillcolor=COLOR_DOWN,
)])
fig.update_layout(
    title=f"Giá {symbol} — {date_range} ngày gần nhất",
    xaxis_rangeslider_visible=False,
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=50),
)
st.plotly_chart(fig, use_container_width=True)

# Cột khối lượng cũng tô theo màu tăng/giảm của phiên đó cho đồng bộ với nến
vol_colors = [
    COLOR_UP if c >= o else COLOR_DOWN
    for o, c in zip(price_df["open_price"], price_df["close_price"])
]
vol_fig = go.Figure(data=[go.Bar(x=price_df["date_key"], y=price_df["volume"], marker_color=vol_colors)])
vol_fig.update_layout(
    title="Khối lượng giao dịch",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=50),
)
st.plotly_chart(vol_fig, use_container_width=True)
