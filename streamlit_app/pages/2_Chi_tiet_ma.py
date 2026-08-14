"""
pages/2_Chi_tiet_ma.py - Chọn 1 mã, xem biểu đồ giá/khối lượng theo thời gian
+ thông tin công ty phát hành (từ dim_stock đã join company_profile)

Fix 2026-08-14: business_model/founded_date/charter_capital/number_of_employees
KHÔNG tồn tại trong dữ liệu thật trả về từ Company.overview() (xem giải thích
đầy đủ ở stg_vnstock__company_profile.sql) -> đổi sang các cột thật:
business_description (mô tả doanh nghiệp dạng text), market_cap (vốn hoá thị
trường, KHÔNG phải vốn điều lệ), foreigner_percentage (room ngoại hiện tại).
"""

import streamlit as st
import plotly.graph_objects as go
from db import run_query

st.set_page_config(page_title="Chi tiết mã cổ phiếu", layout="wide")
st.title("Chi tiết mã cổ phiếu")

all_symbols = run_query("SELECT symbol FROM dim_stock ORDER BY symbol")["symbol"].tolist()
if not all_symbols:
    st.warning("Chưa có dữ liệu mã cổ phiếu.")
    st.stop()

symbol = st.selectbox("Chọn mã", all_symbols)

# ── Thông tin công ty phát hành ──────────────────────────────────────────
info = run_query(
    """
    SELECT company_name, exchange, sector_name, business_description,
           listing_date, market_cap, foreigner_percentage
    FROM dim_stock WHERE symbol = ?
    """,
    (symbol,),
)

if not info.empty:
    row = info.iloc[0]
    st.subheader(f"{row['company_name'] or symbol} ({symbol})")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sàn", row["exchange"] or "—")
    c2.metric("Ngành", row["sector_name"] or "—")
    c3.metric("Vốn hoá", f"{row['market_cap']:,.0f}" if row["market_cap"] else "—")
    c4.metric(
        "Room ngoại",
        f"{row['foreigner_percentage']:.1%}" if row["foreigner_percentage"] is not None else "—",
    )
    if row["listing_date"]:
        st.caption(f"Ngày niêm yết: {row['listing_date']}")
    if row["business_description"]:
        with st.expander("Giới thiệu công ty"):
            st.write(row["business_description"])
else:
    st.info("Chưa có thông tin công ty cho mã này — có thể chưa chạy weekly_company_profile.yml")

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

fig = go.Figure(data=[go.Candlestick(
    x=price_df["date_key"],
    open=price_df["open_price"],
    high=price_df["high_price"],
    low=price_df["low_price"],
    close=price_df["close_price"],
    name=symbol,
)])
fig.update_layout(title=f"Giá {symbol} — {date_range} ngày gần nhất", xaxis_rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

vol_fig = go.Figure(data=[go.Bar(x=price_df["date_key"], y=price_df["volume"])])
vol_fig.update_layout(title="Khối lượng giao dịch")
st.plotly_chart(vol_fig, use_container_width=True)
