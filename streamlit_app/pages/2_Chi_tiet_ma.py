"""
pages/2_Chi_tiet_ma.py - Chọn 1 mã, xem biểu đồ giá/khối lượng theo thời gian
+ thông tin công ty phát hành (từ dim_stock đã join company_profile)
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
    SELECT company_name, exchange, sector_name, business_model,
           founded_date, listing_date, charter_capital, number_of_employees
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
    c3.metric("Vốn điều lệ", f"{row['charter_capital']:,.0f}" if row["charter_capital"] else "—")
    c4.metric("Số nhân viên", f"{row['number_of_employees']:,.0f}" if row["number_of_employees"] else "—")
    if row["business_model"]:
        with st.expander("Mô hình kinh doanh"):
            st.write(row["business_model"])
else:
    st.info("Chưa có thông tin công ty cho mã này — có thể chưa chạy extract_company_profile.py")

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
