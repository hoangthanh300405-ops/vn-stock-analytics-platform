"""
pages/2_Chi_tiet_ma.py - Chọn 1 mã, xem biểu đồ giá/khối lượng theo thời gian
+ thông tin công ty phát hành (từ dim_stock đã join company_profile)

Fix 2026-08-14: business_model/founded_date/charter_capital/number_of_employees
KHÔNG tồn tại trong dữ liệu thật trả về từ Company.overview() (xem giải thích
đầy đủ ở stg_vnstock__company_profile.sql) -> đổi sang các cột thật:
business_description (mô tả doanh nghiệp dạng text), market_cap (vốn hoá thị
trường, KHÔNG phải vốn điều lệ), foreigner_percentage (room ngoại hiện tại).

Fix 2026-08-15: st.metric() tự cắt bớt ("...") khi text/số quá dài (VD "Thực
phẩ...", "1,393,079,..."), vì cột quá hẹp so với nội dung -> đổi sang
st.markdown (tự xuống dòng, không cắt) + rút gọn market_cap về đơn vị "tỷ
đồng" cho dễ đọc thay vì in nguyên số VNĐ.

Thêm 2026-08-15: MA20/MA50 (đường trung bình động) + RSI(14) vẽ thêm từ
chính fct_price_daily đã có sẵn, KHÔNG cần nguồn dữ liệu mới. Riêng so sánh
với VN-Index CHƯA làm được — extract_vnstock.py hiện chỉ lặp qua các mã có
type == "STOCK" từ Listing().symbols_by_exchange(), không thu thập chỉ số
VNINDEX -> cần sửa pipeline ETL trước (thêm lời gọi Quote cho mã chỉ số),
việc này để ở phần sau, không chỉ sửa được bằng code Streamlit.
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from db import run_query


def format_ty_dong(value) -> str:
    """Rút gọn số VNĐ nguyên (VD 1393079000000) về dạng '1,393 tỷ' cho dễ đọc."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value / 1e9:,.0f} tỷ"


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

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
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Sàn**  \n{row['exchange'] or '—'}")
        st.markdown(f"**Ngành**  \n{row['sector_name'] or '—'}")
    with c2:
        st.markdown(f"**Vốn hoá**  \n{format_ty_dong(row['market_cap'])}")
        room_ngoai = (
            f"{row['foreigner_percentage']:.1%}"
            if row["foreigner_percentage"] is not None and not pd.isna(row["foreigner_percentage"])
            else "—"
        )
        st.markdown(f"**Room ngoại**  \n{room_ngoai}")
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

# MA20/MA50 tính trên chính cửa sổ dữ liệu đã lấy (date_range ngày gần nhất)
# -> vài phiên đầu của cửa sổ sẽ chưa đủ 20/50 phiên để tính (hiện NaN, plotly
# tự bỏ qua không vẽ đoạn đó), không cần lấy thêm dữ liệu lịch sử ngoài phạm vi.
price_df["ma20"] = price_df["close_price"].rolling(window=20, min_periods=20).mean()
price_df["ma50"] = price_df["close_price"].rolling(window=50, min_periods=50).mean()
price_df["rsi14"] = compute_rsi(price_df["close_price"], period=14)

show_ma20 = st.checkbox("Hiện MA20", value=True)
show_ma50 = st.checkbox("Hiện MA50", value=True)

fig = go.Figure(data=[go.Candlestick(
    x=price_df["date_key"],
    open=price_df["open_price"],
    high=price_df["high_price"],
    low=price_df["low_price"],
    close=price_df["close_price"],
    name=symbol,
)])
if show_ma20:
    fig.add_trace(go.Scatter(
        x=price_df["date_key"], y=price_df["ma20"],
        mode="lines", name="MA20", line=dict(width=1.3, color="#f5a623"),
    ))
if show_ma50:
    fig.add_trace(go.Scatter(
        x=price_df["date_key"], y=price_df["ma50"],
        mode="lines", name="MA50", line=dict(width=1.3, color="#4a90d9"),
    ))
fig.update_layout(title=f"Giá {symbol} — {date_range} ngày gần nhất", xaxis_rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

vol_fig = go.Figure(data=[go.Bar(x=price_df["date_key"], y=price_df["volume"])])
vol_fig.update_layout(title="Khối lượng giao dịch")
st.plotly_chart(vol_fig, use_container_width=True)

# ── RSI(14) ────────────────────────────────────────────────────────────────
if price_df["rsi14"].notna().any():
    rsi_fig = go.Figure(data=[go.Scatter(
        x=price_df["date_key"], y=price_df["rsi14"], mode="lines", name="RSI(14)",
        line=dict(width=1.3, color="#8e44ad"),
    )])
    rsi_fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Quá mua (70)")
    rsi_fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Quá bán (30)")
    rsi_fig.update_layout(title="RSI(14)", yaxis_range=[0, 100])
    st.plotly_chart(rsi_fig, use_container_width=True)
else:
    st.caption(
        f"Chưa đủ {14 + 1} phiên trong cửa sổ {date_range} ngày để tính RSI(14) "
        "— tăng \"Số ngày gần nhất\" ở trên để xem."
    )
