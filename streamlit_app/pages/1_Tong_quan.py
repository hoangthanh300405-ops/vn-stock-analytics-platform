"""
pages/1_Tong_quan.py - Tổng quan thị trường: top tăng/giảm, số mã tăng/giảm theo sàn
"""

import streamlit as st
import plotly.express as px
from db import run_query

st.set_page_config(page_title="Tổng quan thị trường", layout="wide")
st.title("Tổng quan thị trường")

latest_date_df = run_query("SELECT MAX(date_key) AS d FROM fct_price_daily")
if latest_date_df.empty or latest_date_df["d"].iloc[0] is None:
    st.warning("Chưa có dữ liệu. Kiểm tra lại pipeline ETL đã chạy chưa.")
    st.stop()

latest_date = latest_date_df["d"].iloc[0]
st.caption(f"Dữ liệu ngày: {latest_date}")

# KPI: số mã tăng/giảm/đứng giá
kpi_df = run_query(
    """
    SELECT
        SUM(CASE WHEN price_change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
        SUM(CASE WHEN price_change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
        SUM(CASE WHEN price_change_pct = 0 THEN 1 ELSE 0 END) AS flat_count
    FROM fct_price_daily
    WHERE date_key = ?
    """,
    (latest_date,),
)

col1, col2, col3 = st.columns(3)
col1.metric("Mã tăng giá", int(kpi_df["up_count"].iloc[0]))
col2.metric("Mã giảm giá", int(kpi_df["down_count"].iloc[0]))
col3.metric("Mã đứng giá", int(kpi_df["flat_count"].iloc[0]))

st.subheader("Top 10 tăng mạnh nhất")
top_gainers = run_query(
    """
    SELECT symbol, sector_name, close_price, price_change_pct
    FROM fct_price_daily
    WHERE date_key = ?
    ORDER BY price_change_pct DESC
    LIMIT 10
    """,
    (latest_date,),
)
st.dataframe(top_gainers, use_container_width=True, hide_index=True)

st.subheader("Top 10 giảm mạnh nhất")
top_losers = run_query(
    """
    SELECT symbol, sector_name, close_price, price_change_pct
    FROM fct_price_daily
    WHERE date_key = ?
    ORDER BY price_change_pct ASC
    LIMIT 10
    """,
    (latest_date,),
)
st.dataframe(top_losers, use_container_width=True, hide_index=True)

st.subheader("Khối lượng giao dịch theo sàn")
volume_by_exchange = run_query(
    """
    SELECT d.exchange, SUM(f.volume) AS total_volume
    FROM fct_price_daily f
    JOIN dim_stock d ON f.symbol = d.symbol
    WHERE f.date_key = ?
    GROUP BY d.exchange
    """,
    (latest_date,),
)
fig = px.bar(volume_by_exchange, x="exchange", y="total_volume")
st.plotly_chart(fig, use_container_width=True)
