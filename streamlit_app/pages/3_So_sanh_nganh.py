"""
pages/3_So_sanh_nganh.py - So sánh xu hướng % thay đổi giá trung bình theo ngành
"""

import streamlit as st
import plotly.express as px
from db import run_query

st.set_page_config(page_title="So sánh ngành", layout="wide")
st.title("So sánh xu hướng theo ngành")

latest_date_df = run_query("SELECT MAX(date_key) AS d FROM fct_price_daily")
if latest_date_df.empty or latest_date_df["d"].iloc[0] is None:
    st.warning("Chưa có dữ liệu.")
    st.stop()
latest_date = latest_date_df["d"].iloc[0]

st.subheader(f"% thay đổi giá trung bình theo ngành — {latest_date}")
sector_today = run_query(
    """
    SELECT sector_name, AVG(price_change_pct) AS avg_change_pct, COUNT(*) AS num_stocks
    FROM fct_price_daily
    WHERE date_key = ? AND sector_name IS NOT NULL
    GROUP BY sector_name
    ORDER BY avg_change_pct DESC
    """,
    (latest_date,),
)
fig = px.bar(
    sector_today, x="avg_change_pct", y="sector_name", orientation="h",
    color="avg_change_pct", color_continuous_scale="RdYlGn",
    labels={"avg_change_pct": "% thay đổi TB", "sector_name": "Ngành"},
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Xu hướng ngành theo thời gian (30 ngày gần nhất)")
trend_df = run_query(
    """
    SELECT date_key, sector_name, AVG(price_change_pct) AS avg_change_pct
    FROM fct_price_daily
    WHERE sector_name IS NOT NULL
      AND date_key >= (SELECT MAX(date_key) - INTERVAL 30 DAY FROM fct_price_daily)
    GROUP BY date_key, sector_name
    ORDER BY date_key
    """
)
line_fig = px.line(trend_df, x="date_key", y="avg_change_pct", color="sector_name")
st.plotly_chart(line_fig, use_container_width=True)
