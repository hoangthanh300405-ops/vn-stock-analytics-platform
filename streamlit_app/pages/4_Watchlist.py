"""
pages/4_Watchlist.py - Watchlist cá nhân, CẦN đăng nhập.
Đọc trạng thái đăng nhập từ st.session_state (do app.py set qua authenticator).

LƯU Ý kiến trúc: watchlist nằm ở Supabase (Postgres), còn dim_stock/fct_price_daily
nằm ở file DuckDB tĩnh — 2 database KHÁC NHAU, không JOIN SQL trực tiếp được.
Phải query riêng từng nguồn rồi merge bằng pandas (chậm hơn 1 chút so với join
SQL, nhưng chấp nhận được vì watchlist mỗi người chỉ vài chục mã).
"""

import streamlit as st
from db import run_query, pg_query, pg_write

st.set_page_config(page_title="Watchlist", layout="wide")
st.title("Watchlist cá nhân")

if not st.session_state.get("authentication_status"):
    st.warning("Bạn cần đăng nhập ở trang chính để dùng Watchlist.")
    st.page_link("app.py", label="→ Về trang đăng nhập")
    st.stop()

username = st.session_state["username"]

# ── Thêm mã vào watchlist ──────────────────────────────────────────────
all_symbols = run_query("SELECT symbol FROM dim_stock ORDER BY symbol")["symbol"].tolist()
new_symbol = st.selectbox("Thêm mã vào watchlist", all_symbols)

if st.button("Thêm"):
    existing = pg_query(
        "SELECT 1 FROM watchlist WHERE username = %s AND symbol = %s",
        (username, new_symbol),
    )
    if len(existing) > 0:
        st.info(f"{new_symbol} đã có trong watchlist")
    else:
        pg_write(
            "INSERT INTO watchlist (username, symbol) VALUES (%s, %s)",
            (username, new_symbol),
        )
        st.success(f"Đã thêm {new_symbol}")
        st.rerun()

# ── Hiển thị watchlist với giá mới nhất ──────────────────────────────────
st.subheader("Danh sách theo dõi")

# Bước 1: lấy danh sách mã đang theo dõi từ Supabase
watchlist_symbols_df = pg_query(
    "SELECT symbol FROM watchlist WHERE username = %s ORDER BY symbol", (username,)
)

if watchlist_symbols_df.empty:
    st.info("Watchlist trống — thêm mã ở trên.")
else:
    symbols = watchlist_symbols_df["symbol"].tolist()

    # Bước 2: lấy thông tin công ty + giá mới nhất từ file DuckDB cho đúng
    # các mã đó (IN list build động, an toàn vì symbols đến từ DB không phải
    # input tự do của người dùng)
    placeholders = ", ".join(["?"] * len(symbols))
    detail_df = run_query(
        f"""
        SELECT d.symbol, d.company_name, d.sector_name,
               f.close_price, f.price_change_pct, f.date_key
        FROM dim_stock d
        LEFT JOIN fct_price_daily f ON d.symbol = f.symbol
            AND f.date_key = (SELECT MAX(date_key) FROM fct_price_daily WHERE symbol = d.symbol)
        WHERE d.symbol IN ({placeholders})
        ORDER BY d.symbol
        """,
        tuple(symbols),
    )

    st.dataframe(detail_df, use_container_width=True, hide_index=True)

    remove_symbol = st.selectbox("Xoá mã khỏi watchlist", symbols)
    if st.button("Xoá"):
        pg_write(
            "DELETE FROM watchlist WHERE username = %s AND symbol = %s",
            (username, remove_symbol),
        )
        st.success(f"Đã xoá {remove_symbol}")
        st.rerun()
