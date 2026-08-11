"""
app.py - Entry point. Xử lý đăng nhập/đăng ký, sau đó điều hướng sang các trang
trong pages/ (Streamlit multi-page app tự động nhận thư mục pages/).
"""

import streamlit as st
import streamlit_authenticator as stauth
from db import bootstrap_app_tables, pg_query, pg_write

st.set_page_config(page_title="VN Stock Dashboard", layout="wide")

bootstrap_app_tables()


def load_credentials() -> dict:
    """Đọc user hiện có từ Supabase, dựng thành config cho streamlit-authenticator"""
    df = pg_query("SELECT username, name, email, password_hash FROM users")
    usernames = {}
    for _, row in df.iterrows():
        usernames[row["username"]] = {
            "name": row["name"],
            "email": row["email"],
            "password": row["password_hash"],  # đã hash sẵn từ lúc đăng ký
        }
    return {"usernames": usernames}


credentials = load_credentials()

authenticator = stauth.Authenticate(
    credentials,
    cookie_name="vnstock_dashboard_auth",
    key=st.secrets["COOKIE_SIGNING_KEY"],  # chuỗi bí mật tự đặt, lưu trong Streamlit Secrets
    cookie_expiry_days=7,
)

st.title("📈 VN Stock Dashboard")

tab_login, tab_register = st.tabs(["Đăng nhập", "Đăng ký"])

with tab_login:
    authenticator.login(location="main")

    if st.session_state.get("authentication_status") is False:
        st.error("Sai tên đăng nhập hoặc mật khẩu")
    elif st.session_state.get("authentication_status") is None:
        st.info("Nhập tên đăng nhập và mật khẩu, hoặc xem không cần đăng nhập ở các trang bên trái (trừ Watchlist)")

    if st.session_state.get("authentication_status"):
        st.success(f"Xin chào {st.session_state['name']}!")
        authenticator.logout(location="main")
        st.page_link("pages/4_Watchlist.py", label="→ Vào Watchlist của bạn")

with tab_register:
    st.subheader("Tạo tài khoản mới")
    new_username = st.text_input("Tên đăng nhập", key="reg_username")
    new_name = st.text_input("Tên hiển thị", key="reg_name")
    new_email = st.text_input("Email", key="reg_email")
    new_password = st.text_input("Mật khẩu", type="password", key="reg_password")

    if st.button("Đăng ký"):
        if not new_username or not new_password:
            st.error("Cần nhập tên đăng nhập và mật khẩu")
        else:
            existing = pg_query(
                "SELECT username FROM users WHERE username = %s", (new_username,)
            )
            if len(existing) > 0:
                st.error("Tên đăng nhập đã tồn tại")
            else:
                hashed = stauth.Hasher([new_password]).generate()[0]
                pg_write(
                    "INSERT INTO users (username, name, email, password_hash) VALUES (%s, %s, %s, %s)",
                    (new_username, new_name, new_email, hashed),
                )
                st.success("Đăng ký thành công! Chuyển sang tab Đăng nhập để vào.")

st.divider()
st.caption(
    "Các trang Tổng quan / Chi tiết mã / So sánh ngành xem được không cần đăng nhập. "
    "Watchlist cá nhân cần đăng nhập."
)
