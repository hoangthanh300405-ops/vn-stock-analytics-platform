"""
db.py - Kết nối dùng chung cho toàn bộ Streamlit app.

Đổi kiến trúc (thay MotherDuck): MotherDuck free tier giới hạn 10 giờ
compute/tháng (chính sách mới) -> rủi ro demo CV bị gián đoạn giữa tháng.

Tách làm 2 nguồn:
  1. Phân tích (đọc nhiều, ghi 1 lần/ngày từ workflow) -> file .duckdb tĩnh,
     tải về từ GitHub Release, query LOCAL (embedded) -> không tốn compute
     cloud, không giới hạn giờ.
  2. Watchlist/auth (ghi liên tục từ nhiều user) -> Supabase Postgres free
     tier (luôn bật, không tính theo giờ compute) -> đúng bản chất OLTP,
     tách khỏi phần OLAP như đã ghi chú ở review trước (finding #8).
"""

import streamlit as st
import duckdb
import psycopg2
import psycopg2.extras
import pandas as pd
import requests

# ── Phần 1: Phân tích - file DuckDB tĩnh ──────────────────────────────────

# Đổi <user>/<repo> thành đúng GitHub repo của bạn
DUCKDB_ASSET_URL = "https://github.com/<user>/<repo>/releases/download/latest-data/vnstock.duckdb"
LOCAL_DUCKDB_PATH = "/tmp/vnstock.duckdb"


@st.cache_resource(ttl=3600)  # tải lại mỗi giờ - đủ mới vì data chỉ update 1 lần/ngày
def get_analytics_connection() -> duckdb.DuckDBPyConnection:
    resp = requests.get(DUCKDB_ASSET_URL, timeout=60)
    resp.raise_for_status()
    with open(LOCAL_DUCKDB_PATH, "wb") as f:
        f.write(resp.content)
    return duckdb.connect(LOCAL_DUCKDB_PATH, read_only=True)


@st.cache_data(ttl=3600)
def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = get_analytics_connection()
    return con.execute(sql, params).fetchdf()


# ── Phần 2: Watchlist/auth - Supabase Postgres ────────────────────────────

@st.cache_resource
def get_pg_connection():
    return psycopg2.connect(st.secrets["SUPABASE_DB_URL"])


def pg_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = get_pg_connection()
    with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def pg_write(sql: str, params: tuple = ()):
    """Cho INSERT/UPDATE/DELETE (watchlist, user) - luôn ghi thật, có commit"""
    con = get_pg_connection()
    with con.cursor() as cur:
        cur.execute(sql, params)
    con.commit()


def bootstrap_app_tables():
    """Tạo bảng auth/watchlist trên Supabase nếu chưa có"""
    con = get_pg_connection()
    with con.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username VARCHAR PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                password_hash VARCHAR,
                created_at TIMESTAMP DEFAULT now()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                username VARCHAR REFERENCES users(username),
                symbol VARCHAR,
                added_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (username, symbol)
            )
        """)
    con.commit()
