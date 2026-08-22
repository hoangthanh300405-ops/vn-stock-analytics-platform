"""
pages/5_Du_doan_xu_huong.py - Dự đoán xu hướng giá ngày kế tiếp (down/flat/up) cho 1 mã,
dùng model LightGBM đã train bởi train_price_trend_model.py (weekly_retrain_model.yml).

CÁCH CÀI ĐẶT (bước thủ công 1 lần khi ghép vào streamlit_app/ hiện có):
  1. Copy thư mục ml_price_trend/ (chứa features.py) vào streamlit_app/ml_price_trend/
     — trang này CHỈ dùng lại build_feature_set() để đảm bảo feature lúc dự đoán tính
     Y HỆT lúc train (không viết lại logic feature 1 lần nữa ở đây, tránh lệch nhau).
  2. Thêm "lightgbm", "ta" vào streamlit_app/requirements.txt.
  3. Đổi MODEL_RELEASE_ASSET_BASE_URL bên dưới thành đúng repo GitHub của bạn (cùng repo
     với DUCKDB_ASSET_URL trong db.py, chỉ khác release tag "latest-model").

Model + feature_list.json + model_metadata.json (chứa SHAP importance đã tính sẵn lúc
train) được tải qua GitHub Release "latest-model" — publish bởi weekly_retrain_model.yml,
đúng kiến trúc "file tĩnh qua GitHub Release" đã dùng cho vnstock.duckdb (xem db.py).

LƯU Ý: đây là công cụ MINH HOẠ cho mục đích phỏng vấn/CV — không phải khuyến nghị đầu tư.
"""

import json

import pandas as pd
import requests
import streamlit as st
import lightgbm as lgb
import plotly.express as px

from db import run_query, get_analytics_connection  # tái dùng kết nối DuckDB đã có sẵn

# Đổi <user>/<repo> giống DUCKDB_ASSET_URL trong db.py
MODEL_RELEASE_ASSET_BASE_URL = (
    "https://github.com/hoangthanh300405-ops/vn-stock-analytics-platform/releases/download/latest-model"
)
LOCAL_MODEL_DIR = "/tmp/price_trend_model"

st.set_page_config(page_title="Dự đoán xu hướng giá", page_icon="🔮", layout="wide")
st.title("🔮 Dự đoán xu hướng giá ngày kế tiếp")
st.caption(
    "Model LightGBM 3 lớp (giảm / đứng / tăng), retrain hàng tuần — công cụ minh hoạ cho "
    "mục đích phỏng vấn/CV, **không phải khuyến nghị đầu tư**."
)


@st.cache_resource(ttl=6 * 3600)  # model chỉ retrain 1 lần/tuần -> cache dài hơn giá (1h)
def load_model_artifacts():
    import os
    os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)

    files = ["price_trend_lgbm.txt", "feature_list.json", "model_metadata.json"]
    local_paths = {}
    for fname in files:
        url = f"{MODEL_RELEASE_ASSET_BASE_URL}/{fname}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        local_path = f"{LOCAL_MODEL_DIR}/{fname}"
        with open(local_path, "wb") as f:
            f.write(resp.content)
        local_paths[fname] = local_path

    booster = lgb.Booster(model_file=local_paths["price_trend_lgbm.txt"])
    with open(local_paths["feature_list.json"], encoding="utf-8") as f:
        feature_info = json.load(f)
    with open(local_paths["model_metadata.json"], encoding="utf-8") as f:
        metadata = json.load(f)
    return booster, feature_info, metadata


try:
    booster, feature_info, metadata = load_model_artifacts()
except Exception as e:
    st.error(
        "Chưa tải được model — kiểm tra weekly_retrain_model.yml đã chạy thành công lần nào "
        f"chưa (Release 'latest-model'), hoặc MODEL_RELEASE_ASSET_BASE_URL đã đúng repo chưa.\n\n{e}"
    )
    st.stop()

feature_cols = feature_info["feature_cols"]
categorical_features = feature_info.get("categorical_features", [])
# Fix REVIEW_FINDINGS #3: KHÔNG tự suy category từ dữ liệu hiện tại (1 dòng duy nhất
# sẽ luôn thiếu gần hết category khác) — dùng lại ĐÚNG vocab đã lưu lúc train.
categorical_vocab = feature_info.get("categorical_vocab", {})
if categorical_features and not categorical_vocab:
    st.error(
        "feature_list.json thiếu categorical_vocab — model này train bằng phiên bản "
        "pipeline CŨ (trước fix REVIEW_FINDINGS #3). Retrain lại qua "
        "weekly_retrain_model.yml trước khi dùng trang này."
    )
    st.stop()

all_symbols = run_query("SELECT symbol FROM dim_stock ORDER BY symbol")["symbol"].tolist()
symbol = st.selectbox("Chọn mã", all_symbols)

# ── Tính feature cho mã đã chọn ────────────────────────────────────────────
# Import build_feature_set từ package đã copy vào streamlit_app/ml_price_trend/ (xem
# hướng dẫn cài đặt ở docstring đầu file) — dùng lại NGUYÊN VẸN logic feature lúc train,
# không viết lại để tránh 2 nơi tính feature bị lệch nhau theo thời gian.
try:
    from ml_price_trend.features import build_feature_set
except ImportError:
    st.error(
        "Chưa copy thư mục ml_price_trend/ vào streamlit_app/ — xem hướng dẫn cài đặt ở "
        "đầu file pages/5_Du_doan_xu_huong.py."
    )
    st.stop()

con = get_analytics_connection()
history = con.execute(
    """
    SELECT f.symbol, f.date_key, f.exchange, f.sector_name,
           f.open_price, f.high_price, f.low_price, f.close_price, f.volume,
           f.reference_price, f.ceiling_price, f.floor_price
    FROM fct_price_daily f
    WHERE f.symbol = ?
    ORDER BY f.date_key
    """,
    (symbol,),
).fetchdf()

if len(history) < 60:
    st.warning(f"Mã {symbol} chưa đủ lịch sử giá (cần tối thiểu ~60 phiên) để tính đầy đủ feature.")
    st.stop()

history["date_key"] = pd.to_datetime(history["date_key"])
featured, _ = build_feature_set(history)
latest_row = featured.sort_values("date_key").iloc[[-1]].copy()

missing_features = [c for c in feature_cols if c not in latest_row.columns]
if missing_features:
    st.error(f"Thiếu feature so với lúc train: {missing_features} — kiểm tra lại phiên bản ml_price_trend/.")
    st.stop()

for c in categorical_features:
    # Fix #3: pd.Categorical với categories CỐ ĐỊNH từ lúc train (không để pandas tự
    # suy từ dữ liệu hiện tại — 1 dòng chỉ có đúng 1 giá trị mỗi cột categorical).
    latest_row[c] = pd.Categorical(latest_row[c], categories=categorical_vocab.get(c, []))

if latest_row[feature_cols].isna().any(axis=None):
    st.warning("Một số feature bị thiếu (NaN) ở phiên gần nhất — kết quả dự đoán có thể kém tin cậy.")

proba = booster.predict(latest_row[feature_cols])[0]
class_names = ["Giảm", "Đứng giá", "Tăng"]

st.subheader(f"Dự đoán cho {symbol} — phiên kế tiếp sau {latest_row['date_key'].iloc[0].date()}")

c1, c2, c3 = st.columns(3)
c1.metric("P(Giảm)", f"{proba[0]:.1%}")
c2.metric("P(Đứng giá)", f"{proba[1]:.1%}")
c3.metric("P(Tăng)", f"{proba[2]:.1%}")

proba_df = pd.DataFrame({"Xu hướng": class_names, "Xác suất": proba})
fig = px.bar(proba_df, x="Xu hướng", y="Xác suất", color="Xu hướng", range_y=[0, 1])
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── SHAP feature importance (tính SẴN lúc train, lưu trong model_metadata.json) ─────────
# Không tính SHAP realtime cho từng lượt user bấm chọn mã (chậm, tốn CPU cho mỗi request) —
# hiển thị importance TOÀN CỤC từ lần train gần nhất, đủ để minh hoạ model dựa vào yếu tố
# nào nhiều nhất cho lớp "tăng", đúng mục đích giải thích cho phỏng vấn.
st.subheader("Yếu tố ảnh hưởng nhiều nhất tới dự đoán 'Tăng' (SHAP, toàn cục từ lần train gần nhất)")
shap_importance = metadata.get("top_shap_features_class_up", {})
if shap_importance:
    shap_df = pd.DataFrame({"Feature": list(shap_importance.keys()), "Mức ảnh hưởng (|SHAP| TB)": list(shap_importance.values())})
    shap_df = shap_df.sort_values("Mức ảnh hưởng (|SHAP| TB)", ascending=True)
    fig2 = px.bar(shap_df, x="Mức ảnh hưởng (|SHAP| TB)", y="Feature", orientation="h")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Chưa có dữ liệu SHAP trong model_metadata.json.")

with st.expander("Thông tin model (metadata lần train gần nhất)"):
    st.write(f"Train tới ngày: {metadata.get('final_train_end_date')}")
    st.write(f"Số mã dùng để train: {metadata.get('n_symbols')}")
    st.write(f"Ngưỡng epsilon phân lớp: ±{metadata.get('label_epsilon_pct')}%")
    holdout = metadata.get("holdout_metrics")
    if holdout:
        st.write(
            f"**Hiệu năng blind thật (holdout fold #{metadata.get('holdout_fold_id')}, "
            "KHÔNG dùng trong bước chọn hyperparameter):** "
            f"macro F1 = {holdout.get('macro_f1', 0):.3f}, baseline "
            f"(giữ nguyên xu hướng hôm trước) = "
            f"{holdout.get('baseline_same_as_yesterday_macro_f1') or 0:.3f}"
        )
    bt = metadata.get("backtest", {})
    if bt:
        st.write(
            f"Backtest minh hoạ (holdout fold, blind): return chiến lược "
            f"{bt.get('strategy_total_return_pct', 0):.2f}% vs buy-and-hold "
            f"{bt.get('buy_hold_total_return_pct', 0):.2f}%, Sharpe "
            f"{bt.get('strategy_sharpe', 0):.2f} vs {bt.get('buy_hold_sharpe', 0):.2f}"
        )
    st.caption(
        "Các fold khác trong model_metadata.json['fold_results'] có thể có cờ "
        "is_used_for_hyperparameter_tuning=True — số liệu của fold đó KHÔNG phải "
        "hiệu năng blind, chỉ số holdout fold ở trên mới đáng tin cậy."
    )
