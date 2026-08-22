"""
config.py - Cấu hình chung cho pipeline dự đoán xu hướng giá (ML_PROJECT_SPEC.model =
LightGBM classifier 3 lớp). Mọi hằng số dùng ở nhiều module đều đặt ở đây để dễ tinh
chỉnh mà không phải sửa rải rác trong code.
"""

# ── Kết nối dữ liệu ────────────────────────────────────────────────────
# Tái dùng ĐÚNG biến môi trường của pipeline ETL hiện có (extract_vnstock.py,
# build_duckdb_file.py) — không thêm biến cấu hình riêng cho phần ML.
DUCKDB_FILE_PATH_ENV = "DUCKDB_FILE_PATH"
DEFAULT_DUCKDB_PATH = "vnstock.duckdb"

# ── Nhãn (label) ────────────────────────────────────────────────────────
# epsilon: ngưỡng % thay đổi giá ngày kế tiếp để phân vào lớp "tăng"/"giảm" so với "đứng".
# 0.5% được chọn vì tổng phí giao dịch thực tế ở VN (mua + bán) thường quanh 0.3-0.5%,
# một biến động nhỏ hơn phí gần như không có ý nghĩa hành động (actionability) -> coi
# là "đứng" thay vì ép model phải phân biệt nhiễu nhỏ hơn cả chi phí giao dịch.
LABEL_EPSILON_PCT = 0.5

# ── Loại phiên trần/sàn khỏi NHÃN ────────────────────────────────────────
# Phiên đóng cửa sát trần/sàn (so với ceiling_price/floor_price ước tính sẵn trong
# fct_price_daily) khiến % thay đổi giá bị "kẹp" bởi biên độ dao động, không phản ánh
# đúng cung-cầu thật (lệnh dư mua/bán không khớp được do hết biên độ). Dùng làm FEATURE
# của ngày sau vẫn hợp lệ (đó là thông tin thị trường đã quan sát được), nhưng dùng làm
# chính NHÃN của ngày đó thì dễ gây nhiễu/label leakage kiểu khác -> loại khỏi tập nhãn.
LIMIT_BAND_TOLERANCE_PCT = 0.1  # sai số cho phép khi so sánh close với ceiling/floor

# ── Feature engineering ──────────────────────────────────────────────────
MA_WINDOWS = [5, 10, 20, 50]
EMA_WINDOWS = [12, 26]
RSI_WINDOW = 14
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
BOLLINGER_WINDOW, BOLLINGER_STD = 20, 2
VOLUME_ZSCORE_WINDOW = 20
LAG_RETURN_PERIODS = [1, 2, 3, 5, 10]
SECTOR_MOMENTUM_WINDOW = 5  # số ngày tính momentum tương đối theo ngành

# Số phiên lịch sử tối thiểu/mã để rolling window dài nhất (MA50) không toàn NaN
MIN_HISTORY_DAYS_PER_SYMBOL = max(MA_WINDOWS) + 5

# Fix REVIEW_FINDINGS #10: song song hoá vòng lặp feature theo từng mã. -1 = dùng hết
# core sẵn có. Đặt = 1 nếu môi trường chạy không hỗ trợ multiprocessing (không đổi kết
# quả, chỉ đổi thời gian chạy).
FEATURE_ENGINEERING_N_JOBS = -1

# ── Walk-forward validation ───────────────────────────────────────────────
N_WALKFORWARD_SPLITS = 5
EMBARGO_DAYS = 2  # đệm giữa cuối train và đầu validation, chống rò nhãn t+1 qua ranh giới fold

# ── LightGBM + Optuna ──────────────────────────────────────────────────
# Giữ số trial nhỏ: retrain phải chạy hàng tuần trong giới hạn free tier của GitHub
# Actions (xem constraints trong spec) — mỗi trial chỉ chạy trên 1 fold (fold cuối,
# xem train.py) để không nhân thời gian lên N_WALKFORWARD_SPLITS lần.
OPTUNA_N_TRIALS = 25
OPTUNA_TIMEOUT_SECONDS = 20 * 60
LGBM_EARLY_STOPPING_ROUNDS = 50
LGBM_NUM_BOOST_ROUND = 2000
RANDOM_STATE = 42

CLASS_NAMES = ["down", "flat", "up"]
CLASS_LABEL_MAP = {"down": 0, "flat": 1, "up": 2}

# Feature categorical — đặt ở đây (không phải train.py) để feature_list.json,
# train.py và trang Streamlit inference đều import từ CÙNG 1 nguồn, tránh lệch
# danh sách giữa nơi train và nơi predict (xem REVIEW_FINDINGS #3).
CATEGORICAL_FEATURES = ["exchange", "sector_name"]

# ── Backtest (minh hoạ, KHÔNG phải chiến lược trading thật) ──────────────
BACKTEST_LONG_PROB_THRESHOLD = 0.45
TRADING_COST_PCT = 0.35  # phí round-trip (mua+bán) ước tính, trừ vào return backtest

# ── Output paths (artifact publish qua GitHub Release, giống vnstock.duckdb) ─────
MODEL_OUTPUT_PATH = "price_trend_lgbm.txt"
FEATURE_LIST_OUTPUT_PATH = "feature_list.json"
METADATA_OUTPUT_PATH = "model_metadata.json"
SHAP_SUMMARY_PLOT_PATH = "shap_summary.png"
CALIBRATION_PLOT_PATH = "calibration_curve_up.png"
BACKTEST_REPORT_PATH = "backtest_report.json"
