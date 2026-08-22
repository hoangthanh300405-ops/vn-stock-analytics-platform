"""
data_loading.py - Đọc dữ liệu ĐÃ CLEAN từ warehouse DuckDB (fct_price_daily + dim_stock,
đã qua dbt) cho pipeline dự đoán xu hướng giá. KHÔNG tự transform lại raw — dùng lại đúng
kết quả staging/marts đã có, đúng theo pipeline.stage "Trích xuất dữ liệu" trong spec.
"""
import logging
import os

import duckdb
import pandas as pd

from . import config

log = logging.getLogger(__name__)

# Fix REVIEW_FINDINGS #11 — ĐÃ CÂN NHẮC LẠI phạm vi so với đề xuất ban đầu ("thêm
# foreigner_percentage/rating/target_price làm feature"): dim_stock/company_profile
# trong warehouse KHÔNG được historize (full refresh, không phải SCD Type 2) — mỗi lần
# join, MỌI dòng giá lịch sử (kể cả của 2-3 năm trước) đều nhận đúng 1 giá trị company
# profile là giá trị TẠI THỜI ĐIỂM EXTRACT GẦN NHẤT, không phải giá trị "tại đúng ngày
# đó" trong quá khứ. Với các trường biến động theo thời gian (foreigner_percentage thay
# đổi liên tục theo giao dịch thật, rating/target_price do chuyên gia cập nhật định kỳ),
# dùng làm FEATURE huấn luyện trên dữ liệu lịch sử sẽ tạo ra 1 dạng look-ahead bias khác
# (mô hình "nhìn thấy" thông tin của hiện tại khi học từ quá khứ) — nên KHÔNG đưa các
# trường đó vào, dù có sẵn trong dim_stock.
# maximum_foreign_percentage (room ngoại TỐI ĐA cho phép) là ngoại lệ hợp lý: đây là 1
# hạn mức mang tính pháp lý/điều lệ công ty, gần như không đổi theo thời gian (khác hẳn
# foreigner_percentage - tỉ lệ sở hữu THỰC TẾ, đổi theo từng phiên) -> rủi ro point-in-
# time-correctness thấp hơn nhiều, chấp nhận dùng làm feature tĩnh.


def load_price_panel(duckdb_path: str | None = None) -> pd.DataFrame:
    """Đọc toàn bộ fct_price_daily join dim_stock (sector_name, exchange, market_cap),
    trả về 1 DataFrame dạng panel (nhiều mã x nhiều ngày), sort theo (symbol, date_key).

    read_only=True: pipeline train KHÔNG được ghi vào file .duckdb dùng chung với
    Streamlit/dbt (tránh tranh chấp khoá file khi chạy song song trong CI).
    """
    path = duckdb_path or os.environ.get(config.DUCKDB_FILE_PATH_ENV, config.DEFAULT_DUCKDB_PATH)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Không tìm thấy file DuckDB tại '{path}'. Tải file vnstock.duckdb (GitHub Release "
            "'latest-data' của pipeline ETL hiện có) về trước khi chạy training, hoặc set biến "
            f"môi trường {config.DUCKDB_FILE_PATH_ENV}."
        )

    con = duckdb.connect(path, read_only=True)
    try:
        df = con.execute(
            """
            SELECT
                f.symbol, f.date_key, f.exchange, f.sector_name,
                f.open_price, f.high_price, f.low_price, f.close_price, f.volume,
                f.reference_price, f.ceiling_price, f.floor_price,
                d.market_cap, d.maximum_foreign_percentage
            FROM fct_price_daily f
            LEFT JOIN dim_stock d ON f.symbol = d.symbol
            ORDER BY f.symbol, f.date_key
            """
        ).fetchdf()
    finally:
        con.close()

    log.info(f"Đọc {len(df)} dòng, {df['symbol'].nunique()} mã từ {path}")
    df["date_key"] = pd.to_datetime(df["date_key"])
    return df


def filter_sparse_symbols(df: pd.DataFrame, min_days: int = config.MIN_HISTORY_DAYS_PER_SYMBOL) -> pd.DataFrame:
    """Loại mã có QUÁ ÍT phiên lịch sử — không đủ để tính feature rolling window dài
    (VD MA50) một cách có ý nghĩa, tránh toàn NaN bị loại/impute rồi gây nhiễu cho model."""
    counts = df.groupby("symbol")["date_key"].transform("count")
    before_symbols = df["symbol"].nunique()
    out = df[counts >= min_days].copy()
    after_symbols = out["symbol"].nunique()
    if after_symbols < before_symbols:
        log.info(f"Loại {before_symbols - after_symbols} mã có < {min_days} phiên lịch sử")
    return out
