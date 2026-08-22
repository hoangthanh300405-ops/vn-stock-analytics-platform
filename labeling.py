"""
labeling.py - Tạo nhãn 3 lớp (down/flat/up) cho xu hướng giá NGÀY KẾ TIẾP.

Chống look-ahead bias: nhãn của ngày t dùng giá đóng cửa ngày t+1 (shift(-1) THEO TỪNG
MÃ) — đây là NƠI DUY NHẤT trong toàn pipeline có shift(-N)/nhìn về tương lai. Mọi feature
ở features.py chỉ nhìn lùi, nên không có rủi ro feature "biết trước" thông tin dùng để
tạo nhãn.
"""
import logging

import numpy as np
import pandas as pd

from . import config

log = logging.getLogger(__name__)


def add_next_day_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date_key"]).copy()

    next_close = df.groupby("symbol")["close_price"].shift(-1)

    df["next_close_price"] = next_close
    df["next_return_pct"] = (next_close - df["close_price"]) / df["close_price"] * 100

    conditions = [
        df["next_return_pct"] > config.LABEL_EPSILON_PCT,
        df["next_return_pct"] < -config.LABEL_EPSILON_PCT,
    ]
    choices = ["up", "down"]
    df["label"] = np.select(conditions, choices, default="flat")
    df["label_id"] = df["label"].map(config.CLASS_LABEL_MAP)

    # Dòng cuối cùng của mỗi mã (chưa có ngày kế tiếp) -> không có nhãn thật, phải loại
    no_next_day = df["next_close_price"].isna()
    df.loc[no_next_day, "label"] = np.nan
    df.loc[no_next_day, "label_id"] = np.nan

    # Fix REVIEW_FINDINGS #6: tính reference_yesterday_label_id (nhãn của PHIÊN LIỀN
    # TRƯỚC thật, theo lịch giao dịch) NGAY TẠI ĐÂY — trên dữ liệu còn LIÊN TỤC, trước
    # khi drop_limit_target_sessions()/dropna feature ở các bước sau làm dữ liệu bị
    # "đứt đoạn". Nếu tính baseline bằng .shift(1) SAU khi đã lọc bớt dòng, "hôm qua"
    # có thể thực ra là vài phiên trước đó chứ không phải phiên liền trước thật.
    df["reference_yesterday_label_id"] = df.groupby("symbol")["label_id"].shift(1)

    return df


def drop_limit_target_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Loại các dòng mà NGÀY KẾ TIẾP (ngày mang thông tin cho nhãn) đóng cửa sát trần/sàn
    — target bị "kẹp" bởi biên độ dao động chứ không phản ánh đúng cung-cầu thật, coi là
    nhiễu cho bài toán phân loại xu hướng (xem risks.mất cân bằng lớp và nhiễu trong spec).
    Chỉ áp dụng cho NHÃN — các phiên trần/sàn vẫn được GIỮ LẠI làm feature của ngày sau đó
    (pct_dist_to_ceiling/floor ở features.py)."""
    df = df.copy()
    next_ceiling = df.groupby("symbol")["ceiling_price"].shift(-1)
    next_floor = df.groupby("symbol")["floor_price"].shift(-1)

    is_next_limit_up = (
        (df["next_close_price"] - next_ceiling).abs() / next_ceiling.replace(0, np.nan) * 100
        <= config.LIMIT_BAND_TOLERANCE_PCT
    )
    is_next_limit_down = (
        (df["next_close_price"] - next_floor).abs() / next_floor.replace(0, np.nan) * 100
        <= config.LIMIT_BAND_TOLERANCE_PCT
    )
    is_next_limit = is_next_limit_up.fillna(False) | is_next_limit_down.fillna(False)

    before = len(df)
    df = df[~is_next_limit].copy()
    log.info(f"Loại {before - len(df)} dòng có NGÀY KẾ TIẾP đóng cửa sát trần/sàn (target bị kẹp biên độ)")
    return df


def finalize_labeled_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = add_next_day_label(df)
    df = drop_limit_target_sessions(df)
    df = df.dropna(subset=["label_id"]).copy()
    df["label_id"] = df["label_id"].astype(int)

    dist = df["label"].value_counts(normalize=True).round(3)
    log.info(f"Phân bố nhãn: {dist.to_dict()}")
    return df
