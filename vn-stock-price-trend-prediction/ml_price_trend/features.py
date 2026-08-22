"""
features.py - Feature engineering: MA/EMA crossover, RSI14, MACD, Bollinger Bands,
volume z-score, momentum tương đối theo ngành, lag features.

Nguyên tắc chống look-ahead bias: MỌI rolling/window đều tính TRONG PHẠM VI TỪNG MÃ
(groupby symbol) — không bao giờ để rolling window tràn qua ranh giới 2 mã khác nhau
(lỗi cross-contamination thường gặp nhất khi làm feature cho dữ liệu dạng panel). Toàn
bộ hàm ở đây chỉ dùng .shift(dương)/.rolling() nhìn LÙI — không có shift âm nào (shift
âm/nhìn tới tương lai chỉ xảy ra ở labeling.py, tách biệt hẳn khỏi feature).
"""
import logging

import numpy as np
import pandas as pd
import ta
from joblib import Parallel, delayed

from . import config

log = logging.getLogger(__name__)


def _add_indicators_for_one_symbol(g: pd.DataFrame) -> pd.DataFrame:
    """g: dữ liệu ĐÃ sort theo date_key của ĐÚNG 1 mã. Trả về thêm các cột feature."""
    g = g.sort_values("date_key").copy()
    close = g["close_price"]
    volume = g["volume"]

    # MA / EMA + tín hiệu crossover (giá so với MA, MA ngắn so với MA dài)
    for w in config.MA_WINDOWS:
        g[f"ma_{w}"] = close.rolling(w, min_periods=w).mean()
        g[f"close_over_ma_{w}"] = close / g[f"ma_{w}"] - 1
    for w in config.EMA_WINDOWS:
        g[f"ema_{w}"] = close.ewm(span=w, adjust=False, min_periods=w).mean()
    g["ma_cross_fast_slow"] = g[f"ma_{config.MA_WINDOWS[0]}"] / g[f"ma_{config.MA_WINDOWS[-1]}"] - 1

    # RSI14
    g["rsi_14"] = ta.momentum.RSIIndicator(close=close, window=config.RSI_WINDOW).rsi()

    # MACD
    macd = ta.trend.MACD(
        close=close,
        window_fast=config.MACD_FAST,
        window_slow=config.MACD_SLOW,
        window_sign=config.MACD_SIGNAL,
    )
    g["macd"] = macd.macd()
    g["macd_signal"] = macd.macd_signal()
    g["macd_hist"] = macd.macd_diff()

    # Bollinger Bands
    bb = ta.volatility.BollingerBands(
        close=close, window=config.BOLLINGER_WINDOW, window_dev=config.BOLLINGER_STD
    )
    g["bb_percent_b"] = bb.bollinger_pband()
    g["bb_bandwidth"] = bb.bollinger_wband()

    # Volume z-score (so với trung bình/độ lệch chuẩn 20 phiên gần nhất CỦA CHÍNH MÃ ĐÓ)
    vol_mean = volume.rolling(config.VOLUME_ZSCORE_WINDOW, min_periods=config.VOLUME_ZSCORE_WINDOW).mean()
    vol_std = volume.rolling(config.VOLUME_ZSCORE_WINDOW, min_periods=config.VOLUME_ZSCORE_WINDOW).std()
    g["volume_zscore"] = (volume - vol_mean) / vol_std.replace(0, np.nan)

    # Lag returns: return_lag_1 = return CỦA CHÍNH PHIÊN HÔM ĐÓ (đã biết tại cuối phiên t,
    # không phải tương lai) — return_lag_p = return của phiên t-(p-1)
    daily_return = close.pct_change()
    for p in config.LAG_RETURN_PERIODS:
        g[f"return_lag_{p}"] = daily_return.shift(p - 1)

    # Khoảng cách tới trần/sàn của CHÍNH PHIÊN ĐÓ — feature phản ánh áp lực cung/cầu ĐÃ
    # QUAN SÁT được (khác với việc dùng trần/sàn của ngày MAI làm nhãn, xem labeling.py)
    band_width = (g["ceiling_price"] - g["floor_price"]).replace(0, np.nan)
    g["pct_dist_to_ceiling"] = (g["ceiling_price"] - close) / band_width
    g["pct_dist_to_floor"] = (close - g["floor_price"]) / band_width

    return g


def add_technical_features(df: pd.DataFrame, n_jobs: int = config.FEATURE_ENGINEERING_N_JOBS) -> pd.DataFrame:
    """Áp dụng _add_indicators_for_one_symbol cho từng mã (groupby symbol).

    Dùng vòng lặp thủ công qua df.groupby(...) (thay vì .groupby().apply(...)) vì từ
    pandas 3.0, DataFrameGroupBy.apply() mặc định loại bỏ hẳn cột dùng để group (ở đây
    là "symbol") khỏi sub-dataframe truyền vào hàm — làm mất cột "symbol" trong kết quả
    cuối cùng mà không báo lỗi rõ ràng. Duyệt trực tiếp qua groupby giữ nguyên cột này.

    Fix REVIEW_FINDINGS #10: mỗi mã tính ĐỘC LẬP hoàn toàn (không có state dùng chung
    giữa các mã) -> song song hoá bằng joblib để tận dụng nhiều core, quan trọng với quy
    mô ~1700+ mã thật của dự án (risks.thời gian train chưa được đo trong spec — đây là
    điểm nghẽn dễ đo/dễ sửa nhất, ưu tiên trước khi động vào Optuna). n_jobs=-1 dùng hết
    core sẵn có; đặt n_jobs=1 (config.FEATURE_ENGINEERING_N_JOBS) nếu môi trường chạy
    không hỗ trợ multiprocessing (một số sandbox/container hạn chế) — kết quả giống hệt
    chạy tuần tự (chỉ khác thời gian), không đánh đổi độ chính xác."""
    log.info(f"Đang tính technical indicators theo từng mã (n_jobs={n_jobs})...")
    groups = [g for _, g in df.groupby("symbol", sort=False)]
    parts = Parallel(n_jobs=n_jobs)(delayed(_add_indicators_for_one_symbol)(g) for g in groups)
    out = pd.concat(parts, ignore_index=True)
    return out


def add_sector_relative_momentum(df: pd.DataFrame, window: int = config.SECTOR_MOMENTUM_WINDOW) -> pd.DataFrame:
    """Momentum tương đối theo ngành: return N ngày của mã trừ đi return N ngày TRUNG BÌNH
    của toàn ngành cùng ngày đó, LOẠI TRỪ chính mã đang xét khỏi trung bình ngành (tránh 1
    mã vốn hoá lớn tự "kéo" trung bình ngành gần về chính nó)."""
    df = df.copy()
    return_nd = df.groupby("symbol")["close_price"].pct_change(window)
    df[f"_return_{window}d_raw"] = return_nd

    sector_sum = df.groupby(["sector_name", "date_key"])[f"_return_{window}d_raw"].transform("sum")
    sector_count = df.groupby(["sector_name", "date_key"])[f"_return_{window}d_raw"].transform("count")
    sector_mean_excl_self = (sector_sum - df[f"_return_{window}d_raw"]) / (sector_count - 1).replace(0, np.nan)

    df["sector_relative_momentum"] = df[f"_return_{window}d_raw"] - sector_mean_excl_self
    df = df.drop(columns=[f"_return_{window}d_raw"])
    return df


def build_feature_set(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Pipeline đầy đủ: technical indicators + sector momentum. Trả về (df đã thêm cột,
    danh sách tên cột feature số) để dùng lại Y HỆT lúc train và lúc inference."""
    df = add_technical_features(df)
    df = add_sector_relative_momentum(df)

    feature_cols = [
        c for c in df.columns
        if c.startswith((
            "ma_", "close_over_ma_", "ema_", "ma_cross", "rsi_", "macd",
            "bb_", "volume_zscore", "return_lag_", "pct_dist_to_", "sector_relative_momentum",
        ))
    ]
    return df, feature_cols
