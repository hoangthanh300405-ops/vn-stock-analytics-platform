"""
splitting.py - Walk-forward validation theo MỐC THỜI GIAN TOÀN CỤC (không phải theo
từng mã riêng lẻ) — đảm bảo mọi dòng trong 1 fold train luôn có date_key sớm hơn (trừ
embargo) mọi dòng trong fold validation tương ứng, tránh leakage giữa các mã đang cùng
được huấn luyện ở cùng giai đoạn thị trường.

EMBARGO_DAYS: nhãn của ngày CUỐI train được tính từ giá NGÀY KẾ TIẾP (xem labeling.py)
— nếu ngày kế tiếp đó rơi vào đầu validation, một phần thông tin validation đã "rò" vào
nhãn train qua next_close_price. Lùi train_end lại vài phiên loại trừ rủi ro này.
"""
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from . import config

log = logging.getLogger(__name__)


@dataclass
class WalkForwardFold:
    fold_id: int
    train_end_date: pd.Timestamp
    val_start_date: pd.Timestamp
    val_end_date: pd.Timestamp


def make_walkforward_folds(
    all_dates: pd.Series,
    n_splits: int = config.N_WALKFORWARD_SPLITS,
    embargo_days: int = config.EMBARGO_DAYS,
) -> list[WalkForwardFold]:
    """Chia các mốc ngày giao dịch DUY NHẤT thành n_splits fold kiểu expanding window:
    50% đầu tiên luôn nằm trong train của fold đầu; mỗi fold kế tiếp mở rộng train thêm
    1 block validation của fold trước."""
    unique_dates = np.sort(pd.Series(all_dates).unique())
    if len(unique_dates) < (n_splits + 1) * 5:
        raise ValueError(
            f"Chỉ có {len(unique_dates)} ngày giao dịch duy nhất — quá ít để chia "
            f"{n_splits} fold walk-forward có ý nghĩa. Giảm N_WALKFORWARD_SPLITS hoặc "
            "chờ có thêm dữ liệu."
        )

    initial_train_frac = 0.5
    initial_train_end_idx = int(len(unique_dates) * initial_train_frac)
    val_pool_dates = unique_dates[initial_train_end_idx:]
    val_blocks = [b for b in np.array_split(val_pool_dates, n_splits) if len(b) > 0]

    folds = []
    for i, block in enumerate(val_blocks):
        val_start_date = pd.Timestamp(block[0])
        val_end_date = pd.Timestamp(block[-1])

        dates_before_val = unique_dates[unique_dates < np.datetime64(val_start_date)]
        train_end_idx = max(0, len(dates_before_val) - embargo_days)
        if train_end_idx == 0:
            continue
        train_end_date = pd.Timestamp(dates_before_val[train_end_idx - 1])

        folds.append(WalkForwardFold(
            fold_id=i, train_end_date=train_end_date,
            val_start_date=val_start_date, val_end_date=val_end_date,
        ))

    log.info(f"Tạo {len(folds)} walk-forward fold, embargo={embargo_days} phiên")
    return folds


def split_fold(df: pd.DataFrame, fold: WalkForwardFold) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = df[df["date_key"] <= fold.train_end_date]
    val = df[(df["date_key"] >= fold.val_start_date) & (df["date_key"] <= fold.val_end_date)]
    return train, val
