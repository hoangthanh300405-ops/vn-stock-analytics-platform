"""
tests/test_labeling.py - REVIEW_FINDINGS #8: kiểm tra đúng phần NHẠY CẢM NHẤT với
leakage của toàn pipeline (labeling.py) bằng dữ liệu nhỏ, tự tay tính trước kết quả
đúng, thay vì chỉ tin vào code.
"""
import numpy as np
import pandas as pd
import pytest

from ml_price_trend import config
from ml_price_trend.labeling import (
    add_next_day_label,
    drop_limit_target_sessions,
    finalize_labeled_dataset,
)


def _toy_symbol_df(symbol="AAA", closes=None, ceilings=None, floors=None, dates=None):
    n = len(closes)
    dates = dates or pd.bdate_range("2024-01-02", periods=n)
    ceilings = ceilings or [1e9] * n  # mặc định trần/sàn rất xa, không kích hoạt lọc
    floors = floors or [0.0] * n
    return pd.DataFrame({
        "symbol": [symbol] * n,
        "date_key": pd.to_datetime(dates),
        "close_price": closes,
        "ceiling_price": ceilings,
        "floor_price": floors,
    })


def test_label_up_down_flat_thresholds():
    """Nhãn phải đúng theo epsilon: return > eps -> up, < -eps -> down, còn lại -> flat."""
    eps = config.LABEL_EPSILON_PCT
    # return lần lượt: +2% (up rõ ràng), -2% (down rõ ràng), +0.01% (flat, dưới eps)
    closes = [100, 102, 99.96, 99.97]
    df = _toy_symbol_df(closes=closes)
    out = add_next_day_label(df)

    assert out.loc[0, "label"] == "up"
    assert out.loc[1, "label"] == "down"
    assert out.loc[2, "label"] == "flat"
    assert pytest.approx(out.loc[0, "next_return_pct"], abs=1e-9) == 2.0


def test_last_row_per_symbol_has_no_label():
    """Dòng cuối cùng của mỗi mã không có ngày kế tiếp -> label phải là NaN, không được
    suy ra 1 giá trị mặc định nào (đây chính là chỗ dễ vô tình leak/bịa nhãn nhất)."""
    df = _toy_symbol_df(closes=[100, 101, 102])
    out = add_next_day_label(df)
    assert pd.isna(out.loc[2, "label"])
    assert pd.isna(out.loc[2, "label_id"])


def test_label_never_uses_future_beyond_t_plus_1():
    """Đổi giá trị close_price ở ngày t+2 trở đi không được làm thay đổi nhãn ở ngày t —
    nếu assertion này fail nghĩa là có leakage nhìn xa hơn 1 ngày."""
    df1 = _toy_symbol_df(closes=[100, 105, 200, 300])
    df2 = _toy_symbol_df(closes=[100, 105, 1, 2])  # đổi hẳn giá trị xa tương lai
    out1 = add_next_day_label(df1)
    out2 = add_next_day_label(df2)
    assert out1.loc[0, "label"] == out2.loc[0, "label"]
    assert out1.loc[0, "next_return_pct"] == out2.loc[0, "next_return_pct"]


def test_reference_yesterday_label_uses_true_previous_row_before_filtering():
    """Fix #6: reference_yesterday_label_id phải phản ánh đúng NHÃN của phiên liền
    trước, tính TRƯỚC khi bất kỳ dòng nào bị lọc bỏ."""
    df = _toy_symbol_df(closes=[100, 102, 99.9, 100.5, 105])
    out = add_next_day_label(df)
    # dòng index 1 (t=1): yesterday phải là label của dòng index 0
    assert out.loc[1, "reference_yesterday_label_id"] == out.loc[0, "label_id"]
    # dòng đầu tiên của mỗi mã không có "hôm qua"
    assert pd.isna(out.loc[0, "reference_yesterday_label_id"])


def test_reference_yesterday_label_survives_row_filtering():
    """Ngay cả sau khi drop_limit_target_sessions() xoá bớt dòng ở giữa, cột
    reference_yesterday_label_id của các dòng CÒN LẠI vẫn phải là nhãn của phiên liền
    trước THẬT (không bị dịch sang phiên xa hơn do dòng ở giữa bị xoá) — đây chính là
    lỗi baseline bị nhiễu đã ghi trong REVIEW_FINDINGS #6 trước khi sửa."""
    # Thiết kế: dòng index 2 (t=2) có ngày kế tiếp (t=3) đóng cửa sát trần -> sẽ bị
    # drop_limit_target_sessions() loại bỏ. Dòng t=3 phải vẫn giữ đúng
    # reference_yesterday_label_id = label của t=2 (không bị "nhảy" về t=1).
    # LƯU Ý: ceiling_price gắn ở ĐÚNG dòng t=3 (không phải t=2) — hàm kiểm tra
    # next_ceiling = ceiling_price.shift(-1), tức so next_close_price (đã là giá t=3)
    # với ceiling_price CỦA CHÍNH DÒNG t=3. Đặt close[3] TRÙNG KHỚP ceiling[3] (thay vì
    # chỉ "gần") để chắc chắn nằm trong LIMIT_BAND_TOLERANCE_PCT (0.1%).
    closes = [100, 102, 99, 105.93]  # t=2->t=3: đúng bằng trần HOSE (ref=99*1.07=105.93)
    ceilings = [1e9, 1e9, 1e9, 105.93]
    df = _toy_symbol_df(closes=closes, ceilings=ceilings)
    labeled = add_next_day_label(df)
    ref_yesterday_before_filter = labeled.loc[3, "reference_yesterday_label_id"]

    filtered = drop_limit_target_sessions(labeled)
    # dòng t=2 (index 2) đã bị loại vì ngày kế tiếp sát trần
    assert 2 not in filtered.index
    # nhưng dòng t=3 (index 3) vẫn còn, và giá trị reference_yesterday_label_id KHÔNG đổi
    assert 3 in filtered.index
    assert filtered.loc[3, "reference_yesterday_label_id"] == ref_yesterday_before_filter
    assert filtered.loc[3, "reference_yesterday_label_id"] == labeled.loc[2, "label_id"]


def test_drop_limit_target_sessions_only_checks_next_day():
    """Phiên trần/sàn của CHÍNH NGÀY ĐÓ (không phải ngày kế tiếp) không được làm dòng bị
    loại — chỉ ngày kế tiếp (ngày quyết định nhãn) mới cần kiểm tra."""
    # t=1 tự nó đóng cửa sát trần, nhưng ngày kế tiếp (t=2) thì bình thường -> KHÔNG bị loại
    closes = [100, 106.9, 108]
    ceilings = [1e9, 106.93, 1e9]
    df = _toy_symbol_df(closes=closes, ceilings=ceilings)
    labeled = add_next_day_label(df)
    filtered = drop_limit_target_sessions(labeled)
    assert 1 in filtered.index


def test_finalize_labeled_dataset_drops_nan_labels_and_casts_int():
    df = _toy_symbol_df(closes=[100, 101, 99, 100.4])
    out = finalize_labeled_dataset(df)
    assert out["label_id"].isna().sum() == 0
    assert out["label_id"].dtype == int or np.issubdtype(out["label_id"].dtype, np.integer)
    # dòng cuối cùng (không có ngày kế tiếp) phải bị loại khỏi kết quả cuối
    assert len(out) == 3
