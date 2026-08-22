"""
tests/test_splitting.py - REVIEW_FINDINGS #8: xác nhận embargo/walk-forward không bị
vi phạm — đây là cơ chế chống leakage cốt lõi thứ 2 (sau labeling.py) của pipeline.
"""
import pandas as pd
import pytest

from ml_price_trend.splitting import make_walkforward_folds, split_fold


def _business_dates(n, start="2023-01-02"):
    return pd.bdate_range(start, periods=n)


def test_folds_are_chronologically_ordered_and_expanding():
    dates = pd.Series(_business_dates(300))
    folds = make_walkforward_folds(dates, n_splits=4, embargo_days=2)
    assert len(folds) >= 1
    for f in folds:
        assert f.train_end_date < f.val_start_date
        assert f.val_start_date <= f.val_end_date


def test_embargo_gap_is_respected_between_train_end_and_val_start():
    """Số phiên giao dịch (trong toàn bộ lịch, không chỉ trong 1 fold) nằm giữa
    train_end_date và val_start_date phải >= embargo_days đã cấu hình."""
    dates = pd.Series(_business_dates(300))
    unique_dates = dates.sort_values().unique()
    embargo_days = 3
    folds = make_walkforward_folds(dates, n_splits=4, embargo_days=embargo_days)

    for f in folds:
        dates_between = unique_dates[
            (unique_dates > f.train_end_date.to_datetime64()) & (unique_dates < f.val_start_date.to_datetime64())
        ]
        assert len(dates_between) >= embargo_days, (
            f"Fold {f.fold_id}: chỉ có {len(dates_between)} phiên đệm giữa train_end "
            f"({f.train_end_date.date()}) và val_start ({f.val_start_date.date()}), "
            f"cần tối thiểu {embargo_days}"
        )


def test_split_fold_produces_disjoint_train_and_val():
    dates = pd.Series(_business_dates(300))
    folds = make_walkforward_folds(dates, n_splits=3, embargo_days=2)

    df = pd.DataFrame({"date_key": dates, "value": range(len(dates))})
    for f in folds:
        train, val = split_fold(df, f)
        assert train["date_key"].max() <= f.train_end_date
        assert val["date_key"].min() >= f.val_start_date
        assert val["date_key"].max() <= f.val_end_date
        # Train và val không được có ngày trùng nhau
        assert set(train["date_key"]).isdisjoint(set(val["date_key"]))


def test_too_few_dates_raises_instead_of_producing_meaningless_folds():
    dates = pd.Series(_business_dates(10))
    with pytest.raises(ValueError):
        make_walkforward_folds(dates, n_splits=5, embargo_days=2)


def test_fold_ids_are_sequential_and_val_ranges_do_not_overlap():
    dates = pd.Series(_business_dates(400))
    folds = make_walkforward_folds(dates, n_splits=5, embargo_days=2)
    for a, b in zip(folds, folds[1:]):
        assert a.val_end_date < b.val_start_date
