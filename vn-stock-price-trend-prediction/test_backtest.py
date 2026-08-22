"""
tests/test_backtest.py - REVIEW_FINDINGS #5 và #8: kiểm tra backtest_long_strategy()
dùng ĐÚNG universe không lọc cho benchmark buy-and-hold, và các công thức Sharpe/
drawdown cơ bản.
"""
import numpy as np
import pandas as pd

from ml_price_trend.backtest import _max_drawdown_pct, _sharpe_ratio, backtest_long_strategy


def test_sharpe_ratio_zero_when_no_variance():
    daily_returns = pd.Series([0.0, 0.0, 0.0])
    assert _sharpe_ratio(daily_returns) == 0.0


def test_max_drawdown_detects_known_drop():
    # Tăng dần tới 1.2, rơi về 0.9 (drawdown = 0.9/1.2 - 1 = -25%), rồi hồi lại
    cum_returns = pd.Series([1.0, 1.1, 1.2, 0.9, 1.0])
    dd = _max_drawdown_pct(cum_returns)
    assert dd is not None
    assert abs(dd - (-25.0)) < 1e-6


def test_backtest_buy_hold_uses_unfiltered_universe_not_selected_positions():
    """Fix #5: buy_hold_universe_df PHẢI là nguồn benchmark — nếu val_df (đã lọc, dùng
    để chọn lệnh) chỉ có 1 mã nhưng buy_hold_universe_df có thêm mã khác (đại diện cho
    phiên đã bị lọc khỏi val_df), buy-hold phải phản ánh ĐẦY ĐỦ universe, không phải chỉ
    những gì còn sót lại trong val_df."""
    dates = pd.bdate_range("2024-01-02", periods=3)

    # val_df: chỉ có mã AAA, dùng để chọn lệnh long (proba_up cao ngày đầu)
    val_df = pd.DataFrame({
        "symbol": ["AAA", "AAA", "AAA"],
        "date_key": dates,
        "next_return_pct": [5.0, -2.0, 1.0],
    })
    proba_up = np.array([0.9, 0.1, 0.1])  # chỉ vào lệnh ngày đầu

    # buy_hold_universe: đầy đủ cả AAA và BBB (BBB giả lập mã đã bị lọc khỏi val_df ở
    # bước drop_limit_target_sessions, nhưng vẫn phải tính vào benchmark thị trường thật)
    buy_hold_universe = pd.DataFrame({
        "symbol": ["AAA", "AAA", "AAA", "BBB", "BBB", "BBB"],
        "date_key": list(dates) * 2,
        "next_return_pct": [5.0, -2.0, 1.0, 7.0, 7.0, 7.0],  # BBB tăng trần liên tục
    })

    result = backtest_long_strategy(val_df, proba_up, buy_hold_universe_df=buy_hold_universe, trading_cost_pct=0.0)

    # buy_hold phải > 0 nhiều hơn hẳn nếu chỉ tính từ AAA (5-2+1=4%) vì có thêm BBB (7%/ngày)
    # trung bình mỗi ngày = (AAA + BBB)/2 -> ngày 1: (5+7)/2=6%, ngày2: (-2+7)/2=2.5%, ngày3: (1+7)/2=4%
    assert result["buy_hold_total_return_pct"] > 4.0  # cao hơn hẳn nếu chỉ dùng riêng AAA
    assert result["n_trading_days"] == 3


def test_backtest_no_position_days_have_zero_strategy_return():
    dates = pd.bdate_range("2024-01-02", periods=2)
    val_df = pd.DataFrame({
        "symbol": ["AAA", "AAA"],
        "date_key": dates,
        "next_return_pct": [10.0, -10.0],
    })
    proba_up = np.array([0.1, 0.1])  # không bao giờ vượt threshold -> không vào lệnh ngày nào
    buy_hold_universe = val_df.copy()

    result = backtest_long_strategy(val_df, proba_up, buy_hold_universe_df=buy_hold_universe)
    assert result["n_days_with_position"] == 0
    assert result["strategy_total_return_pct"] == 0.0
