"""
backtest.py - Backtest chiến lược long-only dựa trên P(up) từ model, so với buy-and-hold.

LƯU Ý: đây KHÔNG phải backtest giao dịch thật — bỏ qua slippage, độ sâu sổ lệnh, khả năng
khớp lệnh đúng giá đóng cửa, thuế... Mục đích là MINH HOẠ cho phỏng vấn (constraints trong
spec: "mục đích chính là phỏng vấn, không phải trading thật").
"""
import logging

import numpy as np
import pandas as pd

from . import config

log = logging.getLogger(__name__)


def _max_drawdown_pct(cum_returns: pd.Series):
    if cum_returns.empty:
        return None
    running_max = cum_returns.cummax()
    drawdown = cum_returns / running_max - 1
    return float(drawdown.min() * 100)


def _sharpe_ratio(daily_returns: pd.Series, periods_per_year: int = 252) -> float:
    if daily_returns.empty or daily_returns.std() == 0:
        return 0.0
    return float((daily_returns.mean() / daily_returns.std()) * np.sqrt(periods_per_year))


def backtest_long_strategy(
    val_df: pd.DataFrame,
    proba_up: np.ndarray,
    buy_hold_universe_df: pd.DataFrame,
    threshold: float = config.BACKTEST_LONG_PROB_THRESHOLD,
    trading_cost_pct: float = config.TRADING_COST_PCT,
) -> dict:
    """Chiến lược: mỗi ngày t, với MỖI mã có P(up) > threshold -> giả định long 1 phiên
    (mua đóng cửa t, bán đóng cửa t+1). Return dùng next_return_pct đã có sẵn từ
    labeling.py (không tính lại). Danh mục backtest = trung bình cộng (equal-weight) return
    của các mã được chọn trong ngày đó.

    Fix REVIEW_FINDINGS #5: buy_hold_universe_df PHẢI là dữ liệu giá CHƯA qua
    drop_limit_target_sessions() (xem labeling.py) — val_df (dùng để chọn lệnh long) thì
    ĐÃ bị lọc bớt các phiên có ngày kế tiếp trần/sàn, nên nếu dùng val_df để tính luôn cả
    benchmark buy-and-hold, benchmark sẽ bị thiếu đúng những phiên biến động mạnh nhất
    (không phải universe thị trường thật) -> so sánh Sharpe/return không công bằng. Truyền
    2 nguồn dữ liệu tách biệt: val_df (đã lọc, dùng để CHỌN lệnh) và buy_hold_universe_df
    (KHÔNG lọc, chỉ dùng để tính benchmark) — xem cách gọi ở train_price_trend_model.py.
    """
    df = val_df.copy()
    df["proba_up"] = proba_up
    df["take_position"] = df["proba_up"] > threshold

    daily_strategy = (
        df[df["take_position"]].groupby("date_key")["next_return_pct"].mean()
    )
    daily_strategy = (daily_strategy - trading_cost_pct) / 100  # trừ phí, đổi sang tỉ lệ thập phân

    # Lịch giao dịch dùng để reindex lấy từ buy_hold_universe_df (KHÔNG lọc) — đây mới là
    # "toàn bộ phiên giao dịch thật" trong khung ngày của fold, đúng nghĩa 1 buy-and-hold
    # benchmark, thay vì lịch đã bị thu hẹp bởi bộ lọc nhãn.
    all_dates = np.sort(buy_hold_universe_df["date_key"].unique())
    daily_returns = daily_strategy.reindex(all_dates, fill_value=0.0)
    cum_returns = (1 + daily_returns).cumprod()

    buy_hold_daily = (
        buy_hold_universe_df.groupby("date_key")["next_return_pct"].mean().reindex(all_dates).fillna(0) / 100
    )
    buy_hold_cum = (1 + buy_hold_daily).cumprod()

    return {
        "n_trading_days": int(len(all_dates)),
        "n_days_with_position": int((daily_returns != 0).sum()),
        "strategy_total_return_pct": float((cum_returns.iloc[-1] - 1) * 100) if len(cum_returns) else None,
        "strategy_sharpe": _sharpe_ratio(daily_returns),
        "strategy_max_drawdown_pct": _max_drawdown_pct(cum_returns),
        "buy_hold_total_return_pct": float((buy_hold_cum.iloc[-1] - 1) * 100) if len(buy_hold_cum) else None,
        "buy_hold_sharpe": _sharpe_ratio(buy_hold_daily),
        "buy_hold_max_drawdown_pct": _max_drawdown_pct(buy_hold_cum),
        "threshold_used": threshold,
        "trading_cost_pct_per_trade": trading_cost_pct,
    }
