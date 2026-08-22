"""
train_price_trend_model.py
----------------------------
Entry point cho pipeline dự đoán xu hướng giá ngày kế tiếp (down/flat/up):
  1. Đọc dữ liệu từ vnstock.duckdb (đã qua dbt) — fct_price_daily + dim_stock
  2. Feature engineering (MA/EMA, RSI, MACD, Bollinger, volume z-score, momentum ngành, lag)
  3. Tạo nhãn 3 lớp, loại phiên trần/sàn khỏi nhãn
  4. Walk-forward validation (tuning_fold riêng, holdout_fold thật) + Optuna tuning LightGBM
  5. SHAP explainability + calibration curve + backtest minh hoạ TRÊN HOLDOUT FOLD (blind)
  6. Lưu artifact (model .txt, feature_list.json, model_metadata.json, shap_summary.png,
     calibration_curve_up.png, backtest_report.json) — publish qua GitHub Release

── Sửa theo REVIEW_FINDINGS ─────────────────────────────────────────────────
#1: SHAP/backtest/số liệu "headline" giờ lấy từ holdout_fold (blind thật) thay vì
    tuning_fold như bản trước — xem model_metadata["holdout_metrics"] vs
    model_metadata["fold_results"] (fold nào is_used_for_hyperparameter_tuning=True
    KHÔNG phải số liệu blind, chỉ để tham khảo).
#3: categorical_vocab tính 1 lần từ TOÀN BỘ dữ liệu, lưu vào feature_list.json để
    Streamlit inference dùng lại y hệt — kèm 1 bước self-check round-trip
    save->load->predict ngay sau khi train để phát hiện sớm nếu encoding lệch.
#4: thêm calibration_curve_up.png + brier_score trong metadata.
#5: backtest buy-and-hold benchmark giờ tính trên raw_next_return (KHÔNG lọc phiên
    trần/sàn) thay vì trên holdout_val đã lọc — xem buy_hold_universe bên dưới.
#11: thêm maximum_foreign_percentage làm feature tĩnh (xem data_loading.py giải
    thích vì sao KHÔNG thêm foreigner_percentage/rating/target_price — rủi ro
    point-in-time leakage vì dim_stock không historize).

Dùng lại ĐÚNG biến môi trường DUCKDB_FILE_PATH của pipeline ETL hiện có.
Chạy: python train_price_trend_model.py
Yêu cầu: pip install -r requirements_ml.txt
"""
import json
import logging

import numpy as np

from ml_price_trend import config
from ml_price_trend.backtest import backtest_long_strategy
from ml_price_trend.data_loading import filter_sparse_symbols, load_price_panel
from ml_price_trend.explain import compute_shap_summary, plot_calibration_curve
from ml_price_trend.features import build_feature_set
from ml_price_trend.labeling import add_next_day_label, finalize_labeled_dataset
from ml_price_trend.splitting import make_walkforward_folds, split_fold
from ml_price_trend.train import (
    apply_categorical_vocab,
    build_categorical_vocab,
    run_walkforward_training,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _round_trip_sanity_check(model_path: str, feature_cols: list[str], vocab: dict, sample_row) -> None:
    """Fix #3: self-check nhẹ ngay sau khi save model — train xong, load lại model
    TỪ FILE (giống hệt cách Streamlit sẽ load) rồi predict lại đúng 1 dòng đã biết
    trước xác suất lúc train, assert khớp. Không thay được cho 1 bộ test đầy đủ
    (vẫn còn REVIEW_FINDINGS #8 cần làm), nhưng bắt được NGAY LẬP TỨC nếu round-trip
    save->load->predict với categorical feature bị lệch, thay vì phát hiện âm thầm
    ở production."""
    import lightgbm as lgb

    reloaded = lgb.Booster(model_file=model_path)
    row = apply_categorical_vocab(sample_row.copy(), vocab)
    before = sample_row["_sanity_proba"].iloc[0]
    after = reloaded.predict(row[feature_cols])[0]
    if not np.allclose(before, after, atol=1e-6):
        raise RuntimeError(
            "Round-trip sanity check THẤT BẠI: xác suất dự đoán trước và sau khi "
            "save→load model KHÔNG khớp (chênh lệch: "
            f"{np.abs(np.array(before) - np.array(after))}). Rất có thể do categorical "
            "encoding (exchange/sector_name) không ổn định giữa lúc train và lúc load lại "
            "model — xem REVIEW_FINDINGS #3. KHÔNG publish model này."
        )
    log.info("Round-trip sanity check (save -> load -> predict) OK — categorical encoding ổn định.")


def main():
    log.info("── Bước 1/6: Đọc dữ liệu từ DuckDB ──")
    raw = load_price_panel()
    raw = filter_sparse_symbols(raw)

    log.info("── Bước 2/6: Feature engineering ──")
    featured, feature_cols = build_feature_set(raw)
    log.info(f"{len(feature_cols)} feature số: {feature_cols}")
    # Fix #11: maximum_foreign_percentage thêm làm feature số TĨNH (không qua dropna
    # chung với feature_cols kỹ thuật — cột này KHÔNG liên quan tới window rolling, và
    # LightGBM xử lý NaN gốc (mã chưa từng chạy extract_company_profile.py) tốt hơn là
    # bị ép dropna cả dòng). exchange/sector_name vẫn xử lý riêng làm categorical.
    static_numeric_cols = [c for c in ["maximum_foreign_percentage"] if c in featured.columns]
    all_feature_cols = feature_cols + static_numeric_cols + config.CATEGORICAL_FEATURES

    log.info("── Bước 3/6: Tạo nhãn 3 lớp + loại phiên trần/sàn ──")
    labeled = finalize_labeled_dataset(featured)
    before = len(labeled)
    labeled = labeled.dropna(subset=feature_cols)  # loại dòng thiếu feature số (đầu lịch sử mỗi mã)
    log.info(f"Loại {before - len(labeled)} dòng thiếu feature (đầu lịch sử mỗi mã, chưa đủ window)")

    if labeled.empty:
        raise RuntimeError("Dataset rỗng sau khi lọc — kiểm tra lại dữ liệu nguồn/tham số config.")

    # Fix #3: vocab categorical tính 1 LẦN trên TOÀN BỘ dataset đã lọc — dùng lại y
    # hệt ở mọi fold (tuning/holdout) và lưu ra feature_list.json cho Streamlit.
    categorical_vocab = build_categorical_vocab(labeled)
    log.info(f"Categorical vocab: {categorical_vocab}")

    log.info("── Bước 4/6: Walk-forward training (tuning_fold riêng, holdout_fold blind) + Optuna ──")
    folds = make_walkforward_folds(labeled["date_key"])
    result = run_walkforward_training(labeled, all_feature_cols, folds, categorical_vocab)
    booster = result["final_booster"]
    if booster is None:
        raise RuntimeError("Không tạo được model cuối — kiểm tra log walk-forward ở trên.")
    if result["holdout_metrics"] is None:
        raise RuntimeError("Không có holdout_metrics — holdout_fold rỗng, kiểm tra lại dữ liệu/fold.")

    log.info("── Bước 5/6: SHAP + calibration + backtest TRÊN HOLDOUT FOLD (blind, fix #1) ──")
    holdout_fold = result["holdout_fold"]
    _, holdout_val = split_fold(labeled, holdout_fold)
    holdout_val_cat = apply_categorical_vocab(holdout_val, categorical_vocab)
    X_holdout = holdout_val_cat[all_feature_cols]

    shap_importance = compute_shap_summary(booster, X_holdout, config.SHAP_SUMMARY_PLOT_PATH, config.CLASS_NAMES)
    proba = booster.predict(X_holdout)
    proba_up = proba[:, config.CLASS_LABEL_MAP["up"]]

    # Fix #5: benchmark buy-and-hold PHẢI dùng dữ liệu giá KHÔNG lọc phiên trần/sàn —
    # tính lại next_return_pct trên `featured` (trước finalize_labeled_dataset()), giới
    # hạn đúng khung ngày của holdout_fold. Đây là universe thị trường THẬT, khác với
    # holdout_val (đã bị lọc, chỉ dùng để chọn lệnh long ở trên).
    raw_next_return = add_next_day_label(featured)
    buy_hold_universe = raw_next_return[
        (raw_next_return["date_key"] >= holdout_fold.val_start_date)
        & (raw_next_return["date_key"] <= holdout_fold.val_end_date)
    ].dropna(subset=["next_return_pct"])

    backtest_result = backtest_long_strategy(holdout_val, proba_up, buy_hold_universe_df=buy_hold_universe)

    calib_up = result["holdout_metrics"]["calibration_curve"].get("up")
    if calib_up:
        plot_calibration_curve(calib_up["prob_true"], calib_up["prob_pred"], "up", config.CALIBRATION_PLOT_PATH)

    log.info("── Bước 6/6: Lưu artifact + round-trip sanity check ──")
    booster.save_model(config.MODEL_OUTPUT_PATH)

    sample_row = X_holdout.iloc[[0]].copy()
    sample_row["_sanity_proba"] = [booster.predict(sample_row[all_feature_cols])[0].tolist()]
    _round_trip_sanity_check(config.MODEL_OUTPUT_PATH, all_feature_cols, categorical_vocab, sample_row)

    with open(config.FEATURE_LIST_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "feature_cols": all_feature_cols,
                "categorical_features": config.CATEGORICAL_FEATURES,
                "categorical_vocab": categorical_vocab,  # fix #3 — Streamlit phải dùng lại đúng vocab này
            },
            f, ensure_ascii=False, indent=2,
        )

    metadata = {
        "best_params": result["best_params"],
        "best_num_boost_round": result["best_num_boost_round"],
        "final_train_end_date": result["final_train_end_date"],
        # fold_results chứa CẢ tuning_fold lẫn holdout_fold — LUÔN kiểm tra cờ
        # is_used_for_hyperparameter_tuning trước khi trích dẫn 1 con số bất kỳ (fix #1).
        "fold_results": result["fold_results"],
        "holdout_fold_id": result["holdout_fold"].fold_id,
        "holdout_metrics": result["holdout_metrics"],  # <- con số NÊN dùng khi báo cáo hiệu năng thật
        "top_shap_features_class_up": shap_importance.head(15).to_dict(),
        "backtest": backtest_result,
        "label_epsilon_pct": config.LABEL_EPSILON_PCT,
        "n_rows_trained": len(labeled),
        "n_symbols": int(labeled["symbol"].nunique()),
    }
    with open(config.METADATA_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2, default=str)

    with open(config.BACKTEST_REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(backtest_result, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"Đã lưu model: {config.MODEL_OUTPUT_PATH}")
    log.info(
        f"[BLIND — holdout fold {result['holdout_fold'].fold_id}] macro F1="
        f"{result['holdout_metrics']['macro_f1']:.4f}, baseline="
        f"{result['holdout_metrics']['baseline_same_as_yesterday_macro_f1']}"
    )
    log.info(f"Backtest (holdout): {json.dumps(backtest_result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
