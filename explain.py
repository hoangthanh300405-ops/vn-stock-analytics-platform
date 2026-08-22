"""
explain.py - SHAP explainability cho model LightGBM đa lớp. Mục đích chính (theo spec):
giải thích được cho người không chuyên (phỏng vấn), không phải để trading thật.
"""
import logging

import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap

log = logging.getLogger(__name__)


def compute_shap_summary(booster: lgb.Booster, X: pd.DataFrame, output_path: str, class_names: list[str]) -> pd.Series:
    """Tính SHAP values (TreeExplainer — nhanh, chính xác cho GBM) và lưu summary plot
    cho lớp 'up' (quan trọng nhất cho câu hỏi "có nên vào lệnh long không"). Trả về
    mean(|SHAP|) mỗi feature để ghi vào metadata (feature importance có thể trích dẫn)."""
    explainer = shap.TreeExplainer(booster)
    shap_values = explainer.shap_values(X)

    up_idx = class_names.index("up")
    # LightGBM multiclass: TreeExplainer có thể trả list[n_class] array hoặc 1 array 3 chiều
    # tuỳ phiên bản shap -> xử lý cả 2 dạng cho chắc.
    if isinstance(shap_values, list):
        shap_up = shap_values[up_idx]
    elif shap_values.ndim == 3:
        shap_up = shap_values[:, :, up_idx]
    else:
        shap_up = shap_values

    plt.figure()
    shap.summary_plot(shap_up, X, show=False, max_display=20)
    plt.title("SHAP feature importance — lớp 'up'")
    plt.tight_layout()
    plt.savefig(output_path, dpi=140)
    plt.close()
    log.info(f"Đã lưu SHAP summary plot: {output_path}")

    mean_abs_shap = pd.Series(
        np.abs(shap_up).mean(axis=0), index=X.columns
    ).sort_values(ascending=False)
    return mean_abs_shap


def plot_calibration_curve(prob_true: list, prob_pred: list, class_name: str, output_path: str):
    """Fix REVIEW_FINDINGS #4: PROJECT_SPEC.evaluation_metrics yêu cầu calibration
    curve nhưng bản trước thiếu hẳn. Vẽ đường calibration (xác suất dự đoán vs tỉ lệ
    thực tế) cho 1 lớp, so với đường "calibrated hoàn hảo" (y=x)."""
    plt.figure()
    plt.plot(prob_pred, prob_true, marker="o", label=f"Model (lớp '{class_name}')")
    plt.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Calibrated hoàn hảo")
    plt.xlabel("Xác suất dự đoán trung bình mỗi bin")
    plt.ylabel("Tỉ lệ thực tế mỗi bin")
    plt.title(f"Calibration curve — lớp '{class_name}' (holdout fold, blind)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=140)
    plt.close()
    log.info(f"Đã lưu calibration curve: {output_path}")
