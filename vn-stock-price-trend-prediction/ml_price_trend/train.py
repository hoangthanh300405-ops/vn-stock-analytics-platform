"""
train.py - Huấn luyện LightGBM classifier 3 lớp (down/flat/up) với walk-forward CV
+ Optuna hyperparameter tuning.

── Sửa theo REVIEW_FINDINGS ─────────────────────────────────────────────────
#1 (critical, double-dipping): TRƯỚC ĐÂY Optuna tuning và số liệu "fold cuối" báo
   cáo dùng CHUNG 1 fold — số liệu đó thực chất là in-sample. BÂY GIỜ tách hẳn:
     - tuning_fold = folds[-2] — CHỈ dùng để chọn best_params (Optuna) + số vòng
       lặp cố định (best_num_boost_round), không bao giờ dùng để báo cáo hiệu
       năng "kỳ vọng thật".
     - holdout_fold = folds[-1] — KHÔNG hề xuất hiện trong bất kỳ bước chọn
       hyperparameter/early-stopping nào. Model train cho fold này dùng
       best_params + num_boost_round CỐ ĐỊNH (không early-stop trên chính val
       của nó) -> số liệu evaluate_on_fold() cho holdout_fold là số liệu blind
       thật, đúng nghĩa "kỳ vọng hiệu năng thật" nên báo cáo.
   Mỗi fold trong fold_results được gắn cờ is_used_for_hyperparameter_tuning để
   người đọc report không nhầm số nào là blind, số nào không.
#2 (major, is_unbalance vô tác dụng với objective=multiclass theo doc LightGBM):
   Bỏ is_unbalance, thay bằng sample_weight (sklearn compute_sample_weight
   "balanced") truyền qua lgb.Dataset(weight=...) — áp dụng đúng cho MỌI
   objective multiclass, không phụ thuộc bug/giới hạn của is_unbalance.
#3 (major, categorical encoding không ổn định giữa các lần .astype("category")
   độc lập): Không còn tự suy category rời rạc theo từng lát dữ liệu nữa — nhận
   1 "categorical_vocab" cố định (tính 1 LẦN DUY NHẤT từ toàn bộ dataset trước
   khi chia fold, xem train_price_trend_model.py) và áp dụng NHẤT QUÁN bằng
   apply_categorical_vocab() ở mọi nơi (train/val/tuning/holdout/inference).
#4 (major, thiếu calibration curve theo PROJECT_SPEC.evaluation_metrics): thêm
   calibration_curve (sklearn) + Brier score cho từng lớp vào evaluate_on_fold().
"""
import logging
import time

import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, classification_report, confusion_matrix, f1_score, roc_auc_score
from sklearn.utils.class_weight import compute_sample_weight

from . import config
from .splitting import split_fold

log = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)


# ── Categorical vocab cố định (fix #3) ───────────────────────────────────

def build_categorical_vocab(df: pd.DataFrame) -> dict:
    """Tính 1 LẦN DUY NHẤT trên TOÀN BỘ dataset (trước khi chia fold) — danh sách
    category đầy đủ nhất có thể, dùng lại y hệt ở mọi lát dữ liệu sau này (train,
    val của từng fold, và ở bước inference tại Streamlit) để mã hoá category ổn
    định, không phụ thuộc việc 1 fold/1 lần predict có thấy đủ category hay không."""
    return {
        c: sorted(df[c].dropna().unique().tolist())
        for c in config.CATEGORICAL_FEATURES
        if c in df.columns
    }


def apply_categorical_vocab(df: pd.DataFrame, vocab: dict) -> pd.DataFrame:
    """Áp categorical_vocab CỐ ĐỊNH vào df — category không có trong vocab (lẽ ra
    không nên xảy ra vì vocab tính từ toàn bộ dataset) sẽ thành NaN thay vì tự
    được thêm vào làm lệch encoding.

    Loại rõ ràng giá trị ngoài vocab (.where(...)) TRƯỚC khi tạo Categorical, thay vì
    dựa vào việc pd.Categorical(..., categories=...) tự coerce giá trị lạ thành NaN —
    hành vi ngầm này đã bị pandas đánh dấu deprecated (Pandas4Warning) và sẽ raise lỗi
    ở version tương lai."""
    df = df.copy()
    for c, categories in vocab.items():
        if c in df.columns:
            cleaned = df[c].where(df[c].isin(categories), other=np.nan)
            df[c] = pd.Categorical(cleaned, categories=categories)
    return df


def _make_dataset(df: pd.DataFrame, feature_cols: list[str], vocab: dict):
    df = apply_categorical_vocab(df, vocab)
    cat_cols = [c for c in config.CATEGORICAL_FEATURES if c in feature_cols]
    X = df[feature_cols]
    y = df["label_id"]
    return X, y, cat_cols


def _balanced_weight(y: pd.Series) -> np.ndarray:
    """sample_weight kiểu 'balanced' — thay thế is_unbalance (fix #2), hoạt động
    đúng với objective='multiclass' (softmax), khác is_unbalance chỉ có tác dụng
    với binary/multiclassova theo doc LightGBM."""
    return compute_sample_weight("balanced", y)


# ── Optuna tuning (CHỈ trên tuning_fold, xem fix #1) ─────────────────────

def _optuna_objective(trial, train_X, train_y, train_w, val_X, val_y, cat_cols):
    params = {
        "objective": "multiclass",
        "num_class": len(config.CLASS_NAMES),
        "metric": "multi_logloss",
        "verbosity": -1,
        "seed": config.RANDOM_STATE,
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "min_child_samples": trial.suggest_int("min_child_samples", 10, 200),
        "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
        "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
        "bagging_freq": trial.suggest_int("bagging_freq", 1, 7),
        "lambda_l1": trial.suggest_float("lambda_l1", 1e-8, 10.0, log=True),
        "lambda_l2": trial.suggest_float("lambda_l2", 1e-8, 10.0, log=True),
    }

    train_set = lgb.Dataset(train_X, label=train_y, weight=train_w, categorical_feature=cat_cols, free_raw_data=False)
    val_set = lgb.Dataset(val_X, label=val_y, categorical_feature=cat_cols, reference=train_set, free_raw_data=False)

    booster = lgb.train(
        params, train_set, num_boost_round=config.LGBM_NUM_BOOST_ROUND,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(config.LGBM_EARLY_STOPPING_ROUNDS, verbose=False)],
    )
    pred = booster.predict(val_X, num_iteration=booster.best_iteration)
    pred_class = np.argmax(pred, axis=1)
    # Macro F1 (không phải accuracy): 3 lớp có thể mất cân bằng, macro F1 phạt
    # nặng việc model chỉ đoán toàn lớp đa số ("flat").
    return f1_score(val_y, pred_class, average="macro")


def tune_hyperparameters_and_rounds(
    tuning_train: pd.DataFrame, tuning_val: pd.DataFrame, feature_cols: list[str], vocab: dict,
) -> tuple[dict, int, lgb.Booster]:
    """Chọn best_params (Optuna) VÀ best_num_boost_round — cả 2 đều chỉ dùng
    tuning_fold, không đụng tới holdout_fold hay bất kỳ dữ liệu nào khác (fix #1)."""
    train_X, train_y, cat_cols = _make_dataset(tuning_train, feature_cols, vocab)
    val_X, val_y, _ = _make_dataset(tuning_val, feature_cols, vocab)
    train_w = _balanced_weight(train_y)

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=config.RANDOM_STATE))
    t0 = time.monotonic()
    study.optimize(
        lambda trial: _optuna_objective(trial, train_X, train_y, train_w, val_X, val_y, cat_cols),
        n_trials=config.OPTUNA_N_TRIALS,
        timeout=config.OPTUNA_TIMEOUT_SECONDS,
    )
    log.info(
        f"Optuna: {len(study.trials)} trial trong {time.monotonic() - t0:.0f}s, "
        f"macro F1 tốt nhất (trên tuning_fold) = {study.best_value:.4f}"
    )

    # Refit 1 lần nữa với best_params để lấy best_iteration DETERMINISTIC — số vòng
    # lặp này sẽ dùng CỐ ĐỊNH (không early-stop lại) cho mọi fold khác, kể cả
    # holdout_fold, để tránh phải early-stop trên chính dữ liệu muốn đánh giá blind.
    train_set = lgb.Dataset(train_X, label=train_y, weight=train_w, categorical_feature=cat_cols, free_raw_data=False)
    val_set = lgb.Dataset(val_X, label=val_y, categorical_feature=cat_cols, reference=train_set, free_raw_data=False)
    params = {
        "objective": "multiclass", "num_class": len(config.CLASS_NAMES),
        "metric": "multi_logloss", "verbosity": -1, "seed": config.RANDOM_STATE,
        **study.best_params,
    }
    booster = lgb.train(
        params, train_set, num_boost_round=config.LGBM_NUM_BOOST_ROUND,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(config.LGBM_EARLY_STOPPING_ROUNDS, verbose=False), lgb.log_evaluation(0)],
    )
    best_num_boost_round = booster.best_iteration or config.LGBM_NUM_BOOST_ROUND
    log.info(f"Số vòng lặp cố định chọn từ tuning_fold: {best_num_boost_round}")
    return study.best_params, best_num_boost_round, booster


def train_fixed_rounds_model(
    train_df: pd.DataFrame, feature_cols: list[str], vocab: dict, best_params: dict, num_boost_round: int,
) -> lgb.Booster:
    """Train KHÔNG early-stop — num_boost_round CỐ ĐỊNH (đã chọn từ tuning_fold).
    Dùng cho mọi fold KHÔNG PHẢI tuning_fold (đặc biệt là holdout_fold) và cho
    model deploy cuối cùng — đảm bảo không có bước nào "nhìn" vào val của các
    fold này để quyết định kiến trúc/số vòng lặp (fix #1)."""
    train_X, train_y, cat_cols = _make_dataset(train_df, feature_cols, vocab)
    train_w = _balanced_weight(train_y)

    params = {
        "objective": "multiclass", "num_class": len(config.CLASS_NAMES),
        "metric": "multi_logloss", "verbosity": -1, "seed": config.RANDOM_STATE,
        **best_params,
    }
    train_set = lgb.Dataset(train_X, label=train_y, weight=train_w, categorical_feature=cat_cols, free_raw_data=False)
    booster = lgb.train(params, train_set, num_boost_round=num_boost_round, callbacks=[lgb.log_evaluation(0)])
    return booster


def evaluate_on_fold(booster: lgb.Booster, val_df: pd.DataFrame, feature_cols: list[str], vocab: dict) -> dict:
    X, y, _ = _make_dataset(val_df, feature_cols, vocab)
    best_iter = getattr(booster, "best_iteration", None)
    proba = booster.predict(X, num_iteration=best_iter if best_iter else None)
    pred_class = np.argmax(proba, axis=1)

    report = classification_report(
        y, pred_class, labels=[0, 1, 2], target_names=config.CLASS_NAMES,
        output_dict=True, zero_division=0,
    )
    try:
        auc_ovr = roc_auc_score(y, proba, multi_class="ovr", average="macro", labels=[0, 1, 2])
    except ValueError:
        auc_ovr = None  # có thể thiếu hẳn 1 lớp trong fold nhỏ

    # Baseline "giữ nguyên xu hướng hôm trước": dùng cột reference_yesterday_label_id
    # đã tính SẴN ở labeling.py TRƯỚC KHI lọc phiên trần/sàn/NaN feature (fix #6 — xem
    # labeling.py — đảm bảo "hôm qua" luôn là phiên giao dịch liền trước THẬT, không bị
    # lệch bởi các dòng đã bị loại ở bước lọc phía sau).
    if "reference_yesterday_label_id" in val_df.columns:
        valid_mask = val_df["reference_yesterday_label_id"].notna()
        baseline_f1 = (
            f1_score(
                val_df.loc[valid_mask, "label_id"], val_df.loc[valid_mask, "reference_yesterday_label_id"],
                average="macro", zero_division=0,
            ) if valid_mask.sum() > 0 else None
        )
    else:
        baseline_f1 = None

    # Calibration curve + Brier score TỪNG LỚP (fix #4 — PROJECT_SPEC yêu cầu nhưng
    # bản trước thiếu hẳn). n_bins nhỏ (5) vì val của 1 fold không quá nhiều dòng.
    calibration = {}
    brier = {}
    for i, cname in enumerate(config.CLASS_NAMES):
        y_binary = (y == i).astype(int)
        brier[cname] = float(brier_score_loss(y_binary, proba[:, i]))
        try:
            prob_true, prob_pred = calibration_curve(y_binary, proba[:, i], n_bins=5, strategy="quantile")
            calibration[cname] = {"prob_true": prob_true.tolist(), "prob_pred": prob_pred.tolist()}
        except ValueError:
            calibration[cname] = None

    return {
        "classification_report": report,
        # Fix REVIEW_FINDINGS #9: confusion matrix thô — classification_report chỉ cho
        # precision/recall/F1 từng lớp, không cho thấy model đang nhầm "down"<->"up" hay
        # "down"<->"flat" nhiều hơn. labels=[0,1,2] cố định thứ tự = CLASS_NAMES (hàng/cột
        # đều theo down, flat, up).
        "confusion_matrix": confusion_matrix(y, pred_class, labels=[0, 1, 2]).tolist(),
        "roc_auc_ovr_macro": auc_ovr,
        "macro_f1": report["macro avg"]["f1-score"],
        "baseline_same_as_yesterday_macro_f1": baseline_f1,
        "calibration_curve": calibration,
        "brier_score": brier,
        "n_val_rows": len(val_df),
    }


def run_walkforward_training(df: pd.DataFrame, feature_cols: list[str], folds: list, vocab: dict) -> dict:
    """Pipeline đầy đủ (fix #1): tune trên folds[-2] -> refit KHÔNG early-stop cho
    mọi fold khác (đặc biệt folds[-1] = holdout thật) -> model deploy = refit trên
    toàn bộ dữ liệu tới hết holdout_fold, cùng best_params/num_boost_round đã chọn
    CHỈ từ tuning_fold (không leak thêm dữ liệu mới hơn vào việc chọn kiến trúc)."""
    if len(folds) < 2:
        raise RuntimeError(
            "Cần tối thiểu 2 fold walk-forward để tách riêng tuning_fold khỏi "
            "holdout_fold thật (fix REVIEW_FINDINGS #1) — tăng N_WALKFORWARD_SPLITS "
            "hoặc kiểm tra lại dữ liệu."
        )

    tuning_fold = folds[-2]
    holdout_fold = folds[-1]
    tuning_train, tuning_val = split_fold(df, tuning_fold)
    log.info(
        f"Tuning (Optuna, KHÔNG dùng để báo cáo hiệu năng) trên fold {tuning_fold.fold_id}: "
        f"train đến {tuning_fold.train_end_date.date()}, validate "
        f"{tuning_fold.val_start_date.date()} -> {tuning_fold.val_end_date.date()}"
    )
    best_params, best_num_boost_round, tuning_booster = tune_hyperparameters_and_rounds(
        tuning_train, tuning_val, feature_cols, vocab
    )

    fold_results = []
    holdout_metrics = None
    for fold in folds:
        train_part, val_part = split_fold(df, fold)
        if len(train_part) == 0 or len(val_part) == 0:
            continue
        is_tuning_fold = fold.fold_id == tuning_fold.fold_id
        booster = tuning_booster if is_tuning_fold else train_fixed_rounds_model(
            train_part, feature_cols, vocab, best_params, best_num_boost_round
        )
        metrics = evaluate_on_fold(booster, val_part, feature_cols, vocab)
        metrics["fold_id"] = fold.fold_id
        # QUAN TRỌNG khi đọc report: fold có is_used_for_hyperparameter_tuning=True
        # là số liệu IN-SAMPLE (đã dùng để chọn hyperparameter), KHÔNG đại diện cho
        # hiệu năng thật — chỉ fold holdout (fold_id == holdout_fold.fold_id, luôn là
        # fold cuối) mới là số liệu blind đáng tin.
        metrics["is_used_for_hyperparameter_tuning"] = is_tuning_fold
        metrics["train_end_date"] = str(fold.train_end_date.date())
        metrics["val_range"] = f"{fold.val_start_date.date()} -> {fold.val_end_date.date()}"
        fold_results.append(metrics)
        log.info(
            f"Fold {fold.fold_id} (tuning={is_tuning_fold}): macro F1={metrics['macro_f1']:.4f} "
            f"(baseline={metrics['baseline_same_as_yesterday_macro_f1']})"
        )
        if fold.fold_id == holdout_fold.fold_id:
            holdout_metrics = metrics

    # Model DEPLOY: refit trên TOÀN BỘ dữ liệu tới hết holdout_fold (dùng dữ liệu mới
    # nhất có) — nhưng best_params/num_boost_round vẫn chỉ được chọn từ tuning_fold ở
    # trên, nên bước refit cuối này KHÔNG thêm bất kỳ leakage nào mới.
    holdout_train_part, holdout_val_part = split_fold(df, holdout_fold)
    deploy_data = pd.concat([holdout_train_part, holdout_val_part], ignore_index=True)
    final_booster = train_fixed_rounds_model(deploy_data, feature_cols, vocab, best_params, best_num_boost_round)

    return {
        "best_params": best_params,
        "best_num_boost_round": best_num_boost_round,
        "fold_results": fold_results,
        "holdout_metrics": holdout_metrics,  # <- con số nên trích dẫn khi báo cáo hiệu năng thật
        "final_booster": final_booster,
        "final_train_end_date": str(holdout_fold.val_end_date.date()),
        "tuning_fold": tuning_fold,
        "holdout_fold": holdout_fold,
    }
