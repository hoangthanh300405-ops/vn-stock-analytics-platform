"""
tests/test_categorical_vocab.py - REVIEW_FINDINGS #3 và #8: xác nhận categorical_vocab
cho ra encoding ỔN ĐỊNH bất kể lát dữ liệu (subset) đưa vào có thiếu category nào hay
không — đây chính là bug đã sửa (trước đó mỗi lát dữ liệu tự suy category riêng).
"""
import pandas as pd

from ml_price_trend.train import apply_categorical_vocab, build_categorical_vocab


def test_vocab_built_from_full_dataset_includes_all_categories():
    df = pd.DataFrame({
        "exchange": ["HOSE", "HNX", "UPCOM", "HOSE"],
        "sector_name": ["Ngân hàng", "Công nghệ", "Ngân hàng", None],
    })
    vocab = build_categorical_vocab(df)
    assert sorted(vocab["exchange"]) == ["HNX", "HOSE", "UPCOM"]
    assert sorted(vocab["sector_name"]) == ["Công nghệ", "Ngân hàng"]  # None bị loại (dropna)


def test_apply_categorical_vocab_gives_same_codes_regardless_of_subset_seen():
    """Đây là assertion cốt lõi: 2 lát dữ liệu khác nhau (1 lát thiếu hẳn 1 category)
    khi áp cùng 1 vocab phải cho ra CÙNG 1 code cho cùng 1 giá trị — trước khi sửa,
    .astype('category') độc lập trên từng lát sẽ cho code KHÁC NHAU trong trường hợp này."""
    vocab = {"exchange": ["HNX", "HOSE", "UPCOM"]}  # cố định, sorted

    full_slice = pd.DataFrame({"exchange": ["HOSE", "HNX", "UPCOM"]})
    partial_slice = pd.DataFrame({"exchange": ["HOSE"]})  # THIẾU hẳn HNX, UPCOM

    full_applied = apply_categorical_vocab(full_slice, vocab)
    partial_applied = apply_categorical_vocab(partial_slice, vocab)

    hose_code_full = full_applied["exchange"].cat.codes[full_applied["exchange"] == "HOSE"].iloc[0]
    hose_code_partial = partial_applied["exchange"].cat.codes[partial_applied["exchange"] == "HOSE"].iloc[0]

    assert hose_code_full == hose_code_partial, (
        "Code của 'HOSE' bị lệch giữa 2 lát dữ liệu khác nhau — đúng bug REVIEW_FINDINGS #3 "
        "mà apply_categorical_vocab() phải ngăn chặn."
    )


def test_single_row_dataframe_gets_correct_category_dtype():
    """Mô phỏng đúng tình huống ở Streamlit inference: chỉ có 1 dòng dữ liệu (1 giá trị
    category), vẫn phải mã hoá đúng theo vocab đầy đủ, không tự suy category rời rạc."""
    vocab = {"exchange": ["HNX", "HOSE", "UPCOM"], "sector_name": ["Bất động sản", "Công nghệ", "Ngân hàng"]}
    one_row = pd.DataFrame({"exchange": ["UPCOM"], "sector_name": ["Công nghệ"]})

    applied = apply_categorical_vocab(one_row, vocab)
    assert list(applied["exchange"].cat.categories) == vocab["exchange"]
    assert list(applied["sector_name"].cat.categories) == vocab["sector_name"]


def test_value_not_in_vocab_becomes_nan_instead_of_silently_extending_vocab():
    """Nếu 1 giá trị mới xuất hiện (không có trong vocab lúc train — VD mã vừa đổi sàn
    niêm yết), phải thành NaN tường minh (LightGBM xử lý NaN được) thay vì âm thầm mã
    hoá sai lệch với các category khác."""
    vocab = {"exchange": ["HNX", "HOSE"]}
    df = pd.DataFrame({"exchange": ["HOSE", "UPCOM"]})  # UPCOM không có trong vocab
    applied = apply_categorical_vocab(df, vocab)
    assert applied["exchange"].isna().iloc[1]
    assert applied["exchange"].iloc[0] == "HOSE"
