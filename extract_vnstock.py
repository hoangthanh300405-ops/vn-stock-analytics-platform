"""
extract_vnstock.py
----------------------
Giai đoạn 1 (Extract) trong pipeline: gọi API vnstock, ghi ra các file CSV
trung gian mà build_duckdb_file.py (giai đoạn 2 - Load) sẽ đọc vào.

File này có 2 phần:
  1. dim_stock.csv       - danh mục mã (symbol, organ_name, exchange, industry)
  2. fact_price_daily.csv - giá OHLCV theo mã-ngày, chạy sau khi có dim_stock
                            (cần danh sách mã từ bước 1 để lặp qua từng mã)

company_profile.csv (thông tin công ty, chạy hàng tuần) CHƯA có trong file
này — build_duckdb_file.py đã tự bỏ qua nếu thiếu file này (xem
load_company_profile()), nên không chặn pipeline hàng ngày.

Nguồn dữ liệu: vnstock v4.0.5, nguồn VCI. Đối chiếu trực tiếp với source code
vnstock/explorer/vci/listing.py và vnstock/explorer/vci/quote.py để đảm bảo
đúng tên hàm/tên cột thật (KHÔNG dùng tên cột đoán từ tài liệu cũ):

  - Listing().symbols_by_exchange()
      -> 1 dòng / 1 mã, cột: symbol, exchange, type, organ_name, organ_short_name

  - Listing().symbols_by_industries()
      -> DẠNG DÀI: 1 mã có thể có tới 4 dòng (icb_level 1..4, từ ngành lớn
         (VD "Tài chính") tới ngành nhỏ (VD "Ngân hàng thương mại")).
      -> Ta chọn icb_level=2 làm "industry" hiển thị trên dashboard.
         Đổi ICB_LEVEL_FOR_INDUSTRY bên dưới nếu muốn cấp khác.

  - Quote(symbol=<mã>, source="VCI").history(start=..., end=..., interval="1D")
      -> 1 dòng / 1 ngày giao dịch của ĐÚNG 1 mã, cột: time, open, high, low,
         close, volume (KHÔNG có sẵn cột symbol -> phải tự thêm vào).
      -> Gọi lặp qua TỪNG mã (API không có endpoint lấy nhiều mã 1 lần), nên
         cần throttle (nghỉ giữa các lần gọi) để tránh bị chặn IP/rate-limit.

Yêu cầu: pip install vnstock==4.0.5 pandas tenacity (đã pin trong requirements.txt)
"""

import logging
import time
from datetime import date, timedelta

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from vnstock.explorer.vci.listing import Listing
from vnstock.explorer.vci.quote import Quote

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DIM_STOCK_CSV = "dim_stock.csv"
FACT_PRICE_CSV = "fact_price_daily.csv"

# Cấp ICB dùng làm cột "industry" — xem giải thích ở docstring phía trên.
ICB_LEVEL_FOR_INDUSTRY = 2

# Số ngày lấy lùi về trước mỗi lần chạy. Đặt dư ra (không chỉ lấy 1 ngày hôm
# qua) để tự vá những ngày bị miss do lần chạy trước lỗi/API tạm thời không
# có dữ liệu — trùng lặp với dữ liệu cũ sẽ được dbt dedup ở bước staging
# (ROW_NUMBER theo symbol+trade_date, xem stg_vnstock__fact_price_daily.sql).
LOOKBACK_DAYS = 10

# Nghỉ giữa mỗi lần gọi API cho 1 mã, để không bị VCI chặn vì gọi quá dồn dập.
# Với ~1700 mã: 1700 * 0.3s ≈ 8.5 phút, cộng thời gian mạng thực tế mỗi call
# -> nằm trong ngân sách 20-30 phút đã tính trong daily_etl.yml.
THROTTLE_SECONDS = 0.3

# Số mã lỗi tối đa được phép bỏ qua trước khi coi là API đang có sự cố diện
# rộng và dừng hẳn (tránh ghi ra 1 file gần như rỗng mà không ai biết).
MAX_FAILED_SYMBOLS_RATIO = 0.2


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_symbols_by_exchange() -> pd.DataFrame:
    """Lấy danh sách mã + sàn + tên công ty. Retry vì API vnstock/VCI đôi khi timeout."""
    log.info("Đang gọi Listing().symbols_by_exchange()...")
    df = Listing().symbols_by_exchange(lang="vi")
    log.info(f"symbols_by_exchange trả về {len(df)} dòng")
    return df


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_symbols_by_industries() -> pd.DataFrame:
    """Lấy phân ngành ICB theo mã (dạng dài, nhiều icb_level/mã)."""
    log.info("Đang gọi Listing().symbols_by_industries()...")
    df = Listing().symbols_by_industries(lang="vi")
    log.info(f"symbols_by_industries trả về {len(df)} dòng (dạng dài, nhiều level/mã)")
    return df


def build_dim_stock() -> pd.DataFrame:
    """
    Gộp symbols_by_exchange() + symbols_by_industries() thành 1 bảng
    dim_stock: 1 dòng / 1 mã, cột symbol, organ_name, exchange, industry.
    """
    exchange_df = _fetch_symbols_by_exchange()

    # Chỉ giữ cổ phiếu thường (loại bỏ chứng quyền/trái phiếu/ETF nếu type
    # có các giá trị khác "STOCK") — theo đúng cách all_symbols() của chính
    # vnstock lọc, xem listing.py.
    if "type" in exchange_df.columns:
        before = len(exchange_df)
        exchange_df = exchange_df[exchange_df["type"] == "STOCK"].reset_index(drop=True)
        log.info(f"Lọc type == 'STOCK': {before} -> {len(exchange_df)} dòng")

    exchange_df = exchange_df[["symbol", "exchange", "organ_name"]]

    industries_df = _fetch_symbols_by_industries()

    # symbols_by_industries() trả dạng dài -> lọc đúng 1 level, mỗi mã còn 1 dòng.
    # Một số mã có thể thiếu level 2 (dữ liệu ICB không đầy đủ) -> các mã đó sẽ
    # có industry = NULL sau khi left join, dbt staging đã NULLIF/TRIM sẵn.
    industry_at_level = (
        industries_df[industries_df["icb_level"] == ICB_LEVEL_FOR_INDUSTRY][
            ["symbol", "icb_name"]
        ]
        .drop_duplicates(subset=["symbol"])
        .rename(columns={"icb_name": "industry"})
    )
    missing = set(exchange_df["symbol"]) - set(industry_at_level["symbol"])
    if missing:
        log.warning(
            f"{len(missing)} mã không có industry ở icb_level={ICB_LEVEL_FOR_INDUSTRY} "
            "(sẽ để trống, không chặn pipeline)"
        )

    dim_stock = exchange_df.merge(industry_at_level, on="symbol", how="left")
    dim_stock = dim_stock[["symbol", "organ_name", "exchange", "industry"]]

    return dim_stock


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
def _fetch_ohlcv_one_symbol(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Lấy giá OHLCV cho ĐÚNG 1 mã. Retry 3 lần vì API hay timeout/lỗi mạng tạm thời.

    LƯU Ý: Quote ở đây là vnstock.explorer.vci.quote.Quote — bản đã gắn sẵn
    nguồn VCI, __init__ KHÔNG nhận tham số source (khác với class Quote hợp
    nhất ở top-level `from vnstock import Quote`). Truyền source="VCI" vào
    đây sẽ bị TypeError ngay lập tức — đã xác nhận bằng inspect.signature().
    """
    df = Quote(symbol=symbol, show_log=False).history(start=start, end=end, interval="1D")
    return df


def build_fact_price_daily(symbols: list[str]) -> pd.DataFrame:
    """
    Lặp qua từng mã trong `symbols`, gọi Quote.history(), gộp lại thành 1 bảng
    fact_price_daily: cột symbol, time, open, high, low, close, volume.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    log.info(
        f"Lấy giá OHLCV cho {len(symbols)} mã, khoảng {start_str} -> {end_str} "
        f"(lookback {LOOKBACK_DAYS} ngày)..."
    )

    all_frames = []
    failed_symbols = []

    for i, symbol in enumerate(symbols, start=1):
        try:
            df = _fetch_ohlcv_one_symbol(symbol, start_str, end_str)
            if df is not None and len(df) > 0:
                df = df.copy()
                df["symbol"] = symbol
                all_frames.append(df)
        except Exception as e:
            # Không để 1 mã lỗi (VD mã đã huỷ niêm yết, mã mới chưa có giá)
            # làm chết cả job — log lại và đi tiếp, tổng hợp cảnh báo ở cuối.
            failed_symbols.append(symbol)
            log.warning(f"[{i}/{len(symbols)}] Lỗi khi lấy giá {symbol}: {e}")

        if i % 100 == 0:
            log.info(f"Tiến độ: {i}/{len(symbols)} mã đã xử lý...")

        time.sleep(THROTTLE_SECONDS)

    fail_ratio = len(failed_symbols) / len(symbols) if symbols else 0
    if fail_ratio > MAX_FAILED_SYMBOLS_RATIO:
        raise RuntimeError(
            f"{len(failed_symbols)}/{len(symbols)} mã ({fail_ratio:.0%}) lấy giá thất bại "
            f"— vượt ngưỡng {MAX_FAILED_SYMBOLS_RATIO:.0%}, nghi ngờ API đang lỗi diện rộng. "
            "Dừng lại, không ghi file dữ liệu thiếu sót."
        )
    if failed_symbols:
        log.warning(f"Có {len(failed_symbols)} mã lấy giá thất bại (đã bỏ qua): {failed_symbols}")

    if not all_frames:
        return pd.DataFrame(columns=["symbol", "time", "open", "high", "low", "close", "volume"])

    fact_price_daily = pd.concat(all_frames, ignore_index=True)
    fact_price_daily = fact_price_daily[["symbol", "time", "open", "high", "low", "close", "volume"]]
    return fact_price_daily


def main():
    dim_stock = build_dim_stock()

    if len(dim_stock) == 0:
        # Không ghi file rỗng — build_duckdb_file.py đã có sẵn guard chặn CSV
        # rỗng ghi đè dữ liệu tốt, nhưng chặn sớm ở đây rõ ràng hơn.
        raise RuntimeError("build_dim_stock() trả về 0 dòng — dừng lại, không ghi CSV rỗng.")

    dim_stock.to_csv(DIM_STOCK_CSV, index=False)
    log.info(f"Đã ghi {DIM_STOCK_CSV}: {len(dim_stock)} dòng")

    fact_price_daily = build_fact_price_daily(dim_stock["symbol"].tolist())

    if len(fact_price_daily) == 0:
        raise RuntimeError(
            "build_fact_price_daily() trả về 0 dòng — dừng lại, không ghi CSV rỗng."
        )

    fact_price_daily.to_csv(FACT_PRICE_CSV, index=False)
    log.info(f"Đã ghi {FACT_PRICE_CSV}: {len(fact_price_daily)} dòng")


if __name__ == "__main__":
    main()
