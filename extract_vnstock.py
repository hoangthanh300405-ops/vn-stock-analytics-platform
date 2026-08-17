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

  - Quote(symbol=<mã>, show_log=False).history(start=..., end=..., interval="1D")
      -> 1 dòng / 1 ngày giao dịch của ĐÚNG 1 mã, cột: time, open, high, low,
         close, volume (KHÔNG có sẵn cột symbol -> phải tự thêm vào).
      -> Gọi lặp qua TỪNG mã (API không có endpoint lấy nhiều mã 1 lần), nên
         cần throttle (nghỉ giữa các lần gọi) để tránh bị chặn IP/rate-limit.
      -> ⚠️ Quote ở đây là vnstock.explorer.vci.quote.Quote (đã gắn sẵn nguồn
         VCI) — __init__ KHÔNG nhận tham số source. Truyền source="VCI" vào
         sẽ bị TypeError ngay lập tức trên MỌI mã (xác nhận bằng
         inspect.signature() thật, xem sự cố ngày 16/08).

Fix 2026-08-16: LOOKBACK_DAYS=10 trước đây áp dụng ĐỒNG LOẠT cho mọi mã, kể
cả mã lần đầu xuất hiện trong dim_stock (mới niêm yết, hoặc mới được vnstock
thêm vào symbols_by_exchange()) -> mã nào niêm yết trước lần đầu tiên
pipeline này chạy sẽ VĨNH VIỄN chỉ có tối đa 10 ngày giá gần nhất, không bao
giờ tự vá lại lịch sử cũ hơn (lần chạy nào cũng chỉ lùi đúng 10 ngày).
-> Trước khi gọi API, đọc file .duckdb đã tải về workspace ở bước trước
trong daily_etl.yml (bước "Tải file .duckdb của lần chạy trước" chạy TRƯỚC
bước Extract) để biết mã nào ĐÃ từng có ít nhất 1 dòng giá. Mã nào chưa từng
có (mã mới) thì lấy lịch sử dài hơn hẳn (BACKFILL_LOOKBACK_DAYS) thay vì chỉ
LOOKBACK_DAYS ngày — API tự trả về đúng từ ngày niêm yết thật nếu ta xin
khoảng ngày rộng hơn cả lịch sử thật của mã, không cần biết chính xác ngày
niêm yết trước.

Yêu cầu: pip install vnstock==4.0.5 pandas duckdb tenacity (đã pin trong requirements.txt)
"""

import collections
import concurrent.futures
import logging
import os
import time
from datetime import date, timedelta

import duckdb
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

# Số ngày lấy lùi về trước mỗi lần chạy CHO MÃ ĐÃ CÓ DỮ LIỆU RỒI. Đặt dư ra
# (không chỉ lấy 1 ngày hôm qua) để tự vá những ngày bị miss do lần chạy
# trước lỗi/API tạm thời không có dữ liệu — trùng lặp với dữ liệu cũ sẽ được
# dbt dedup ở bước staging (ROW_NUMBER theo symbol+trade_date, xem
# stg_vnstock__fact_price_daily.sql).
LOOKBACK_DAYS = 10

# Số ngày lấy lùi về trước CHO MÃ CHƯA TỪNG CÓ DỮ LIỆU GIÁ (xem Fix 2026-08-16
# ở docstring). ~10 năm là đủ dài để phủ toàn bộ lịch sử niêm yết của bất kỳ
# mã nào trên VN — không cần biết chính xác ngày niêm yết, API tự trả về
# đúng từ ngày mã đó thực sự có giao dịch.
BACKFILL_LOOKBACK_DAYS = 3650

# Số mã lỗi tối đa được phép bỏ qua (khi chạy HẾT danh sách bình thường)
# trước khi coi là API đang có sự cố diện rộng và dừng hẳn.
MAX_FAILED_SYMBOLS_RATIO = 0.2

# ── KHÔI PHỤC 2026-08-16 — các cơ chế chống "treo cả job hàng giờ" khi API
# lỗi diện rộng, bị mất khi thêm tính năng backfill (xem điều tra sự cố
# ngày 16/08: 100% mã lỗi liên tiếp, job chạy tới 566 mã rồi mới bị GitHub
# Actions timeout-cancel vì không có cơ chế nào dừng sớm) ─────────────────

# Timeout tầng ứng dụng cho MỖI LẦN gọi Quote.history(), không phụ thuộc
# timeout mặc định (30s) của thư viện HTTP bên trong vnstock.
HARD_CALL_TIMEOUT_SECONDS = 10

# Nếu có ngần này mã LIÊN TIẾP lỗi, coi như trading.vietcap.com.vn đang sập
# diện rộng -> dừng sớm thay vì cắm đầu chạy hết ~1700+ mã còn lại.
CIRCUIT_BREAKER_CONSECUTIVE_FAILURES = 25


class RateLimiter:
    """Giới hạn tối đa `max_calls` lần gọi trong cửa sổ trượt `period_seconds`
    giây — tính CHUNG cho MỌI lần gọi API thật, không phải sleep cố định sau
    mỗi mã (0.3s cố định cho phép ~200 req/phút, vượt xa giới hạn thật 60
    req/phút của vnstock Community -> đây chính là nguyên nhân sự cố
    "Rate Limit Exceeded, Process terminated" ngày 13-14/08)."""

    def __init__(self, max_calls: int, period_seconds: float):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self._call_times: collections.deque = collections.deque()

    def wait(self):
        now = time.monotonic()
        while self._call_times and now - self._call_times[0] > self.period_seconds:
            self._call_times.popleft()
        if len(self._call_times) >= self.max_calls:
            sleep_for = self.period_seconds - (now - self._call_times[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            while self._call_times and now - self._call_times[0] > self.period_seconds:
                self._call_times.popleft()
        self._call_times.append(time.monotonic())


# Giới hạn thật của vnstock (Community) là 60 request/phút — đặt 45 (biên an
# toàn ~25%) để chừa chỗ cho các request phụ (Listing() ở đầu, các lần retry).
rate_limiter = RateLimiter(max_calls=45, period_seconds=60)


def _is_vnstock_hard_rate_limit(exc: BaseException) -> bool:
    msg = str(exc)
    return "Rate Limit" in msg or "rate limit" in msg.lower()


def _call_with_hard_timeout(fn, timeout_seconds: float):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as e:
        raise TimeoutError(f"Vượt quá hard timeout {timeout_seconds}s (tầng ứng dụng)") from e
    finally:
        executor.shutdown(wait=False)


def _rotate_symbols(symbols: list[str]) -> list[str]:
    """Xoay vòng thứ tự mã theo ngày, để nếu 1 lần chạy phải dừng sớm, cùng 1
    nhóm mã không phải lúc nào cũng bị bỏ sót."""
    if not symbols:
        return symbols
    offset = date.today().toordinal() % len(symbols)
    return symbols[offset:] + symbols[:offset]


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


def _get_symbols_with_existing_price_data() -> set[str]:
    """
    Đọc file .duckdb đã tải về workspace ở bước TRƯỚC bước Extract trong
    daily_etl.yml (xem step "Tải file .duckdb của lần chạy trước") để biết
    mã nào ĐÃ từng có ít nhất 1 dòng giá trong raw.fact_price_daily.

    Dùng read_only=True vì Extract và Load (build_duckdb_file.py) không chạy
    cùng lúc trong 1 job nên không tranh chấp, nhưng tránh lỡ tay ghi nhầm.
    Trả về set rỗng (coi MỌI mã là mã mới, backfill hết) nếu file chưa tồn
    tại (lần chạy đầu tiên của cả pipeline) hoặc bảng raw.fact_price_daily
    chưa từng được tạo — cả 2 trường hợp đều hợp lý để backfill toàn bộ.
    """
    db_path = os.environ.get("DUCKDB_FILE_PATH", "vnstock.duckdb")
    if not os.path.exists(db_path):
        log.info(
            "Chưa có file .duckdb sẵn từ lần chạy trước -> coi MỌI mã là mã mới, "
            f"backfill {BACKFILL_LOOKBACK_DAYS} ngày cho tất cả."
        )
        return set()

    try:
        con = duckdb.connect(db_path, read_only=True)
        existing = set(
            con.execute("SELECT DISTINCT symbol FROM raw.fact_price_daily")
            .fetchdf()["symbol"]
            .tolist()
        )
        con.close()
        log.info(f"Đã có dữ liệu giá cho {len(existing)} mã trong file .duckdb hiện tại")
        return existing
    except duckdb.CatalogException:
        log.info("Bảng raw.fact_price_daily chưa tồn tại trong file .duckdb -> coi MỌI mã là mã mới.")
        return set()


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=1, max=6))
def _fetch_ohlcv_one_symbol(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Lấy giá OHLCV cho ĐÚNG 1 mã.

    LƯU Ý: Quote ở đây (vnstock.explorer.vci.quote.Quote) KHÔNG nhận tham số
    source — truyền vào sẽ bị TypeError ngay lập tức trên MỌI mã (đã xác
    nhận bằng inspect.signature() thật, xem sự cố ngày 16/08).
    """
    rate_limiter.wait()
    return _call_with_hard_timeout(
        lambda: Quote(symbol=symbol, show_log=False).history(start=start, end=end, interval="1D"),
        HARD_CALL_TIMEOUT_SECONDS,
    )


def build_fact_price_daily(symbols: list[str]) -> pd.DataFrame:
    """
    Lặp qua từng mã trong `symbols`, gọi Quote.history(), gộp lại thành 1 bảng
    fact_price_daily: cột symbol, time, open, high, low, close, volume.

    Mã CHƯA từng có dữ liệu giá (mới niêm yết/mới thêm vào danh sách) được
    lấy lùi BACKFILL_LOOKBACK_DAYS ngày; mã đã có dữ liệu rồi chỉ lấy lùi
    LOOKBACK_DAYS ngày như cũ (xem Fix 2026-08-16 ở docstring đầu file).
    """
    symbols = _rotate_symbols(symbols)

    end_date = date.today()
    normal_start_str = (end_date - timedelta(days=LOOKBACK_DAYS)).isoformat()
    backfill_start_str = (end_date - timedelta(days=BACKFILL_LOOKBACK_DAYS)).isoformat()
    end_str = end_date.isoformat()

    existing_symbols = _get_symbols_with_existing_price_data()
    new_symbols = set(symbols) - existing_symbols

    if new_symbols:
        preview = sorted(new_symbols)[:20]
        log.info(
            f"Phát hiện {len(new_symbols)} mã CHƯA từng có dữ liệu giá -> backfill từ "
            f"{backfill_start_str} thay vì chỉ {LOOKBACK_DAYS} ngày gần nhất: {preview}"
            f"{' ...' if len(new_symbols) > 20 else ''}"
        )

    log.info(
        f"Lấy giá OHLCV cho {len(symbols)} mã "
        f"({len(symbols) - len(new_symbols)} mã cũ: {normal_start_str} -> {end_str}, "
        f"{len(new_symbols)} mã mới/backfill: {backfill_start_str} -> {end_str})..."
    )

    all_frames = []
    failed_symbols = []
    consecutive_failures = 0
    stopped_early_reason = None

    for i, symbol in enumerate(symbols, start=1):
        start_str = backfill_start_str if symbol in new_symbols else normal_start_str
        try:
            df = _fetch_ohlcv_one_symbol(symbol, start_str, end_str)
            if df is not None and len(df) > 0:
                df = df.copy()
                df["symbol"] = symbol
                all_frames.append(df)
            consecutive_failures = 0
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            # Bắt cả BaseException (không chỉ Exception): "Rate Limit Exceeded"
            # của vnstock từng khiến cả tiến trình bị thoát bằng SystemExit
            # (kế thừa BaseException, KHÔNG bị `except Exception` bắt được).
            failed_symbols.append(symbol)
            consecutive_failures += 1
            if _is_vnstock_hard_rate_limit(e):
                stopped_early_reason = (
                    f"Chạm rate-limit cứng của vnstock ở mã {i}/{len(symbols)} dù đã qua "
                    "RateLimiter — dừng ngay, phần còn lại sẽ được vá ở lần chạy sau."
                )
                log.error(stopped_early_reason)
                break
            log.warning(f"[{i}/{len(symbols)}] Lỗi khi lấy giá {symbol}: {e}")

            if consecutive_failures >= CIRCUIT_BREAKER_CONSECUTIVE_FAILURES:
                stopped_early_reason = (
                    f"{consecutive_failures} mã liên tiếp lỗi -> nghi ngờ "
                    "trading.vietcap.com.vn đang lỗi diện rộng/sập, dừng sớm ở mã "
                    f"{i}/{len(symbols)} thay vì tiếp tục chạy hết danh sách."
                )
                log.error(stopped_early_reason)
                break

        if i % 100 == 0:
            log.info(f"Tiến độ: {i}/{len(symbols)} mã đã xử lý...")

    attempted = len(all_frames) + len(failed_symbols)
    fail_ratio = len(failed_symbols) / attempted if attempted else 1.0
    if fail_ratio > MAX_FAILED_SYMBOLS_RATIO:
        if stopped_early_reason:
            log.warning(
                f"Tỉ lệ lỗi {fail_ratio:.0%} trong {attempted} mã đã thử vượt ngưỡng, nhưng "
                f"do dừng sớm chủ động nên vẫn ghi {len(all_frames)} mã lấy được thành công."
            )
        else:
            raise RuntimeError(
                f"{len(failed_symbols)}/{attempted} mã ({fail_ratio:.0%}) lấy giá thất bại "
                f"— vượt ngưỡng {MAX_FAILED_SYMBOLS_RATIO:.0%}, nghi ngờ API đang lỗi diện rộng. "
                "Dừng lại, không ghi file dữ liệu thiếu sót."
            )
    elif failed_symbols:
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
