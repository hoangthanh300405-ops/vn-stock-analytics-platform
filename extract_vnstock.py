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

⚠️ KHÔI PHỤC 2026-08-16 — file này đã bị dán đè mất các fix của ngày 13-14/08
   khi thêm tính năng backfill mã mới bên dưới. Gộp lại đầy đủ cả 2:

⚠️ FIX SỰ CỐ 2026-08-14 — "Rate Limit Exceeded, Process terminated" từ vnstock:
   vnstock (gói Community) tự chặn cứng ở 60 request/phút — vượt ngưỡng này,
   thư viện in cảnh báo rồi THOÁT LUÔN CẢ TIẾN TRÌNH, không phải raise 1
   Exception bình thường (nghi là SystemExit — kế thừa BaseException, không
   bị `except Exception` bắt được) -> circuit breaker/time budget cũ KHÔNG
   cứu được vì script chết trước khi kịp chạy tới đoạn xử lý lỗi.
   Nguyên nhân: throttle cố định 0.3s/mã cũ cho phép tới ~200 request/phút,
   gấp hơn 3 lần giới hạn thật. Fix: thay bằng RateLimiter (sliding window,
   tính CHUNG cho mọi lần gọi API kể cả Listing() và các lần retry) giữ
   dưới ngưỡng với biên an toàn, + bắt cả BaseException (trừ
   KeyboardInterrupt) ở vòng lặp per-mã để phòng hờ nếu vẫn lỡ chạm ngưỡng.

⚠️ FIX SỰ CỐ 2026-08-13 — "job bị GitHub Actions cancel giữa chừng":
   trading.vietcap.com.vn đôi khi lỗi diện rộng (hàng loạt mã liên tiếp
   ReadTimeout 30s). Code cũ không có cơ chế dừng sớm -> mỗi mã lỗi tốn tới
   ~100s (30s timeout mặc định của thư viện x 3 lần retry + backoff), nhân
   với hàng trăm mã lỗi liên tiếp -> vượt xa timeout-minutes: 60 của job ->
   GitHub cancel -> MẤT TRẮNG kể cả phần đã lấy được.

   3 cơ chế thêm vào để dứt điểm:
     1. Hard timeout tầng ứng dụng (HARD_CALL_TIMEOUT_SECONDS) — không phụ
        thuộc timeout mặc định 30s của thư viện nữa.
     2. Circuit breaker (CIRCUIT_BREAKER_CONSECUTIVE_FAILURES) — nhiều mã
        liên tiếp lỗi thì coi là API đang sập diện rộng, dừng sớm thay vì
        chạy hết danh sách.
     3. Ngân sách thời gian (MAX_RUNTIME_SECONDS_FACT_PRICE) — tự dừng
        trước khi chạm timeout-minutes của job, luôn ghi được phần đã lấy
        thay vì mất trắng.
   Kèm theo: xoay vòng thứ tự mã theo ngày (_rotate_symbols) để nếu có dừng
   sớm, các mã bị bỏ sót không phải lúc nào cũng là cùng 1 nhóm ở cuối danh
   sách.

⚠️ LỖI ĐÃ TỪNG XÁC NHẬN: Quote ở đây là vnstock.explorer.vci.quote.Quote —
   bản đã gắn sẵn nguồn VCI, __init__ KHÔNG nhận tham số source (khác với
   class Quote hợp nhất ở top-level `from vnstock import Quote`). Truyền
   source="VCI" vào đây bị TypeError NGAY LẬP TỨC — đã xác nhận bằng
   inspect.signature(). Đây chính là nguyên nhân job lỗi 100% các mã ngày
   16/08 sau khi bản backfill dán đè làm mất fix này.

Fix 2026-08-16: LOOKBACK_DAYS=10 trước đây áp dụng ĐỒNG LOẠT cho mọi mã, kể
cả mã lần đầu xuất hiện trong dim_stock (mới niêm yết, hoặc mới được vnstock
thêm vào symbols_by_exchange()) -> mã nào niêm yết trước lần đầu tiên
pipeline này chạy sẽ VĨNH VIỄN chỉ có tối đa 10 ngày giá gần nhất, không bao
giờ tự vá lại lịch sử cũ hơn (lần chạy nào cũng chỉ lùi đúng 10 ngày).
-> Trước khi gọi API, đọc file .duckdb đã tải về workspace ở bước trước
trong daily_etl.yml (bước "Tải file .duckdb của lần chạy trước" chạy TRƯỚC
bước Extract) để biết mã nào ĐÃ từng có ít nhất 1 dòng giá. Mã nào chưa từng
có (mã mới) thì lấy lịch sử dài hơn hẳn (BACKFILL_LOOKBACK_DAYS) thay vì chỉ
LOOKBACK_DAYS ngày.

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

# Số ngày lấy lùi về trước mỗi lần chạy CHO MÃ ĐÃ CÓ DỮ LIỆU RỒI.
LOOKBACK_DAYS = 10

# Số ngày lấy lùi về trước CHO MÃ CHƯA TỪNG CÓ DỮ LIỆU GIÁ (mã mới niêm yết).
# ~10 năm là đủ dài để phủ toàn bộ lịch sử niêm yết của bất kỳ mã nào trên VN.
BACKFILL_LOOKBACK_DAYS = 3650

# Số mã lỗi tối đa được phép bỏ qua (khi chạy HẾT danh sách bình thường)
# trước khi coi là API đang có sự cố diện rộng và dừng hẳn.
MAX_FAILED_SYMBOLS_RATIO = 0.2

# ── Chống "treo cả job hàng giờ" khi API lỗi diện rộng ─────────────────────

# Timeout tầng ứng dụng cho MỖI LẦN gọi Quote.history(), không phụ thuộc
# timeout mặc định (30s) của thư viện HTTP bên trong vnstock.
HARD_CALL_TIMEOUT_SECONDS = 10

# Nếu có ngần này mã LIÊN TIẾP lỗi, coi như trading.vietcap.com.vn đang sập
# diện rộng -> dừng sớm thay vì cắm đầu chạy hết ~1700+ mã còn lại.
CIRCUIT_BREAKER_CONSECUTIVE_FAILURES = 25

# Ngân sách thời gian tối đa cho riêng bước lấy giá (build_fact_price_daily).
MAX_RUNTIME_SECONDS_FACT_PRICE = 30 * 60


class RateLimiter:
    """Giới hạn tối đa `max_calls` lần gọi trong cửa sổ trượt `period_seconds`
    giây — tính CHUNG cho MỌI lần gọi API thật (Listing, Quote, kể cả các lần
    retry), không phải sleep cố định sau mỗi mã như trước."""

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
    """Nhận diện thông báo 'Rate Limit Exceeded ... Process terminated' đặc
    trưng của vnstock, để log rõ ràng hơn là lỗi mạng thông thường."""
    msg = str(exc)
    return "Rate Limit" in msg or "rate limit" in msg.lower()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_symbols_by_exchange() -> pd.DataFrame:
    """Lấy danh sách mã + sàn + tên công ty. Retry vì API vnstock/VCI đôi khi timeout."""
    rate_limiter.wait()
    log.info("Đang gọi Listing().symbols_by_exchange()...")
    df = Listing().symbols_by_exchange(lang="vi")
    log.info(f"symbols_by_exchange trả về {len(df)} dòng")
    return df


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_symbols_by_industries() -> pd.DataFrame:
    """Lấy phân ngành ICB theo mã (dạng dài, nhiều icb_level/mã)."""
    rate_limiter.wait()
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

    if "type" in exchange_df.columns:
        before = len(exchange_df)
        exchange_df = exchange_df[exchange_df["type"] == "STOCK"].reset_index(drop=True)
        log.info(f"Lọc type == 'STOCK': {before} -> {len(exchange_df)} dòng")

    exchange_df = exchange_df[["symbol", "exchange", "organ_name"]]

    industries_df = _fetch_symbols_by_industries()

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
    daily_etl.yml để biết mã nào ĐÃ từng có ít nhất 1 dòng giá trong
    raw.fact_price_daily. Trả về set rỗng (coi MỌI mã là mã mới, backfill
    hết) nếu file chưa tồn tại hoặc bảng chưa từng được tạo.
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


def _rotate_symbols(symbols: list[str]) -> list[str]:
    """Xoay vòng thứ tự mã theo ngày trong năm, để nếu 1 lần chạy phải dừng
    sớm, cùng 1 nhóm mã không phải lúc nào cũng bị bỏ sót."""
    if not symbols:
        return symbols
    offset = date.today().toordinal() % len(symbols)
    return symbols[offset:] + symbols[:offset]


def _call_with_hard_timeout(fn, timeout_seconds: float):
    """Chạy fn() với timeout tầng ứng dụng, không phụ thuộc timeout mặc định
    (30s) của thư viện HTTP bên trong vnstock."""
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as e:
        raise TimeoutError(
            f"Vượt quá hard timeout {timeout_seconds}s (tầng ứng dụng)"
        ) from e
    finally:
        executor.shutdown(wait=False)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=1, max=6))
def _fetch_ohlcv_one_symbol(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Lấy giá OHLCV cho ĐÚNG 1 mã.

    LƯU Ý: Quote ở đây KHÔNG nhận tham số source — truyền vào sẽ bị
    TypeError ngay lập tức (xem cảnh báo ở đầu file).
    """
    rate_limiter.wait()
    return _call_with_hard_timeout(
        lambda: Quote(symbol=symbol, show_log=False).history(
            start=start, end=end, interval="1D"
        ),
        HARD_CALL_TIMEOUT_SECONDS,
    )


def build_fact_price_daily(symbols: list[str]) -> pd.DataFrame:
    """
    Lặp qua từng mã trong `symbols`, gọi Quote.history(), gộp lại thành 1 bảng
    fact_price_daily: cột symbol, time, open, high, low, close, volume.

    Mã CHƯA từng có dữ liệu giá (mới niêm yết) được lấy lùi
    BACKFILL_LOOKBACK_DAYS ngày; mã đã có dữ liệu rồi chỉ lấy lùi
    LOOKBACK_DAYS ngày.

    Có 2 cơ chế dừng sớm để không bị treo hàng giờ khi API lỗi diện rộng:
    circuit breaker theo số mã lỗi liên tiếp, và ngân sách thời gian tối đa.
    Cả 2 trường hợp đều VẪN ghi ra phần dữ liệu đã lấy được thành công.
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
        f"{len(new_symbols)} mã mới/backfill: {backfill_start_str} -> {end_str}, "
        "đã xoay vòng thứ tự theo ngày)..."
    )

    all_frames = []
    failed_symbols = []
    consecutive_failures = 0
    stopped_early_reason = None
    loop_start = time.monotonic()

    for i, symbol in enumerate(symbols, start=1):
        elapsed = time.monotonic() - loop_start
        if elapsed > MAX_RUNTIME_SECONDS_FACT_PRICE:
            stopped_early_reason = (
                f"Đã chạy {elapsed:.0f}s, vượt ngân sách "
                f"{MAX_RUNTIME_SECONDS_FACT_PRICE}s dành cho bước lấy giá -> dừng sớm "
                f"ở mã {i}/{len(symbols)} để các bước sau (Load, dbt) còn kịp chạy "
                "trong timeout-minutes của job."
            )
            log.warning(stopped_early_reason)
            break

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
            failed_symbols.append(symbol)
            consecutive_failures += 1
            if _is_vnstock_hard_rate_limit(e):
                log.error(
                    f"[{i}/{len(symbols)}] Chạm giới hạn rate-limit CỨNG của vnstock "
                    f"dù đã qua RateLimiter — dừng ngay lập tức thay vì tiếp tục gọi "
                    f"(sẽ chỉ chạm lại ngay): {e}"
                )
                stopped_early_reason = (
                    f"Chạm rate-limit cứng của vnstock ở mã {i}/{len(symbols)} — dừng "
                    "sớm, phần còn lại sẽ được vá ở lần chạy sau."
                )
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

    if stopped_early_reason and not all_frames:
        raise RuntimeError(
            f"{stopped_early_reason} Và 0 mã lấy được giá thành công trước khi dừng "
            "-> dừng hẳn, không ghi CSV."
        )

    if fail_ratio > MAX_FAILED_SYMBOLS_RATIO:
        if stopped_early_reason:
            log.warning(
                f"Tỉ lệ lỗi {fail_ratio:.0%} trong {attempted} mã đã thử vượt ngưỡng "
                f"{MAX_FAILED_SYMBOLS_RATIO:.0%}, nhưng do dừng sớm chủ động nên vẫn "
                f"ghi {len(all_frames)} mã lấy được thành công."
            )
        else:
            raise RuntimeError(
                f"{len(failed_symbols)}/{attempted} mã ({fail_ratio:.0%}) lấy giá thất "
                f"bại — vượt ngưỡng {MAX_FAILED_SYMBOLS_RATIO:.0%}, nghi ngờ API đang "
                "lỗi diện rộng. Dừng lại, không ghi file dữ liệu thiếu sót."
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
