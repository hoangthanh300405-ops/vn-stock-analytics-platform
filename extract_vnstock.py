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

⚠️ FIX SỰ CỐ 2026-08-13 — "job bị GitHub Actions cancel giữa chừng":
   trading.vietcap.com.vn đôi khi lỗi diện rộng (hàng loạt mã liên tiếp
   ReadTimeout 30s). Code cũ không có cơ chế dừng sớm -> mỗi mã lỗi tốn tới
   ~100s (30s timeout mặc định của thư viện x 3 lần retry + backoff), nhân
   với hàng trăm mã lỗi liên tiếp -> vượt xa timeout-minutes: 60 của job ->
   GitHub cancel -> MẤT TRẮNG kể cả phần đã lấy được (vì to_csv() chỉ chạy ở
   cuối main(), sau khi build_fact_price_daily() return).

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
   sách — về lâu dài mọi mã đều được thử qua, không có nhóm nào bị bỏ sót
   vĩnh viễn.

Yêu cầu: pip install vnstock==4.0.5 pandas tenacity (đã pin trong requirements.txt)
"""

import concurrent.futures
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
# có dữ liệu, hoặc do circuit breaker/ngân sách thời gian dừng sớm (xem dưới)
# — trùng lặp với dữ liệu cũ sẽ được dbt dedup ở bước staging (ROW_NUMBER
# theo symbol+trade_date, xem stg_vnstock__fact_price_daily.sql).
LOOKBACK_DAYS = 10

# Nghỉ giữa mỗi lần gọi API cho 1 mã, để không bị VCI chặn vì gọi quá dồn dập.
THROTTLE_SECONDS = 0.3

# Số mã lỗi tối đa được phép bỏ qua trước khi coi là API đang có sự cố diện
# rộng và dừng hẳn (tránh ghi ra 1 file gần như rỗng mà không ai biết).
# Áp dụng khi vòng lặp CHẠY HẾT danh sách bình thường; nếu dừng sớm vì circuit
# breaker/ngân sách thời gian thì tỉ lệ lỗi cao là chuyện đương nhiên, không
# raise nữa (xem build_fact_price_daily).
MAX_FAILED_SYMBOLS_RATIO = 0.2

# ── Chống "treo cả job hàng giờ" khi API lỗi diện rộng ─────────────────────

# Timeout tầng ứng dụng cho MỖI LẦN gọi Quote.history(), không phụ thuộc
# timeout mặc định (30s) của thư viện HTTP bên trong vnstock. Nếu 1 lần gọi
# vượt quá số giây này, coi như lỗi và retry/bỏ qua ngay — không phải chờ đủ
# 30s như trước.
HARD_CALL_TIMEOUT_SECONDS = 10

# Nếu có ngần này mã LIÊN TIẾP lỗi, coi như trading.vietcap.com.vn đang sập
# diện rộng (đúng như log sự cố ngày 13/08: hàng trăm mã liên tiếp
# ReadTimeout) -> dừng sớm thay vì cắm đầu chạy hết ~1700+ mã còn lại.
CIRCUIT_BREAKER_CONSECUTIVE_FAILURES = 25

# Ngân sách thời gian tối đa cho riêng bước lấy giá (build_fact_price_daily).
# job daily_etl.yml có timeout-minutes: 60 cho CẢ job (extract dim_stock +
# extract giá + load + dbt seed/run/test + upload) -> để dư khoảng 30 phút
# cho bước lấy giá là hợp lý, phần còn lại (~30 phút) dành cho các bước sau.
MAX_RUNTIME_SECONDS_FACT_PRICE = 30 * 60


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


def _rotate_symbols(symbols: list[str]) -> list[str]:
    """Xoay vòng thứ tự mã theo ngày trong năm.

    Nếu 1 lần chạy phải dừng sớm (circuit breaker hoặc hết ngân sách thời
    gian), các mã ở CUỐI danh sách sẽ không được thử tới. Nếu thứ tự luôn cố
    định, cùng 1 nhóm mã sẽ luôn bị bỏ sót mỗi khi có sự cố -> dữ liệu của
    nhóm đó có thể thiếu nhiều ngày liền. Xoay vòng theo ngày đảm bảo nhóm bị
    bỏ sót đổi khác mỗi ngày, về lâu dài mọi mã đều được phủ.
    """
    if not symbols:
        return symbols
    offset = date.today().toordinal() % len(symbols)
    return symbols[offset:] + symbols[:offset]


def _call_with_hard_timeout(fn, timeout_seconds: float):
    """Chạy fn() với timeout tầng ứng dụng, không phụ thuộc timeout mặc định
    (30s) của thư viện HTTP bên trong vnstock.

    Chạy trong 1 thread riêng: nếu quá hạn, ta bỏ qua ngay (raise TimeoutError)
    thay vì chờ tiếp — request gốc có thể vẫn đang chạy ngầm và tự kết thúc
    sau đó (bị hủy tham chiếu), nhưng ta không còn phải CHỜ nó nữa, đây chính
    là điểm mấu chốt để không bị treo hàng chục phút khi API lỗi diện rộng.
    """
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
    """Lấy giá OHLCV cho ĐÚNG 1 mã. Retry 2 lần (giảm từ 3 — xem ghi chú sự cố
    ở đầu file), mỗi lần bị chặn bởi HARD_CALL_TIMEOUT_SECONDS thay vì chờ
    30s mặc định của thư viện.

    LƯU Ý: Quote ở đây là vnstock.explorer.vci.quote.Quote — bản đã gắn sẵn
    nguồn VCI, __init__ KHÔNG nhận tham số source (khác với class Quote hợp
    nhất ở top-level `from vnstock import Quote`). Truyền source="VCI" vào
    đây sẽ bị TypeError ngay lập tức — đã xác nhận bằng inspect.signature().
    """
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

    Có 2 cơ chế dừng sớm để không bị treo hàng giờ khi API lỗi diện rộng
    (xem ghi chú sự cố ở đầu file): circuit breaker theo số mã lỗi liên tiếp,
    và ngân sách thời gian tối đa. Cả 2 trường hợp đều VẪN ghi ra phần dữ
    liệu đã lấy được thành công (không mất trắng như code cũ).
    """
    symbols = _rotate_symbols(symbols)

    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    log.info(
        f"Lấy giá OHLCV cho {len(symbols)} mã, khoảng {start_str} -> {end_str} "
        f"(lookback {LOOKBACK_DAYS} ngày, đã xoay vòng thứ tự theo ngày)..."
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

        try:
            df = _fetch_ohlcv_one_symbol(symbol, start_str, end_str)
            if df is not None and len(df) > 0:
                df = df.copy()
                df["symbol"] = symbol
                all_frames.append(df)
            consecutive_failures = 0
        except Exception as e:
            # 1 mã lỗi (VD mã đã huỷ niêm yết, mã mới chưa có giá) không được
            # làm chết cả batch — log lại và đi tiếp, tổng hợp cảnh báo ở cuối.
            failed_symbols.append(symbol)
            consecutive_failures += 1
            log.warning(f"[{i}/{len(symbols)}] Lỗi khi lấy giá {symbol}: {e}")

            if consecutive_failures >= CIRCUIT_BREAKER_CONSECUTIVE_FAILURES:
                stopped_early_reason = (
                    f"{consecutive_failures} mã liên tiếp lỗi -> nghi ngờ "
                    "trading.vietcap.com.vn đang lỗi diện rộng/sập, dừng sớm ở mã "
                    f"{i}/{len(symbols)} thay vì tiếp tục chạy hết danh sách (tránh "
                    "treo job cả giờ rồi bị GitHub Actions cancel giữa chừng)."
                )
                log.error(stopped_early_reason)
                break

        if i % 100 == 0:
            log.info(f"Tiến độ: {i}/{len(symbols)} mã đã xử lý...")

        time.sleep(THROTTLE_SECONDS)

    attempted = len(all_frames) + len(failed_symbols)
    fail_ratio = len(failed_symbols) / attempted if attempted else 1.0

    if stopped_early_reason and not all_frames:
        # Dừng sớm mà 0 mã lấy được -> API sập hoàn toàn, không ghi file rỗng
        # đè lên dữ liệu tốt của lần chạy trước.
        raise RuntimeError(
            f"{stopped_early_reason} Và 0 mã lấy được giá thành công trước khi dừng "
            "-> dừng hẳn, không ghi CSV."
        )

    if fail_ratio > MAX_FAILED_SYMBOLS_RATIO:
        if stopped_early_reason:
            # Dừng sớm chủ động (circuit breaker/ngân sách thời gian) -> tỉ lệ
            # lỗi cao trong phần ĐÃ THỬ là chuyện đương nhiên, không raise.
            # Vẫn ghi phần dữ liệu tốt đã lấy được; phần còn thiếu sẽ được vá
            # ở các lần chạy sau nhờ LOOKBACK_DAYS + xoay vòng thứ tự mã.
            log.warning(
                f"Tỉ lệ lỗi {fail_ratio:.0%} trong {attempted} mã đã thử vượt ngưỡng "
                f"{MAX_FAILED_SYMBOLS_RATIO:.0%}, nhưng do dừng sớm chủ động nên vẫn "
                f"ghi {len(all_frames)} mã lấy được thành công."
            )
        else:
            # Chạy hết TOÀN BỘ danh sách bình thường mà tỉ lệ lỗi vẫn cao ->
            # đúng là API có vấn đề diện rộng, giữ nguyên guard cũ.
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
