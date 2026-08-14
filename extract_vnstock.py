"""
extract_company_profile.py
----------------------------
File RIÊNG, KHÔNG chạy chung với extract_vnstock.py (dim_stock + giá) — vì
company_profile ít đổi, chỉ cần full refresh HÀNG TUẦN, không phải mỗi ngày
(xem comment "chạy hàng tuần" trong build_duckdb_file.py::load_company_profile()
và pages/2_Chi_tiet_ma.py). File này được chạy bởi workflow RIÊNG
weekly_company_profile.yml (lịch thưa hơn daily_etl.yml, VD 1 lần/tuần).

⚠️ SỰ CỐ 2026-08-14 (2) — "Rate Limit Exceeded, Process terminated" từ
   vnstock: gói Community giới hạn cứng 60 request/phút, vượt ngưỡng là
   THOÁT LUÔN CẢ TIẾN TRÌNH (nghi SystemExit, không bị `except Exception`
   bắt được). File này vốn đã throttle 3.5s/mã (~17/phút) nên khó chạm
   ngưỡng hơn extract_vnstock.py, nhưng vẫn thêm RateLimiter + bắt
   BaseException để phòng thủ kép (retry dồn dập có thể đẩy tốc độ request
   thực tế cao hơn throttle danh nghĩa).

⚠️ SỰ CỐ 2026-08-14 — "Extract company profile" từng bị gộp nhầm vào chung
   job với daily_etl.yml (chạy nối tiếp sau "Extract từ vnstock" trong CÙNG
   1 job có timeout-minutes: 60) -> LUÔN LUÔN bị cancel, không liên quan gì
   tới việc API có lỗi hay không: với ~1749 mã x THROTTLE_SECONDS=3.5s, riêng
   bước này cần tối thiểu ~102 PHÚT dù API chạy hoàn hảo không lỗi 1 request
   nào, trong khi job daily chỉ còn lại vài chục phút sau khi "Extract từ
   vnstock" đã chạy xong. Fix: tách hẳn ra workflow tuần riêng
   (weekly_company_profile.yml) với timeout-minutes rộng rãi hơn nhiều,
   đúng như thiết kế ban đầu của docstring này.

Nguồn dữ liệu: vnstock v4.0.5, class Company của nguồn VCI. Đối chiếu trực
tiếp với source code vnstock/explorer/vci/company.py (KHÔNG đoán tên cột từ
tài liệu cũ):

  - Company(symbol=<mã>, show_log=False).overview()
      -> 1 dòng / 1 mã. KHÔNG nhận tham số source (giống Quote, Listing —
         class này đã gắn sẵn nguồn VCI, xem inspect.signature(Company.__init__)
         đã kiểm tra: (self, symbol, random_agent, to_df, show_log) — không
         có source, truyền vào sẽ TypeError y hệt lỗi Quote gặp phải trước đó).
      -> Cột trả về do chính overview() tự đổi tên 1 phần (camelCase ->
         snake_case + vài rename cố định: ticker->symbol, profile->
         company_profile, vi_organ_name->organ_name...), NHƯNG các cột còn
         lại (founded_date, listing_date, charter_capital,
         number_of_employees, business_model...) phụ thuộc JSON THẬT trả về
         từ API, KHÔNG thể xác nhận 100% mà không gọi API thật.

⚠️ QUAN TRỌNG — làm việc này SAU KHI chạy file này lần đầu:
   Mở company_profile.csv vừa tạo ra, đối chiếu danh sách cột thật với các
   cột mà dbt_vnstock/models/staging/vnstock/stg_vnstock__company_profile.sql
   đang SELECT (business_model, founded_date, listing_date, charter_capital,
   number_of_employees). Nếu tên cột thật khác đi, sửa lại đúng theo CSV thật
   trong file .sql đó — comment sẵn có trong file .sql cũng đã cảnh báo y hệt
   điều này ("cần đối chiếu lại với dữ liệu thật sau khi chạy
   extract_company_profile.py lần đầu").

Yêu cầu: pip install vnstock==4.0.5 pandas tenacity (đã pin trong requirements.txt)
"""

import collections
import concurrent.futures
import logging
import time
from datetime import date

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from vnstock.explorer.vci.company import Company
from vnstock.explorer.vci.listing import Listing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

COMPANY_PROFILE_CSV = "company_profile.csv"

# Việc throttle giờ do RateLimiter (bên dưới) đảm nhiệm, tính theo request
# thật thay vì sleep cố định theo số mã — xem class RateLimiter. Với giới
# hạn 45 request/phút (biên an toàn dưới 60), ~1749 mã cần TỐI THIỂU ~39
# phút chỉ riêng phần pacing — đây vẫn là lý do chính khiến file này BẮT
# BUỘC phải chạy trong workflow riêng có timeout-minutes rộng rãi (xem
# weekly_company_profile.yml), không thể nhét chung vào job daily_etl.yml.

# Ngưỡng lỗi tối đa cho phép trước khi coi là API lỗi diện rộng — giống
# MAX_FAILED_SYMBOLS_RATIO trong extract_vnstock.py.
MAX_FAILED_SYMBOLS_RATIO = 0.2

# ── Phòng thủ khi VCI lỗi diện rộng (giống extract_vnstock.py) ─────────────
# Timeout tầng ứng dụng cho MỖI LẦN gọi Company.overview(), không phụ thuộc
# timeout mặc định của thư viện HTTP bên trong vnstock.
HARD_CALL_TIMEOUT_SECONDS = 15

# Nếu có ngần này mã LIÊN TIẾP lỗi, coi như API đang sập diện rộng -> dừng
# sớm, vẫn ghi phần đã lấy được thay vì cố chạy hết rồi bị cancel mất trắng.
CIRCUIT_BREAKER_CONSECUTIVE_FAILURES = 25

# Ngân sách thời gian tối đa cho bước lấy company profile. Đặt theo
# timeout-minutes của weekly_company_profile.yml trừ hao cho các bước
# Load/dbt run/dbt test/upload phía sau (xem workflow đó).
MAX_RUNTIME_SECONDS = 150 * 60


class RateLimiter:
    """Xem giải thích chi tiết ở extract_vnstock.py::RateLimiter — sliding
    window tính CHUNG cho mọi lần gọi API thật, kể cả các lần retry."""

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


# Giới hạn thật của vnstock (Community) là 60 request/phút — đặt 45 làm biên
# an toàn, giống extract_vnstock.py.
rate_limiter = RateLimiter(max_calls=45, period_seconds=60)


def _is_vnstock_hard_rate_limit(exc: BaseException) -> bool:
    msg = str(exc)
    return "Rate Limit" in msg or "rate limit" in msg.lower()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_all_symbols() -> list[str]:
    """Lấy danh sách mã cổ phiếu thường (loại chứng quyền/trái phiếu/ETF)."""
    rate_limiter.wait()
    log.info("Đang lấy danh sách mã từ Listing().symbols_by_exchange()...")
    df = Listing().symbols_by_exchange(lang="vi")
    if "type" in df.columns:
        df = df[df["type"] == "STOCK"]
    symbols = df["symbol"].tolist()
    log.info(f"Có {len(symbols)} mã cần lấy company profile")
    return symbols


def _rotate_symbols(symbols: list[str]) -> list[str]:
    """Xoay vòng thứ tự mã theo tuần (giống _rotate_symbols trong
    extract_vnstock.py) — phòng khi vẫn phải dừng sớm vì circuit breaker/hết
    ngân sách thời gian, nhóm mã bị bỏ sót sẽ không phải lúc nào cũng là cùng
    1 nhóm cuối danh sách."""
    if not symbols:
        return symbols
    offset = (date.today().toordinal() // 7) % len(symbols)
    return symbols[offset:] + symbols[:offset]


def _call_with_hard_timeout(fn, timeout_seconds: float):
    """Xem giải thích chi tiết ở extract_vnstock.py::_call_with_hard_timeout —
    ép timeout tầng ứng dụng, không phụ thuộc timeout mặc định của thư viện."""
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


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_overview_one_symbol(symbol: str) -> pd.DataFrame:
    """Lấy company overview cho ĐÚNG 1 mã. Retry 2 lần (giảm từ 3), mỗi lần bị
    chặn bởi HARD_CALL_TIMEOUT_SECONDS thay vì chờ timeout mặc định của thư viện."""
    rate_limiter.wait()
    return _call_with_hard_timeout(
        lambda: Company(symbol=symbol, show_log=False).overview(),
        HARD_CALL_TIMEOUT_SECONDS,
    )


def build_company_profile(symbols: list[str]) -> pd.DataFrame:
    """Lặp qua từng mã, gọi Company.overview(), gộp lại thành 1 bảng.

    Có circuit breaker + ngân sách thời gian giống build_fact_price_daily()
    trong extract_vnstock.py — nếu phải dừng sớm, vẫn ghi ra phần dữ liệu đã
    lấy được thành công thay vì mất trắng."""
    symbols = _rotate_symbols(symbols)

    all_frames = []
    failed_symbols = []
    consecutive_failures = 0
    stopped_early_reason = None
    loop_start = time.monotonic()

    for i, symbol in enumerate(symbols, start=1):
        elapsed = time.monotonic() - loop_start
        if elapsed > MAX_RUNTIME_SECONDS:
            stopped_early_reason = (
                f"Đã chạy {elapsed:.0f}s, vượt ngân sách {MAX_RUNTIME_SECONDS}s "
                f"-> dừng sớm ở mã {i}/{len(symbols)} để các bước sau (Load, dbt) "
                "còn kịp chạy trong timeout-minutes của job."
            )
            log.warning(stopped_early_reason)
            break

        try:
            df = _fetch_overview_one_symbol(symbol)
            if df is not None and len(df) > 0:
                all_frames.append(df)
            consecutive_failures = 0
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            # Bắt cả BaseException — xem giải thích ở extract_vnstock.py,
            # cùng lỗi "Rate Limit Exceeded ... Process terminated" của
            # vnstock. 1 mã lỗi không được làm chết cả batch.
            failed_symbols.append(symbol)
            consecutive_failures += 1
            if _is_vnstock_hard_rate_limit(e):
                log.error(
                    f"[{i}/{len(symbols)}] Chạm giới hạn rate-limit CỨNG của vnstock "
                    f"dù đã qua RateLimiter — dừng ngay lập tức: {e}"
                )
                stopped_early_reason = (
                    f"Chạm rate-limit cứng của vnstock ở mã {i}/{len(symbols)} — dừng "
                    "sớm, phần còn lại sẽ được vá ở lần chạy tuần sau."
                )
                break
            log.warning(f"[{i}/{len(symbols)}] Lỗi khi lấy company profile {symbol}: {e}")

            if consecutive_failures >= CIRCUIT_BREAKER_CONSECUTIVE_FAILURES:
                stopped_early_reason = (
                    f"{consecutive_failures} mã liên tiếp lỗi -> nghi ngờ API đang lỗi "
                    f"diện rộng/sập, dừng sớm ở mã {i}/{len(symbols)} thay vì tiếp tục "
                    "chạy hết danh sách."
                )
                log.error(stopped_early_reason)
                break

        if i % 100 == 0:
            log.info(f"Tiến độ: {i}/{len(symbols)} mã đã xử lý...")

    attempted = len(all_frames) + len(failed_symbols)
    fail_ratio = len(failed_symbols) / attempted if attempted else 1.0

    if stopped_early_reason and not all_frames:
        raise RuntimeError(
            f"{stopped_early_reason} Và 0 mã lấy được company profile thành công "
            "trước khi dừng -> dừng hẳn, không ghi CSV."
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
                f"{len(failed_symbols)}/{attempted} mã ({fail_ratio:.0%}) lấy company profile "
                f"thất bại — vượt ngưỡng {MAX_FAILED_SYMBOLS_RATIO:.0%}, nghi ngờ API đang lỗi "
                "diện rộng. Dừng lại, không ghi file dữ liệu thiếu sót."
            )
    elif failed_symbols:
        log.warning(
            f"Có {len(failed_symbols)} mã lấy company profile thất bại (đã bỏ qua): "
            f"{failed_symbols}"
        )

    if not all_frames:
        return pd.DataFrame()

    # union_by_name=True ở phía đọc CSV (build_duckdb_file.py) đã xử lý việc
    # các dòng có thể thiếu/thừa cột khác nhau -> ở đây chỉ cần concat thẳng,
    # không ép cứng danh sách cột (KHÔNG đoán tên cột, xem cảnh báo ở docstring).
    company_profile = pd.concat(all_frames, ignore_index=True)
    return company_profile


def main():
    symbols = _fetch_all_symbols()

    if not symbols:
        raise RuntimeError("Không lấy được danh sách mã — dừng lại, không ghi CSV rỗng.")

    company_profile = build_company_profile(symbols)

    if len(company_profile) == 0:
        raise RuntimeError(
            "build_company_profile() trả về 0 dòng — dừng lại, không ghi CSV rỗng "
            "(build_duckdb_file.py sẽ bỏ qua nếu không thấy file này, nên KHÔNG ghi "
            "file rỗng đè lên dữ liệu tốt của tuần trước nếu có)."
        )

    company_profile.to_csv(COMPANY_PROFILE_CSV, index=False)
    log.info(f"Đã ghi {COMPANY_PROFILE_CSV}: {len(company_profile)} dòng")
    log.info(
        f"Các cột thật sự có trong CSV: {list(company_profile.columns)} "
        "— đối chiếu với stg_vnstock__company_profile.sql, xem cảnh báo ở đầu file này."
    )


if __name__ == "__main__":
    main()
