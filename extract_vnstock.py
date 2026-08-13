"""
extract_company_profile.py
----------------------------
File RIÊNG, KHÔNG chạy chung với extract_vnstock.py (dim_stock + giá) — vì
company_profile ít đổi, chỉ cần full refresh HÀNG TUẦN, không phải mỗi ngày
(xem comment "chạy hàng tuần" trong build_duckdb_file.py::load_company_profile()
và pages/2_Chi_tiet_ma.py). Chạy file này trong 1 workflow/step riêng, lịch
thưa hơn daily_etl.yml (VD 1 lần/tuần) để không tốn thời gian + tránh gọi API
quá nhiều lần không cần thiết.

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

import logging
import time

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from vnstock.explorer.vci.company import Company
from vnstock.explorer.vci.listing import Listing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

COMPANY_PROFILE_CSV = "company_profile.csv"

# Giới hạn API guest của VCI: 20 request/phút -> tối thiểu 3s/lần để không bị
# chặn (đã gặp "Rate Limit Exceeded" thực tế với 0.3s cũ, xem lịch sử debug).
# Đặt dư ra 3.5s cho an toàn.
THROTTLE_SECONDS = 3.5

# Ngưỡng lỗi tối đa cho phép trước khi coi là API lỗi diện rộng — giống
# MAX_FAILED_SYMBOLS_RATIO trong extract_vnstock.py.
MAX_FAILED_SYMBOLS_RATIO = 0.2


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20))
def _fetch_all_symbols() -> list[str]:
    """Lấy danh sách mã cổ phiếu thường (loại chứng quyền/trái phiếu/ETF)."""
    log.info("Đang lấy danh sách mã từ Listing().symbols_by_exchange()...")
    df = Listing().symbols_by_exchange(lang="vi")
    if "type" in df.columns:
        df = df[df["type"] == "STOCK"]
    symbols = df["symbol"].tolist()
    log.info(f"Có {len(symbols)} mã cần lấy company profile")
    return symbols


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=5, max=30))
def _fetch_overview_one_symbol(symbol: str) -> pd.DataFrame:
    """Lấy company overview cho ĐÚNG 1 mã. Retry vì API hay timeout/lỗi mạng tạm thời,
    hoặc bị rate-limit (20 request/phút với tài khoản Guest) -> wait tối thiểu 5s."""
    return Company(symbol=symbol, show_log=False).overview()


def build_company_profile(symbols: list[str]) -> pd.DataFrame:
    """Lặp qua từng mã, gọi Company.overview(), gộp lại thành 1 bảng."""
    all_frames = []
    failed_symbols = []

    for i, symbol in enumerate(symbols, start=1):
        try:
            df = _fetch_overview_one_symbol(symbol)
            if df is not None and len(df) > 0:
                all_frames.append(df)
        except Exception as e:
            # 1 mã lỗi (VD mã mới niêm yết chưa có đủ hồ sơ công ty) không
            # được làm chết cả batch — log lại, đi tiếp, tổng hợp ở cuối.
            failed_symbols.append(symbol)
            log.warning(f"[{i}/{len(symbols)}] Lỗi khi lấy company profile {symbol}: {e}")

        if i % 100 == 0:
            log.info(f"Tiến độ: {i}/{len(symbols)} mã đã xử lý...")

        time.sleep(THROTTLE_SECONDS)

    fail_ratio = len(failed_symbols) / len(symbols) if symbols else 0
    if fail_ratio > MAX_FAILED_SYMBOLS_RATIO:
        raise RuntimeError(
            f"{len(failed_symbols)}/{len(symbols)} mã ({fail_ratio:.0%}) lấy company profile "
            f"thất bại — vượt ngưỡng {MAX_FAILED_SYMBOLS_RATIO:.0%}, nghi ngờ API đang lỗi diện "
            "rộng. Dừng lại, không ghi file dữ liệu thiếu sót."
        )
    if failed_symbols:
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
