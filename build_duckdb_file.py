"""
build_duckdb_file.py
----------------------
Giai đoạn 2 (Load) trong pipeline: nạp dim_stock.csv + fact_price_daily.csv
(output của extract_vnstock.py) và/hoặc company_profile.csv (output của
extract_company_profile.py) vào 1 FILE DuckDB TĨNH (không phải MotherDuck).

Đổi kiến trúc: MotherDuck free tier giới hạn 10 giờ compute/tháng (chính sách
mới, khác lúc chọn kiến trúc ban đầu) -> rủi ro demo CV bị gián đoạn giữa tháng.
File .duckdb tĩnh publish qua GitHub Release không bị giới hạn compute.

File .duckdb được publish/tải lại qua GitHub Release (tag "latest-data") ở mỗi
lần chạy workflow, vì mỗi lần GitHub Actions chạy là 1 máy ảo mới, không tự
giữ lại file giữa các lần chạy.

Áp dụng theo skill motherduck-load-data (vẫn dùng được dù đổi sang local file
vì nguyên tắc CTAS/append/validate giống hệt DuckDB thường):
  - CTAS cho lần load đầu, INSERT...BY NAME cho append
  - Landing vào bảng raw tối giản, chưa transform (dbt lo phần đó)
  - Validate row count ngay sau khi load

⚠️ FIX 2026-08-14: file này được dùng chung bởi 2 workflow riêng biệt —
   daily_etl.yml (chạy extract_vnstock.py -> có dim_stock.csv +
   fact_price_daily.csv, KHÔNG có company_profile.csv hầu hết các ngày) và
   weekly_company_profile.yml (chạy extract_company_profile.py -> chỉ có
   company_profile.csv, KHÔNG có dim_stock.csv/fact_price_daily.csv).
   load_dim_stock() và load_fact_price_daily() giờ có guard bỏ qua nếu thiếu
   CSV tương ứng (giống load_company_profile() đã có sẵn từ trước), để file
   này chạy được độc lập trong cả 2 workflow mà không cần 2 script Load riêng.

Yêu cầu: pip install duckdb
Không cần token/secret nào — đây là lợi ích phụ của việc bỏ MotherDuck.
"""

import os
import logging
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DUCKDB_FILE = "vnstock.duckdb"
DIM_STOCK_CSV = "dim_stock.csv"
FACT_PRICE_CSV = "fact_price_daily.csv"


def get_connection() -> duckdb.DuckDBPyConnection:
    # DUCKDB_FILE_PATH cho phép CI dùng đường dẫn tuyệt đối (tránh nhầm lẫn cwd
    # giữa bước Python và bước dbt); mặc định "vnstock.duckdb" cho chạy local.
    db_path = os.environ.get("DUCKDB_FILE_PATH", DUCKDB_FILE)
    # File này có thể đã tồn tại (tải về từ GitHub Release ở bước trước trong
    # workflow) hoặc chưa (lần chạy đầu tiên) -> DuckDB tự tạo mới nếu chưa có.
    return duckdb.connect(db_path)


def bootstrap_schema(con: duckdb.DuckDBPyConnection):
    """Tạo schema nếu chưa có (database đã tạo/USE trong get_connection) - idempotent"""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")


def load_dim_stock(con: duckdb.DuckDBPyConnection):
    """dim_stock nhỏ, ít đổi -> full refresh mỗi lần chạy bằng CTAS"""

    # Fix 2026-08-14: file này có thể chạy trong weekly_company_profile.yml
    # (chỉ có company_profile.csv, KHÔNG chạy extract_vnstock.py) -> bỏ qua
    # thay vì lỗi nếu không có dim_stock.csv trong lần chạy này.
    if not os.path.exists(DIM_STOCK_CSV):
        log.info(
            f"Không có {DIM_STOCK_CSV} trong lần chạy này (bình thường nếu đây là "
            "workflow weekly company-profile-only) — bỏ qua."
        )
        return

    log.info("Đang load dim_stock (full refresh)...")

    # Fix #2: kiểm tra CSV có dữ liệu TRƯỚC khi REPLACE, tránh xoá sạch dữ liệu
    # tốt đang có nếu extract lần này trả về rỗng do lỗi API tạm thời.
    csv_row_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv('{DIM_STOCK_CSV}', union_by_name = true)"
    ).fetchone()[0]
    if csv_row_count == 0:
        raise RuntimeError(
            f"{DIM_STOCK_CSV} rỗng — dừng lại, KHÔNG ghi đè raw.dim_stock để giữ "
            "dữ liệu tốt của lần chạy trước. Kiểm tra lại bước Extract."
        )

    con.execute(f"""
        CREATE OR REPLACE TABLE raw.dim_stock AS
        SELECT * FROM read_csv('{DIM_STOCK_CSV}', union_by_name = true)
    """)
    row_count = con.execute("SELECT COUNT(*) FROM raw.dim_stock").fetchone()[0]
    log.info(f"raw.dim_stock: {row_count} dòng")


def load_fact_price_daily(con: duckdb.DuckDBPyConnection):
    """fact_price_daily append theo ngày -> tạo bảng nếu chưa có, rồi INSERT"""

    # Fix 2026-08-14: giống load_dim_stock() — bỏ qua nếu chạy trong workflow
    # weekly company-profile-only, không có fact_price_daily.csv.
    if not os.path.exists(FACT_PRICE_CSV):
        log.info(
            f"Không có {FACT_PRICE_CSV} trong lần chạy này (bình thường nếu đây là "
            "workflow weekly company-profile-only) — bỏ qua."
        )
        return

    log.info("Đang load fact_price_daily (append)...")

    # Tạo bảng lần đầu nếu chưa tồn tại, dùng đúng schema từ CSV
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.fact_price_daily AS
        SELECT * FROM read_csv('{FACT_PRICE_CSV}', union_by_name = true) WHERE 1=0
    """)

    before_count = con.execute("SELECT COUNT(*) FROM raw.fact_price_daily").fetchone()[0]

    # Fix #3: dùng "INSERT ... BY NAME" thay vì match theo vị trí cột — nếu vnstock
    # đổi thứ tự cột trả về (đã từng xảy ra ở bản v4), dữ liệu sẽ chèn sai cột
    # (VD: volume vào cột close) mà không báo lỗi gì nếu match theo vị trí.
    con.execute(f"""
        INSERT INTO raw.fact_price_daily BY NAME
        SELECT * FROM read_csv('{FACT_PRICE_CSV}', union_by_name = true)
    """)

    after_count = con.execute("SELECT COUNT(*) FROM raw.fact_price_daily").fetchone()[0]
    inserted = after_count - before_count
    log.info(f"raw.fact_price_daily: +{inserted} dòng mới (tổng {after_count})")

    if inserted == 0:
        log.warning("Không có dòng nào được thêm — kiểm tra lại fact_price_daily.csv")


def validate(con: duckdb.DuckDBPyConnection):
    """Kiểm tra nhanh sau khi load — không thay cho dbt test ở bước sau, chỉ chặn lỗi thô"""

    tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'").fetchall()}
    if "fact_price_daily" not in tables:
        log.info("raw.fact_price_daily chưa tồn tại trong lần chạy này — bỏ qua validate (bình thường ở workflow weekly company-profile-only).")
        return

    log.info("Đang validate dữ liệu vừa load...")

    null_symbols = con.execute(
        "SELECT COUNT(*) FROM raw.fact_price_daily WHERE symbol IS NULL"
    ).fetchone()[0]
    if null_symbols > 0:
        log.warning(f"Có {null_symbols} dòng thiếu symbol trong fact_price_daily")

    negative_price = con.execute(
        "SELECT COUNT(*) FROM raw.fact_price_daily WHERE close < 0"
    ).fetchone()[0]
    if negative_price > 0:
        log.warning(f"Có {negative_price} dòng giá close âm — dữ liệu bất thường")

    distinct_symbols = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM raw.fact_price_daily"
    ).fetchone()[0]
    log.info(f"Số mã duy nhất trong fact_price_daily: {distinct_symbols}")


def load_company_profile(con: duckdb.DuckDBPyConnection):
    """company_profile ít đổi -> full refresh, chạy hàng tuần qua weekly_company_profile.yml"""
    if not os.path.exists("company_profile.csv"):
        log.info("Không có company_profile.csv trong lần chạy này (bình thường nếu không phải ngày chạy weekly) — bỏ qua.")
        return

    csv_row_count = con.execute(
        "SELECT COUNT(*) FROM read_csv('company_profile.csv', union_by_name = true)"
    ).fetchone()[0]
    if csv_row_count == 0:
        log.warning("company_profile.csv rỗng — bỏ qua, KHÔNG ghi đè raw.company_profile")
        return

    log.info("Đang load company_profile (full refresh)...")
    con.execute("""
        CREATE OR REPLACE TABLE raw.company_profile AS
        SELECT * FROM read_csv('company_profile.csv', union_by_name = true)
    """)
    row_count = con.execute("SELECT COUNT(*) FROM raw.company_profile").fetchone()[0]
    log.info(f"raw.company_profile: {row_count} dòng")


def main():
    con = get_connection()
    bootstrap_schema(con)
    load_dim_stock(con)
    load_fact_price_daily(con)
    load_company_profile(con)
    validate(con)
    con.close()
    log.info("Load hoàn tất.")


if __name__ == "__main__":
    main()
