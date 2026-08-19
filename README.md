# 📈 VN Stock Analytics Platform

**End-to-end data platform** cho thị trường chứng khoán Việt Nam: tự động thu thập dữ liệu từ [vnstock](https://github.com/thinh-vu/vnstock) → biến đổi bằng **dbt** thành star schema trong **DuckDB** → phục vụ qua dashboard **Streamlit** với watchlist cá nhân có đăng nhập. Toàn bộ pipeline chạy tự động, không server thường trực, chi phí vận hành **$0**.

<p align="center">
  <img src="docs/screenshots/tong_quan.png" alt="Dashboard Tổng quan thị trường" width="800">
  <br><em>Trang Tổng quan: KPI mã tăng/giảm/đứng giá + hiệu suất theo ngành</em>
</p>

<p align="center">
  <img src="docs/screenshots/tong_quan_top10.png" alt="Top 10 mã tăng/giảm mạnh nhất" width="800">
  <br><em>Top 10 mã tăng/giảm mạnh nhất trong ngày</em>
</p>

<p align="center">
  <img src="docs/screenshots/chi_tiet_ma.png" alt="Chi tiết mã cổ phiếu" width="800">
  <br><em>Trang Chi tiết mã: giá đóng cửa/tham chiếu/trần/sàn, cao-thấp 52 tuần, thông tin công ty</em>
</p>

<p align="center">
  <img src="docs/screenshots/chi_tiet_ma_chart.png" alt="Biểu đồ nến giá cổ phiếu" width="800">
  <br><em>Biểu đồ nến — quy ước màu chuẩn Việt Nam (tăng = đỏ, giảm = xanh)</em>
</p>

<p align="center">
  <img src="docs/screenshots/chi_tiet_ma_volume.png" alt="Biểu đồ khối lượng giao dịch" width="800">
  <br><em>Biểu đồ khối lượng giao dịch, đồng bộ màu với biểu đồ nến</em>
</p>

<p align="center">
  <img src="docs/screenshots/so_sanh_nganh.png" alt="So sánh xu hướng theo ngành" width="800">
  <br><em>Trang So sánh ngành: % thay đổi giá trung bình theo ngành</em>
</p>

<p align="center">
  <img src="docs/screenshots/so_sanh_nganh_trend.png" alt="Xu hướng ngành theo thời gian" width="800">
  <br><em>Xu hướng % thay đổi trung bình theo ngành trong 30 ngày gần nhất</em>
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white">
  <img alt="dbt" src="https://img.shields.io/badge/dbt-DuckDB-FF694B?logo=dbt&logoColor=white">
  <img alt="Streamlit" src="https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white">
  <img alt="GitHub Actions" src="https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white">
  <img alt="License" src="https://img.shields.io/badge/license-MIT-lightgrey">
</p>

---

## Mục lục

- [Vì sao dự án này đáng chú ý](#vì-sao-dự-án-này-đáng-chú-ý)
- [Kiến trúc tổng thể](#kiến-trúc-tổng-thể)
- [Data model (star schema)](#data-model-star-schema)
- [Các bài toán kỹ thuật đã xử lý](#các-bài-toán-kỹ-thuật-đã-xử-lý)
- [Tech stack](#tech-stack)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Chạy dự án](#chạy-dự-án)
- [CI/CD](#cicd)
- [Roadmap](#roadmap)

---

## Vì sao dự án này đáng chú ý

Đây không phải một demo "happy path" — pipeline được thiết kế để **tự phục hồi khi API bên thứ 3 (vnstock/VCI) không ổn định**, dữ liệu về bị lệch cột, hoặc job bị GitHub Actions hard-kill giữa chừng. Một số quyết định kỹ thuật đáng chú ý:

- **Rate limiting theo sliding window + circuit breaker**: tự phát hiện khi API bị chặn diện rộng (25 mã lỗi liên tiếp) và dừng sớm thay vì cắm đầu chạy hết ~1700+ mã rồi bị timeout mất trắng dữ liệu.
- **Bắt cả `BaseException`, không chỉ `Exception`**: `SystemExit` do thư viện `vnstock` tự thoát tiến trình khi rate-limit không bị `except Exception` bắt được — một lỗi thực tế đã điều tra và fix.
- **Backfill thông minh**: mã cổ phiếu mới chỉ có trong dữ liệu gần đây bị giới hạn lịch sử vĩnh viễn nếu dùng lookback cố định — pipeline tự phát hiện mã mới (so với dữ liệu đã có) và backfill 10 năm thay vì 10 ngày.
- **Idempotent load + schema evolution**: `INSERT ... BY NAME` (không match theo vị trí cột — vnstock từng đổi thứ tự cột giữa các version) và dbt `on_schema_change='append_new_columns'` để không âm thầm rớt cột mới khi thêm field vào model incremental.
- **Tách OLAP/OLTP đúng bản chất**: dữ liệu phân tích (đọc nhiều, ghi 1 lần/ngày) nằm ở file DuckDB tĩnh publish qua GitHub Release; watchlist/auth (ghi liên tục, nhiều user) nằm ở Postgres (Supabase) — tránh giới hạn compute-hours của giải pháp cloud warehouse ban đầu (MotherDuck free tier).
- **Chuẩn hoá đơn vị dữ liệu xuyên nguồn**: hai API con của vnstock trả về đơn vị tiền tệ khác nhau (nghìn đồng vs. đồng), được document rõ và xử lý nhất quán từ dbt tới tầng hiển thị.
- **Localize đúng ngữ cảnh**: quy ước màu tăng/giảm giá theo chuẩn bảng giá chứng khoán Việt Nam (đỏ = tăng, xanh = giảm) — ngược với mặc định phương Tây của thư viện chart.
- **CI/CD hai lịch chạy tách biệt** (daily cho giá, weekly cho hồ sơ công ty) dùng chung `concurrency group` để tránh 2 job ghi đè cùng lúc lên 1 file dữ liệu.

## Kiến trúc tổng thể

```mermaid
flowchart LR
    subgraph EXT["Nguồn dữ liệu"]
        VCI[("vnstock API<br/>(nguồn VCI)")]
    end

    subgraph ETL["GitHub Actions — Extract & Load"]
        E1["extract_vnstock.py<br/>(daily)"] --> L["build_duckdb_file.py<br/>CTAS / append BY NAME"]
        E2["extract_company_profile.py<br/>(weekly)"] --> L
    end

    subgraph DBT["dbt (transform)"]
        STG["staging models<br/>clean + dedup + cast"] --> MART["marts: dim_stock, dim_sector,<br/>dim_date, fct_price_daily"]
    end

    subgraph STORE["Lưu trữ"]
        REL[("GitHub Release<br/>vnstock.duckdb (file tĩnh)")]
        PG[("Supabase Postgres<br/>users / watchlist")]
    end

    subgraph APP["Streamlit App"]
        UI["Tổng quan · Chi tiết mã ·<br/>So sánh ngành · Watchlist"]
    end

    VCI --> E1
    VCI --> E2
    L --> REL
    REL --> STG
    STG --> MART
    MART --> REL
    REL -- "tải mỗi giờ, read-only" --> UI
    PG <--> UI
```

**Luồng dữ liệu:** Extract (Python + vnstock) → Load thô vào DuckDB (CTAS/append) → Transform bằng dbt (staging → marts, star schema) → Publish file `.duckdb` lên GitHub Release → Streamlit tải về và query embedded (không tốn compute cloud) → Watchlist/auth ghi trực tiếp vào Postgres.

## Data model (star schema)

```mermaid
erDiagram
    dim_stock ||--o{ fct_price_daily : "symbol"
    dim_sector ||--o{ dim_stock : "sector_name"
    dim_date ||--o{ fct_price_daily : "date_key"

    dim_stock {
        varchar stock_key PK
        varchar symbol
        varchar company_name
        varchar exchange
        varchar sector_name
        numeric market_cap
        numeric issue_share
        numeric foreigner_percentage
    }
    dim_sector {
        varchar sector_key PK
        varchar sector_name
    }
    dim_date {
        date date_key PK
        int year
        int quarter
        bool is_trading_day
    }
    fct_price_daily {
        varchar symbol FK
        date date_key FK
        numeric close_price
        numeric reference_price
        numeric ceiling_price
        numeric floor_price
        bigint volume
        numeric price_change_pct
    }
```

`fct_price_daily` được build **incremental** trong dbt (chỉ transform dữ liệu mới), có test tự động cho `unique`, `not_null` và `relationships` (referential integrity với `dim_stock`) — chạy trong bước `dbt test` như một **quality gate** trước khi publish dữ liệu mới ra production.

## Các bài toán kỹ thuật đã xử lý

| Vấn đề gặp phải | Giải pháp |
|---|---|
| Thư viện `vnstock` tự `SystemExit` khi chạm rate-limit, không bắt được bằng `except Exception` | Bắt `BaseException` có chọn lọc + `RateLimiter` sliding-window chủ động tránh chạm ngưỡng |
| Job "Extract company profile" luôn bị GitHub Actions cancel vì chạy chung job với ETL giá, không đủ thời gian (~102 phút tối thiểu cho 1749 mã) | Tách hẳn thành workflow tuần riêng (`weekly_company_profile.yml`) với `timeout-minutes` rộng hơn |
| API trả dữ liệu đổi thứ tự cột giữa các version → insert sai cột mà không báo lỗi | Dùng `INSERT ... BY NAME` thay vì match theo vị trí |
| dbt incremental âm thầm bỏ qua cột mới thêm vào sau khi bảng đích đã tồn tại | `on_schema_change='append_new_columns'` |
| Mã cổ phiếu mới niêm yết bị giới hạn vĩnh viễn chỉ 10 ngày lịch sử giá | So sánh với dữ liệu đã có trong `.duckdb`, mã mới được backfill 10 năm |
| MotherDuck free tier giới hạn 10 giờ compute/tháng, rủi ro gián đoạn demo | Chuyển sang file `.duckdb` tĩnh publish qua GitHub Release, query embedded — không giới hạn compute |
| 2 nguồn API trả đơn vị tiền tệ khác nhau (nghìn đồng vs. đồng) gây sai số hiển thị | Document rõ ràng + 2 hàm format riêng biệt (`format_vnd` vs `format_vnd_full`) |
| App crash toàn bộ nếu chưa cấu hình secrets Supabase (kể cả các trang không cần đăng nhập) | Bọc bootstrap auth trong `try/except`, ẩn phần đăng nhập thay vì sập cả app |

## Tech stack

| Layer | Công nghệ |
|---|---|
| Data source | [`vnstock`](https://pypi.org/project/vnstock/) (nguồn VCI) |
| Extract | Python, `tenacity` (retry), custom rate limiter & circuit breaker |
| Load | DuckDB (file tĩnh) |
| Transform | dbt-core + `dbt-duckdb` (staging → marts, tests, seeds) |
| Orchestration | GitHub Actions (cron: daily + weekly) |
| Distribution | GitHub Releases (không cần server lưu trữ) |
| App layer | Streamlit, Plotly |
| Auth & OLTP | Supabase (Postgres), `streamlit-authenticator` |
| CI/CD | GitHub Actions, `dbt test` làm data quality gate |

## Cấu trúc thư mục

```
.
├── extract_vnstock.py              # Extract: dim_stock + giá OHLCV (chạy hàng ngày)
├── extract_company_profile.py      # Extract: hồ sơ công ty (chạy hàng tuần)
├── build_duckdb_file.py            # Load: CSV → raw tables trong file .duckdb
├── requirements.txt
├── dbt_vnstock/
│   ├── models/staging/vnstock/     # Clean, cast, dedup
│   ├── models/marts/core/          # Star schema: dim_stock, dim_sector, dim_date, fct_price_daily
│   ├── seeds/vn_public_holidays.csv
│   └── profiles.yml
├── streamlit_app/
│   ├── app.py                      # Đăng nhập / đăng ký
│   ├── db.py                       # Kết nối DuckDB (phân tích) + Postgres (watchlist)
│   ├── theme.py                    # Design system dùng chung
│   └── pages/
│       ├── 1_Tong_quan.py
│       ├── 2_Chi_tiet_ma.py
│       ├── 3_So_sanh_nganh.py
│       └── 4_Watchlist.py
└── .github/workflows/
    ├── daily_etl.yml
    └── weekly_company_profile.yml
```

## Chạy dự án

### 1. Chạy pipeline dữ liệu (local)

```bash
pip install -r requirements.txt dbt-duckdb

python extract_vnstock.py          # tạo dim_stock.csv, fact_price_daily.csv
python build_duckdb_file.py        # nạp vào vnstock.duckdb

cp dbt_vnstock/profiles.yml ~/.dbt/profiles.yml
cd dbt_vnstock
dbt seed && dbt run && dbt test
```

### 2. Chạy dashboard (local)

```bash
cd streamlit_app
pip install -r requirements.txt
```

Tạo `streamlit_app/.streamlit/secrets.toml`:

```toml
COOKIE_SIGNING_KEY = "chuỗi-bí-mật-tự-đặt"
SUPABASE_DB_URL = "postgresql://user:password@host:5432/postgres"
```

```bash
streamlit run app.py
```

> Không có secrets? App vẫn chạy — 3 trang công khai (Tổng quan, Chi tiết mã, So sánh ngành) hoạt động bình thường, chỉ ẩn phần đăng nhập/Watchlist.

## CI/CD

<p align="center">
  <img src="docs/screenshots/github_actions.png" alt="GitHub Actions — lịch sử chạy workflow thành công" width="800">
  <br><em>Pipeline chạy tự động theo lịch trên GitHub Actions — không phải chỉ demo local</em>
</p>

- **`daily_etl.yml`** — 15:45 giờ VN, thứ 2–6: Extract giá → Load → dbt run/test → publish `.duckdb` lên GitHub Release (chỉ publish nếu `dbt test` pass).
- **`weekly_company_profile.yml`** — tối Chủ nhật: Extract hồ sơ công ty → Load → rebuild scoped (`dim_stock`, `dim_sector`) → publish.
- Cả 2 workflow dùng chung `concurrency group` để không bao giờ ghi đè file `.duckdb` cùng lúc.

## Roadmap

- [ ] Thêm cảnh báo giá (price alert) qua email/Telegram cho watchlist
- [ ] Backtest chiến lược đơn giản trên dữ liệu lịch sử
- [ ] CI job kiểm tra schema drift từ API trước khi chạy full pipeline

---

<p align="center"><sub>Dự án cá nhân — dữ liệu chỉ phục vụ mục đích học tập/demo, không phải khuyến nghị đầu tư.</sub></p>
