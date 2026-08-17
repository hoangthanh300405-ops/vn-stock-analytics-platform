"""
theme.py - Thiết kế chung (design system) dùng lại ở mọi trang, tránh trùng lặp
CSS/màu sắc giữa app.py và các file trong pages/.

QUY ƯỚC MÀU THEO THỊ TRƯỜNG CHỨNG KHOÁN VIỆT NAM (khác chuẩn phương Tây/Plotly
mặc định, nơi xanh=tăng/đỏ=giảm):
  - Tăng giá (dương)      -> ĐỎ    (COLOR_UP)
  - Giảm giá (âm)         -> XANH LÁ (COLOR_DOWN)
  - Đứng giá / tham chiếu -> VÀNG   (COLOR_FLAT)
Mọi chart (candlestick, bar so sánh ngành, khối lượng) và bảng dữ liệu trong
app phải dùng đúng bộ màu này thay vì màu mặc định của thư viện.
"""

import pandas as pd
import streamlit as st

COLOR_UP = "#E4572E"       # tăng giá - đỏ cam, đủ tương phản trên nền sáng lẫn tối
COLOR_DOWN = "#1F9D55"     # giảm giá - xanh lá
COLOR_FLAT = "#D6A200"     # đứng giá / tham chiếu - vàng đậm
COLOR_PRIMARY = "#2454FF"  # màu thương hiệu / accent chính của app
COLOR_MUTED = "#5B6472"    # chữ phụ, nhãn, caption

_TONE_COLOR = {"up": COLOR_UP, "down": COLOR_DOWN, "flat": COLOR_FLAT}


def tone_color(tone: str) -> str:
    """tone: 'up' | 'down' | 'flat' | bất kỳ -> mã màu tương ứng (mặc định COLOR_PRIMARY)"""
    return _TONE_COLOR.get(tone, COLOR_PRIMARY)


def change_tone(value) -> str:
    """value: price_change_pct (hoặc số delta bất kỳ) -> 'up' | 'down' | 'flat'"""
    if value is None or pd.isna(value):
        return "flat"
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "flat"


def format_pct(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,.2f}%"


def apply_global_style():
    """Gọi 1 lần ở đầu mỗi trang, ngay sau st.set_page_config(). CSS idempotent
    (an toàn khi Streamlit rerun script nhiều lần)."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        html, body, [class*="css"] {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }}

        /* Sidebar tối màu để phân tách rõ với nội dung chính */
        [data-testid="stSidebar"] {{
            background: #0B1220;
        }}
        [data-testid="stSidebar"] * {{
            color: #E5E9F0 !important;
        }}
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {{
            color: #9AA5B1 !important;
        }}

        .block-container {{
            padding-top: 2rem;
            padding-bottom: 3rem;
        }}

        /* Tiêu đề trang: icon + tên + caption phụ, dùng chung qua page_header() */
        .vnx-header {{
            display: flex;
            align-items: center;
            gap: .6rem;
            margin-bottom: .2rem;
        }}
        .vnx-header .vnx-icon {{ font-size: 1.8rem; }}
        .vnx-header h1 {{ margin: 0; font-size: 1.6rem; font-weight: 700; }}
        .vnx-subtitle {{ color: {COLOR_MUTED}; margin-bottom: 1.4rem; font-size: .92rem; }}

        /* Thẻ KPI dùng qua metric_card() - thay cho st.metric mặc định để tự
           kiểm soát màu theo đúng quy ước VN (tăng=đỏ) */
        .vnx-card {{
            background: #FFFFFF;
            border: 1px solid #E7EAF0;
            border-left: 4px solid {COLOR_PRIMARY};
            border-radius: 10px;
            padding: .9rem 1.1rem;
            box-shadow: 0 1px 3px rgba(16,24,40,.04);
        }}
        .vnx-card .vnx-card-label {{
            font-size: .8rem;
            color: {COLOR_MUTED};
            font-weight: 500;
            margin-bottom: .25rem;
        }}
        .vnx-card .vnx-card-value {{
            font-size: 1.5rem;
            font-weight: 700;
            line-height: 1.2;
        }}

        [data-testid="stMetric"] {{
            background: #FFFFFF;
            border: 1px solid #E7EAF0;
            border-radius: 10px;
            padding: .8rem 1rem;
            box-shadow: 0 1px 3px rgba(16,24,40,.04);
        }}

        [data-testid="stDataFrame"] {{
            border: 1px solid #E7EAF0;
            border-radius: 10px;
            overflow: hidden;
        }}

        div[data-testid="stTabs"] button {{
            font-weight: 600;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(icon: str, title: str, subtitle: str = ""):
    """Tiêu đề trang đồng bộ (icon + tên + caption), thay cho st.title() rời rạc."""
    st.markdown(
        f"""
        <div class="vnx-header"><span class="vnx-icon">{icon}</span><h1>{title}</h1></div>
        {f'<div class="vnx-subtitle">{subtitle}</div>' if subtitle else ''}
        """,
        unsafe_allow_html=True,
    )


def metric_card(col, label: str, value: str, tone: str = "neutral"):
    """Thẻ KPI có viền màu theo tone ('up'/'down'/'flat'/'neutral'). Dùng thay
    st.metric() ở những chỗ cần đúng quy ước màu VN (st.metric mặc định tô màu
    delta kiểu phương Tây, không đảo được theo ý muốn một cách gọn gàng)."""
    color = tone_color(tone) if tone in _TONE_COLOR else COLOR_PRIMARY
    col.markdown(
        f"""
        <div class="vnx-card" style="border-left-color:{color}">
            <div class="vnx-card-label">{label}</div>
            <div class="vnx-card-value" style="color:{color}">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_change_df(df: pd.DataFrame, pct_col: str = "price_change_pct"):
    """Trả về pandas Styler tô màu cột % thay đổi theo đúng quy ước VN
    (tăng=đỏ/giảm=xanh lá), truyền thẳng vào st.dataframe(). Nếu df không có
    cột pct_col thì trả nguyên df."""
    if pct_col not in df.columns:
        return df

    def _color(v):
        if pd.isna(v):
            return ""
        c = COLOR_UP if v > 0 else (COLOR_DOWN if v < 0 else COLOR_FLAT)
        return f"color: {c}; font-weight: 600;"

    styler = df.style
    styler = styler.map(_color, subset=[pct_col]) if hasattr(styler, "map") else styler.applymap(_color, subset=[pct_col])
    return styler.format({pct_col: "{:+.2f}%"}, na_rep="—")
