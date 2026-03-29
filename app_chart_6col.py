import streamlit as st
import pandas as pd
import altair as alt
import yfinance as yf
from datetime import datetime, timedelta, time
import re
from pathlib import Path
import time as py_time

from b01_stock_list import WATCH_A, WATCH_B, WATCH_C, WATCH_D, WATCH_E

# --- デフォルトの銘柄リスト ---
DEFAULT_TICKERS = ["8058", "8031", "8001", "8002", "8053", "8015"]

# --- パス定義 ---
BASE_DIR        = Path(__file__).parent
EXCEL_FILE_PATH = BASE_DIR / "_topix_list.xlsx"
CSV_5MIN        = BASE_DIR / "_5min.csv"
CSV_DAILY       = BASE_DIR / "_daily.csv"
PARQUET_5MIN    = BASE_DIR / "_5min.parquet"
PARQUET_DAILY   = BASE_DIR / "_daily.parquet"

# 使用するカラムのみ定義（メモリ削減）
_5MIN_COLS  = ["Datetime", "Ticker", "_date", "Open", "High", "Low", "Close", "Volume"]
_DAILY_COLS = ["Date", "Ticker", "Open", "High", "Low", "Close"]


# ================================================
# --- Parquet変換（フォールバック用）---
# 通常は a03_yfinance_update.py 実行時に変換済み。
# Parquetが存在しない・CSVより古い場合のみここで変換する。
# ================================================

def _build_parquet_if_needed(csv_path: Path, parquet_path: Path) -> bool:
    if not csv_path.exists():
        return False
    if parquet_path.exists() and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
        return False
    if "5min" in csv_path.name:
        df = pd.read_csv(csv_path)
        dt_col = "Datetime_JST" if "Datetime_JST" in df.columns else "Datetime" if "Datetime" in df.columns else None
        if dt_col is None:
            return False
        df["Datetime"] = pd.to_datetime(df[dt_col])
        if df["Datetime"].dt.tz is None:
            df["Datetime"] = df["Datetime"].dt.tz_localize("Asia/Tokyo")
        else:
            df["Datetime"] = df["Datetime"].dt.tz_convert("Asia/Tokyo")
        df["_date"] = df["Datetime"].dt.date.astype(str)
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        drop_cols = [c for c in ["Datetime_JST"] if c in df.columns]
        df.drop(columns=drop_cols, inplace=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    else:
        df = pd.read_csv(csv_path)
        if "Date" not in df.columns or "Ticker" not in df.columns:
            return False
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return True


# ================================================
# 改善① _preload_data: 必要カラムのみ読み込み
# ================================================

@st.cache_resource(show_spinner=False)
def _preload_data():
    _build_parquet_if_needed(CSV_5MIN, PARQUET_5MIN)
    _build_parquet_if_needed(CSV_DAILY, PARQUET_DAILY)

    # --- 5分足 ---
    if PARQUET_5MIN.exists():
        # pyarrow.parquet.read_schema でスキーマのみ取得（読み込みは1回）
        import pyarrow.parquet as pq
        use_cols = [c for c in _5MIN_COLS if c in pq.read_schema(PARQUET_5MIN).names]
        df5 = pd.read_parquet(PARQUET_5MIN, engine="pyarrow", columns=use_cols)
        df5["Datetime"] = pd.to_datetime(df5["Datetime"])
        if df5["Datetime"].dt.tz is None:
            df5["Datetime"] = df5["Datetime"].dt.tz_localize("Asia/Tokyo")
        df5["_date"] = pd.to_datetime(df5["_date"]).dt.date
    elif CSV_5MIN.exists():
        df5 = pd.read_csv(CSV_5MIN)
        dt_col = "Datetime_JST" if "Datetime_JST" in df5.columns else "Datetime"
        df5["Datetime"] = pd.to_datetime(df5[dt_col])
        if df5["Datetime"].dt.tz is None:
            df5["Datetime"] = df5["Datetime"].dt.tz_localize("Asia/Tokyo")
        else:
            df5["Datetime"] = df5["Datetime"].dt.tz_convert("Asia/Tokyo")
        df5["_date"] = df5["Datetime"].dt.date
        df5["Ticker"] = df5["Ticker"].astype(str).str.strip()
    else:
        df5 = pd.DataFrame()

    # --- 日足 ---
    if PARQUET_DAILY.exists():
        import pyarrow.parquet as pq
        use_cols_d = [c for c in _DAILY_COLS if c in pq.read_schema(PARQUET_DAILY).names]
        dfd = pd.read_parquet(PARQUET_DAILY, engine="pyarrow", columns=use_cols_d)
        dfd["Date"] = pd.to_datetime(dfd["Date"]).dt.date
    elif CSV_DAILY.exists():
        dfd = pd.read_csv(CSV_DAILY)
        dfd["Date"] = pd.to_datetime(dfd["Date"]).dt.date
        dfd["Ticker"] = dfd["Ticker"].astype(str).str.strip()
    else:
        dfd = pd.DataFrame()

    # Ticker単位の辞書インデックス（銘柄フィルタをO(1)化）
    df5_by_ticker = {t: grp.reset_index(drop=True) for t, grp in df5.groupby("Ticker")} if not df5.empty else {}
    dfd_by_ticker = {t: grp.reset_index(drop=True) for t, grp in dfd.groupby("Ticker")} if not dfd.empty else {}

    return df5_by_ticker, dfd_by_ticker


@st.cache_resource(show_spinner=False)
def _preload_stock_dict():
    try:
        if not EXCEL_FILE_PATH.exists():
            return {}
        df = pd.read_excel(EXCEL_FILE_PATH)
        return dict(zip(df['ティッカーコード'].astype(str), df['銘柄']))
    except:
        return {}


# ================================================
# --- データ取得関数 ---
# ================================================

@st.cache_data(show_spinner=False, ttl=300)
def get_single_stock_data_csv(code, end_dt, days_5m, daily_months):
    df5_by_ticker, dfd_by_ticker = _preload_data()
    ticker_symbol = code

    # 辞書O(1)アクセス
    if ticker_symbol not in dfd_by_ticker:
        return pd.DataFrame(), pd.DataFrame()
    df_daily = dfd_by_ticker[ticker_symbol].copy()
    daily_start = end_dt - timedelta(days=daily_months * 30 + 10)
    df_daily = df_daily[(df_daily["Date"] >= daily_start) & (df_daily["Date"] <= end_dt)]
    if df_daily.empty:
        return pd.DataFrame(), pd.DataFrame()
    df_daily = df_daily.set_index("Date").sort_index()

    if ticker_symbol not in df5_by_ticker:
        return pd.DataFrame(), df_daily
    df_5m = df5_by_ticker[ticker_symbol]
    df_5m = df_5m[df_5m["_date"] <= end_dt]
    if df_5m.empty:
        return pd.DataFrame(), df_daily
    unique_dates = sorted(df_5m["_date"].unique(), reverse=True)[:days_5m]
    df_5m = df_5m[df_5m["_date"].isin(set(unique_dates))].sort_values("Datetime").reset_index(drop=True)

    return df_5m, df_daily


@st.cache_data(ttl=300)
def get_single_stock_data_yf(code, end_dt, days_5m, daily_months):
    ticker_symbol = code
    daily_start_dt = end_dt - timedelta(days=daily_months * 30 + 10)
    df_daily = yf.download(ticker_symbol, start=daily_start_dt, end=end_dt + timedelta(days=1), interval="1d", progress=False)
    if df_daily.empty:
        return pd.DataFrame(), pd.DataFrame()
    if isinstance(df_daily.columns, pd.MultiIndex):
        df_daily.columns = df_daily.columns.get_level_values(0)
    df_daily.index = pd.to_datetime(df_daily.index).date

    df_5m = pd.DataFrame()
    if (datetime.now().date() - end_dt).days < 60:
        try:
            df_5m = yf.download(ticker_symbol, start=end_dt - timedelta(days=45), end=end_dt + timedelta(days=1), interval="5m", progress=False)
            if not df_5m.empty:
                if isinstance(df_5m.columns, pd.MultiIndex):
                    df_5m.columns = df_5m.columns.get_level_values(0)
                df_5m = df_5m.reset_index()
                dt_col = 'Datetime' if 'Datetime' in df_5m.columns else 'Date'
                df_5m['Datetime'] = pd.to_datetime(df_5m[dt_col]).dt.tz_convert('Asia/Tokyo')
                unique_dates = sorted(df_5m['Datetime'].dt.date.unique(), reverse=True)[:days_5m]
                df_5m = df_5m[df_5m['Datetime'].dt.date.isin(unique_dates)].sort_values('Datetime')
        except Exception:
            df_5m = pd.DataFrame()
    return df_5m, df_daily


# ================================================
# --- チャート関数（変更なし）---
# ================================================

def get_ticker_metrics(df_daily):
    if df_daily.empty: return 0.0, 0.0
    df_valid = df_daily.dropna(subset=['Close'])
    if df_valid.empty: return 0.0, 0.0
    latest_close = df_valid['Close'].iloc[-1]
    latest_date  = df_valid.index[-1]
    prev_data    = df_valid[df_valid.index < latest_date]
    change_pct   = ((latest_close - prev_data['Close'].iloc[-1]) / prev_data['Close'].iloc[-1]) * 100 if not prev_data.empty else 0.0
    return change_pct, latest_close

Y_AXIS_WIDTH = 30

def create_daily_line_chart(df_daily):
    df_chart = df_daily.reset_index()[['Date', 'Close']]
    return alt.Chart(df_chart).mark_line(color='#FF4B4B', strokeWidth=1.5).encode(
        x=alt.X('Date:T', title=None, axis=alt.Axis(labels=True, format='%m', grid=False)),
        y=alt.Y('Close:Q', title=None, scale=alt.Scale(zero=False), axis=alt.Axis(minExtent=Y_AXIS_WIDTH)),
    ).properties(height=180)

def create_pct_change_chart(df_daily, y_domain):
    df_chart = df_daily.reset_index()[['Date', 'Close']]
    first_close = df_chart['Close'].iat[0]
    df_chart = df_chart.assign(PctChange=(df_chart['Close'] - first_close) / first_close * 100)
    gradient_fill = alt.Gradient(
        gradient='linear',
        stops=[alt.GradientStop(color='white', offset=0), alt.GradientStop(color='#FF4B4B', offset=1)],
        x1=1, x2=1, y1=1, y2=0
    )
    area = alt.Chart(df_chart).mark_area(
        line={'color': '#FF4B4B', 'strokeWidth': 1.5},
        color=gradient_fill, opacity=0.4
    ).encode(
        x=alt.X('Date:T', title=None, axis=alt.Axis(labels=True, format='%m', grid=False)),
        y=alt.Y('PctChange:Q', title=None, scale=alt.Scale(domain=y_domain),
                axis=alt.Axis(minExtent=Y_AXIS_WIDTH, format='.1f', labelExpr="datum.value + '%'")),
    )
    return area.properties(height=180)

def create_candle_chart(df):
    pm_start_time = time(12, 30)
    df_c = df.copy().sort_values('Datetime')
    df_c['x_key'] = df_c['Datetime'].dt.strftime('%y/%m/%d %H:%M')
    df_c['date']  = df_c['_date'] if '_date' in df_c.columns else df_c['Datetime'].dt.date
    # groupby.transform('min') の代わりに idxmin で高速化
    day_min_idx = df_c.groupby('date')['Datetime'].idxmin()
    am_mask = pd.Series(False, index=df_c.index)
    am_mask.loc[day_min_idx.values] = True
    df_c['is_am_line'] = am_mask
    df_c['is_pm_line'] = (df_c['Datetime'].dt.time == pm_start_time)
    tick_indices = df_c[df_c['is_am_line'] | df_c['is_pm_line']]['x_key'].unique().tolist()
    y_min, y_max = df_c['Close'].min(), df_c['Close'].max()
    y_scale = alt.Scale(domain=[float(y_min - (y_max - y_min) * 0.05), float(y_max + (y_max - y_min) * 0.05)], zero=False)
    base     = alt.Chart(df_c).encode(x=alt.X('x_key:O', axis=alt.Axis(labels=False, values=tick_indices, grid=False, title=None), sort=None))
    am_rules = alt.Chart(df_c[df_c['is_am_line']]).mark_rule(color='#CCCCCC').encode(x='x_key:O')
    pm_rules = alt.Chart(df_c[df_c['is_pm_line']]).mark_rule(color='#EEEEEE').encode(x='x_key:O')
    candle_color = alt.condition("datum.Open <= datum.Close", alt.value("#ef5350"), alt.value("#26a69a"))
    rule = base.mark_rule().encode(y=alt.Y('Low:Q', scale=y_scale, axis=alt.Axis(minExtent=Y_AXIS_WIDTH, title=None)), y2='High:Q', color=candle_color)
    bar  = base.mark_bar().encode(y='Open:Q', y2='Close:Q', color=candle_color)
    return alt.layer(am_rules, pm_rules, rule, bar).properties(height=180).configure_view(strokeOpacity=0)


# ================================================
# --- Streamlit UI ---
# ================================================
st.set_page_config(page_title="銘柄比較チャート", page_icon="📈", layout="wide")

# ================================================
# 改善② UIを先に描画してからデータロード
# ================================================
col_input, col_stock,  col_code, col_date = st.columns([2, 0.5, 0.5, 1])
with col_input:
    st.markdown("## 📈 銘柄比較チャート")
    data_source = st.radio("データソース", options=["yfinance", "ローカルCSV"], index=1,
                           key="data_source_radio", horizontal=True, label_visibility="collapsed")
    use_csv = (data_source == "ローカルCSV")
with col_stock:
    watch_a_label = f"ウォッチA（{len(WATCH_A)}）"
    watch_b_label = f"ウォッチB（{len(WATCH_B)}）"
    watch_c_label = f"ウォッチC（{len(WATCH_C)}）"
    watch_d_label = f"ウォッチD（{len(WATCH_D)}）"
    watch_e_label = f"ウォッチE（{len(WATCH_E)}）"
    stock_mode = st.radio(
        "銘柄選択",
        options=["銘柄コードを指定", watch_a_label, watch_b_label, watch_c_label, watch_d_label, watch_e_label],
        index=0,
        key="stock_mode_radio"
    )
    is_manual = (stock_mode == "銘柄コードを指定")
with col_code:
    st.markdown("銘柄選択")
    ticker_defaults = ", ".join(DEFAULT_TICKERS)
    raw_input = st.text_area(
        "銘柄コード",
        value=ticker_defaults,
        height=120,
        key="ticker_input",
        label_visibility="collapsed",
        disabled=not is_manual
    )
    if is_manual:
        from b01_stock_list import get_ticker_symbol
        ticker_list = [get_ticker_symbol(t.strip()) for t in re.split(r'[,\s\n]+', raw_input) if t.strip()]
    elif stock_mode == watch_a_label:
        ticker_list = list(WATCH_A)
    elif stock_mode == watch_b_label:
        ticker_list = list(WATCH_B)
    elif stock_mode == watch_c_label:
        ticker_list = list(WATCH_C)
    elif stock_mode == watch_d_label:
        ticker_list = list(WATCH_D)
    elif stock_mode == watch_e_label:
        ticker_list = list(WATCH_E)
    else:
        ticker_list = []
with col_date:
    date_col2, date_col3 = st.columns(2)
    with date_col2:
        st.markdown("基準日設定")
        end_date = st.date_input("基準日", value=datetime.now(), label_visibility="collapsed")
        count_col1, count_col2 = st.columns(2)
        with count_col1:
            daily_months = st.number_input("日足月数", min_value=1, max_value=24, value=6, step=1)
        with count_col2:
            period_days = st.number_input("5分足日数", min_value=1, max_value=60, value=2, step=1)
    with date_col3:
        st.markdown("チャート選択")
        show_daily = st.checkbox("日足チャート", value=True)
        show_pct   = st.checkbox("騰落率チャート", value=True)
        show_5m    = st.checkbox("5分足チャート", value=True)

selected_charts = []
if show_daily: selected_charts.append("daily")
if show_pct:   selected_charts.append("pct")
if show_5m:    selected_charts.append("5m")
cols_per_stock = len(selected_charts)
if cols_per_stock == 0:
    st.warning("表示するチャートを少なくとも1つ選択してください。")
    st.stop()
stocks_per_row = 6 // cols_per_stock

st.markdown("---")

# UIが出てからデータロード
with st.spinner("データを読み込んでいます..."):
    _preload_data()
stock_dict = _preload_stock_dict()

# ================================================
# 改善③ 描画戦略の切り替え
#
# show_pct=True → 全銘柄のY軸共通化が必要 → 全量取得後に描画（従来通り）
# show_pct=False → 1銘柄取得ごとに即時描画（体感速度◎）
# ================================================

def render_row(row_items, cols, show_daily, show_pct, show_5m, cols_per_stock, common_y_domain):
    """1行分（最大stocks_per_row銘柄）を描画する"""
    for row_idx, item in enumerate(row_items):
        base_col  = row_idx * cols_per_stock
        name_text    = f"**{item['name']}** ({item['ticker']})"
        metrics_text = f"{item['close']:,.1f} JPY ({item['chg']:+.2f}%)"
        current_slot = 0
        if show_daily:
            with cols[base_col + current_slot]:
                st.write(name_text if current_slot == 0 else "&nbsp;")
                st.altair_chart(create_daily_line_chart(item["df_daily"]), width='stretch')
                if current_slot == cols_per_stock - 1:
                    st.caption(metrics_text)
            current_slot += 1
        if show_pct:
            with cols[base_col + current_slot]:
                st.write(name_text if current_slot == 0 else "&nbsp;")
                st.altair_chart(create_pct_change_chart(item["df_daily"], common_y_domain), width='stretch')
                if current_slot == cols_per_stock - 1:
                    st.caption(metrics_text)
            current_slot += 1
        if show_5m:
            with cols[base_col + current_slot]:
                st.write(name_text if current_slot == 0 else "&nbsp;")
                if not item["df"].empty:
                    st.altair_chart(create_candle_chart(item["df"]), width='stretch')
                else:
                    st.warning("5分足データなし(60日制限)")
                if current_slot == cols_per_stock - 1:
                    st.caption(metrics_text)
            current_slot += 1


def _fetch_all(ticker_list, use_csv, end_date, period_days, daily_months):
    """全銘柄データを取得してリストで返す（キャッシュ済みのためI/Oなし）"""
    results = []
    for ticker in ticker_list:
        if use_csv:
            df_5m, df_daily = get_single_stock_data_csv(ticker, end_date, period_days, daily_months)
        else:
            py_time.sleep(0.2)
            df_5m, df_daily = get_single_stock_data_yf(ticker, end_date, period_days, daily_months)
        if not df_daily.empty:
            chg, last_c = get_ticker_metrics(df_daily)
            name = stock_dict.get(str(ticker), str(ticker))
            results.append({"ticker": ticker, "df": df_5m, "name": name,
                            "df_daily": df_daily, "chg": chg, "close": last_c})
    return results


if show_pct:
    # --- 騰落率チャートあり: パス1でY軸域を決定 → パス2で行単位即時描画 ---
    with st.spinner(f"{len(ticker_list)}銘柄のデータを取得中..."):
        scored_tickers = _fetch_all(ticker_list, use_csv, end_date, period_days, daily_months)

    all_pct_values = []
    for item in scored_tickers:
        fc = item["df_daily"]['Close'].iloc[0]
        all_pct_values.extend(((item["df_daily"]['Close'] - fc) / fc * 100).tolist())
    common_y_domain = [min(all_pct_values) - 2, max(all_pct_values) + 2] if all_pct_values else [-10, 10]

    for i in range(0, len(scored_tickers), stocks_per_row):
        cols = st.columns(6)
        render_row(scored_tickers[i:i + stocks_per_row], cols,
                   show_daily, show_pct, show_5m, cols_per_stock, common_y_domain)
        st.markdown("")

else:
    # --- 騰落率チャートなし: 1銘柄取得ごとに即時描画 ---
    common_y_domain = [-10, 10]
    row_buffer = []

    for ticker in ticker_list:
        if use_csv:
            df_5m, df_daily = get_single_stock_data_csv(ticker, end_date, period_days, daily_months)
        else:
            py_time.sleep(0.2)
            df_5m, df_daily = get_single_stock_data_yf(ticker, end_date, period_days, daily_months)

        if not df_daily.empty:
            chg, last_c = get_ticker_metrics(df_daily)
            name = stock_dict.get(str(ticker), str(ticker))
            row_buffer.append({"ticker": ticker, "df": df_5m, "name": name,
                               "df_daily": df_daily, "chg": chg, "close": last_c})

        if len(row_buffer) == stocks_per_row:
            cols = st.columns(6)
            render_row(row_buffer, cols, show_daily, show_pct, show_5m, cols_per_stock, common_y_domain)
            st.markdown("")
            row_buffer = []

    if row_buffer:
        cols = st.columns(6)
        render_row(row_buffer, cols, show_daily, show_pct, show_5m, cols_per_stock, common_y_domain)
        st.markdown("")
