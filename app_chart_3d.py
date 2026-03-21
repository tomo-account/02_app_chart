import streamlit as st
import pandas as pd
import altair as alt
import yfinance as yf
from datetime import datetime, timedelta, time
import re
from pathlib import Path
import time as py_time

from b01_stock_list import WATCH_A, WATCH_B, WATCH_C

# --- パス定義 ---
BASE_DIR = Path(__file__).parent
TOPIX_FILE_PATH = BASE_DIR / "_topix_list.xlsx"
CSV_5MIN        = BASE_DIR / "_5min.csv"
CSV_DAILY       = BASE_DIR / "_daily.csv"
# ★ CSVを初回だけParquetに変換してキャッシュ（2回目以降の起動が大幅高速化）
PARQUET_5MIN    = BASE_DIR / "_5min.parquet"
PARQUET_DAILY   = BASE_DIR / "_daily.parquet"


# ================================================
# --- 起動時プリロード（UIより先に実行）---
# ================================================

def _build_parquet_if_needed(csv_path: Path, parquet_path: Path) -> bool:
    """CSVがParquetより新しい場合だけ再変換する。変換したらTrueを返す。"""
    if not csv_path.exists():
        return False
    if parquet_path.exists() and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
        return False  # Parquetが最新なので変換不要
    # --- 5分足 ---
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
        df["_date"] = df["Datetime"].dt.date.astype(str)   # Parquetはdate型不可なのでstr
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        # Parquet出力に不要な元のDateTime列を削除
        drop_cols = [c for c in ["Datetime_JST"] if c in df.columns]
        df.drop(columns=drop_cols, inplace=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    # --- 日足 ---
    else:
        df = pd.read_csv(csv_path)
        if "Date" not in df.columns or "Ticker" not in df.columns:
            return False
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")  # str保存
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return True


@st.cache_resource(show_spinner="データを読み込んでいます...")
def _preload_data():
    """
    ★ st.cache_resource: プロセスが生きている間は1回だけ実行。
    　 アプリ起動時にCSV→Parquet変換＋読み込みをまとめて行い、
    　 以降の全リクエストでメモリ上のDataFrameをそのまま使い回す。
    """
    # Parquetへの変換（CSVが更新されていれば再変換）
    _build_parquet_if_needed(CSV_5MIN, PARQUET_5MIN)
    _build_parquet_if_needed(CSV_DAILY, PARQUET_DAILY)

    # --- 5分足の読み込み ---
    if PARQUET_5MIN.exists():
        df5 = pd.read_parquet(PARQUET_5MIN, engine="pyarrow")
        # Parquetから戻す際に型を復元
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

    # --- 日足の読み込み ---
    if PARQUET_DAILY.exists():
        dfd = pd.read_parquet(PARQUET_DAILY, engine="pyarrow")
        dfd["Date"] = pd.to_datetime(dfd["Date"]).dt.date
    elif CSV_DAILY.exists():
        dfd = pd.read_csv(CSV_DAILY)
        dfd["Date"] = pd.to_datetime(dfd["Date"]).dt.date
        dfd["Ticker"] = dfd["Ticker"].astype(str).str.strip()
    else:
        dfd = pd.DataFrame()

    return df5, dfd


@st.cache_resource(show_spinner=False)
def _preload_dicts():
    """銘柄名辞書をプロセス起動時に1度だけ読み込む"""
    try:
        if not TOPIX_FILE_PATH.exists():
            return {}
        df = pd.read_excel(TOPIX_FILE_PATH)
        return dict(zip(df['コード'].astype(str), df['銘柄']))
    except:
        return {}


# ================================================
# --- データ取得関数 ---
# ================================================

@st.cache_data(show_spinner=False, ttl=300)
def get_single_stock_data_csv(code, end_dt, days):
    """プリロード済みDataFrameから該当銘柄を切り出す（★キャッシュ付き）"""
    raw_5m, raw_daily = _preload_data()
    if raw_5m.empty:
        return pd.DataFrame(), pd.DataFrame()

    ticker_symbol = f"{code}.T"

    # --- 5分足 ---
    df_5m = raw_5m[raw_5m["Ticker"] == ticker_symbol]
    if df_5m.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_5m = df_5m[df_5m["_date"] <= end_dt]
    unique_dates = sorted(df_5m["_date"].unique(), reverse=True)[:days]
    df_5m = df_5m[df_5m["_date"].isin(set(unique_dates))].sort_values("Datetime").copy()

    # --- 日足 ---
    if raw_daily.empty:
        return df_5m, pd.DataFrame()

    df_daily = raw_daily[raw_daily["Ticker"] == ticker_symbol].copy()
    df_daily = df_daily[df_daily["Date"] <= end_dt].set_index("Date").sort_index()

    return df_5m, df_daily


@st.cache_data(ttl=300)
def get_single_stock_data(code, end_dt, days):
    """Yahoo Financeから5分足データと日足データを取得する"""
    ticker_symbol = f"{code}.T"
    df_5m = yf.download(ticker_symbol, start=end_dt - timedelta(days=45), end=end_dt + timedelta(days=1), interval="5m", progress=False)
    df_daily = yf.download(ticker_symbol, start=end_dt - timedelta(days=180), end=end_dt + timedelta(days=1), interval="1d", progress=False)
    if df_5m.empty or df_daily.empty:
        return pd.DataFrame(), pd.DataFrame()
    if isinstance(df_5m.columns, pd.MultiIndex): df_5m.columns = df_5m.columns.get_level_values(0)
    if isinstance(df_daily.columns, pd.MultiIndex): df_daily.columns = df_daily.columns.get_level_values(0)
    df_5m = df_5m.reset_index()
    dt_col = 'Datetime' if 'Datetime' in df_5m.columns else 'Date'
    df_5m['Datetime'] = pd.to_datetime(df_5m[dt_col]).dt.tz_convert('Asia/Tokyo')
    unique_dates = sorted(df_5m['Datetime'].dt.date.unique(), reverse=True)[:days]
    df_5m = df_5m[df_5m['Datetime'].dt.date.isin(unique_dates)].sort_values('Datetime')
    df_daily.index = pd.to_datetime(df_daily.index).date
    return df_5m, df_daily


# ================================================
# --- メトリクス・チャート ---
# ================================================

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def get_market_status(rsi_value):
    if rsi_value >= 80: return f"⚠️イナゴ買 (RSI {rsi_value:.1f})"
    if rsi_value >= 65: return f"🔥上昇中 (RSI {rsi_value:.1f})"
    if rsi_value <= 20: return f"🚀底打ち (RSI {rsi_value:.1f})"
    if rsi_value <= 35: return f"💀暴落 (RSI {rsi_value:.1f})"
    return f"平常〰️ (RSI {rsi_value:.1f})"

def get_ticker_metrics(df_daily):
    if df_daily.empty: return 0.0, 50.0, 0.0
    df_valid = df_daily.dropna(subset=['Close'])
    if df_valid.empty: return 0.0, 50.0, 0.0
    latest_close = df_valid['Close'].iloc[-1]
    latest_date  = df_valid.index[-1]
    prev_data    = df_valid[df_valid.index < latest_date]
    change_pct   = ((latest_close - prev_data['Close'].iloc[-1]) / prev_data['Close'].iloc[-1]) * 100 if not prev_data.empty else 0.0
    rsi_val      = calculate_rsi(df_valid['Close']).iloc[-1]
    return change_pct, float(rsi_val) if pd.notna(rsi_val) else 50.0, latest_close

Y_AXIS_WIDTH = 50

def create_daily_line_chart(df_daily):
    df_chart = df_daily.copy().reset_index().rename(columns={'index': 'Date'})[['Date', 'Close']]
    return alt.Chart(df_chart).mark_line(color='#FF4B4B', strokeWidth=1.5).encode(
        x=alt.X('Date:T', title=None, axis=alt.Axis(format='%m月', grid=False)),
        y=alt.Y('Close:Q', title=None, scale=alt.Scale(zero=False), axis=alt.Axis(minExtent=Y_AXIS_WIDTH)),
        tooltip=['Date', 'Close']
    ).properties(height=200)

def create_candle_layer(base, am_rules, pm_rules, y_scale):
    candle_color = alt.condition("datum.Open <= datum.Close", alt.value("#ef5350"), alt.value("#26a69a"))
    rule = base.mark_rule().encode(
        y=alt.Y('Low:Q', scale=y_scale, axis=alt.Axis(minExtent=Y_AXIS_WIDTH, title=None)),
        y2=alt.Y2('High:Q'), color=candle_color)
    bar = base.mark_bar().encode(y='Open:Q', y2='Close:Q', color=candle_color)
    return alt.layer(am_rules, pm_rules, rule, bar)

def create_volume_layer(base):
    candle_color = alt.condition("datum.Open <= datum.Close", alt.value("#ef5350"), alt.value("#26a69a"))
    return base.mark_bar(opacity=0.5).encode(
        y=alt.Y('Volume_k:Q', axis=alt.Axis(orient='left', minExtent=Y_AXIS_WIDTH, title=None)),
        color=candle_color).properties(height=80)

def create_candle_chart(df, show_volume=True):
    pm_start_time = time(12, 30)
    df_c = df.copy().sort_values('Datetime')
    df_c['x_key']    = df_c['Datetime'].dt.strftime('%y/%m/%d %H:%M')
    df_c['Volume_k'] = df_c['Volume'] / 1000
    df_c['date']     = df_c['_date'] if '_date' in df_c.columns else df_c['Datetime'].dt.date
    df_c['is_am_line'] = (df_c['Datetime'] == df_c.groupby('date')['Datetime'].transform('min'))
    df_c['is_pm_line'] = (df_c['Datetime'].dt.time == pm_start_time)
    tick_indices = df_c[df_c['is_am_line'] | df_c['is_pm_line']]['x_key'].unique().tolist()
    y_min, y_max = df_c['Close'].min(), df_c['Close'].max()
    y_scale = alt.Scale(domain=[float(y_min - (y_max - y_min) * 0.05), float(y_max + (y_max - y_min) * 0.05)], zero=False)
    base     = alt.Chart(df_c).encode(x=alt.X('x_key:O', axis=alt.Axis(labels=False, values=tick_indices, grid=False, title=None), sort=None))
    am_rules = alt.Chart(df_c[df_c['is_am_line']]).mark_rule(color='#CCCCCC').encode(x='x_key:O')
    pm_rules = alt.Chart(df_c[df_c['is_pm_line']]).mark_rule(color='#EEEEEE').encode(x='x_key:O')
    candle   = create_candle_layer(base, am_rules, pm_rules, y_scale)
    if show_volume:
        return alt.vconcat(candle, create_volume_layer(base)).resolve_scale(x='shared').configure_view(strokeOpacity=0)
    return candle.configure_view(strokeOpacity=0)


def render_stock_card(item, selected_cols, stock_dict):
    """1銘柄分の表示ブロックを描画する"""
    if item["df"].empty:
        if item["ticker"]:
            st.warning(f"銘柄コード {item['ticker']}: データなし（市場閉場中またはコードミス）")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        ticker_code = str(item["ticker"])
        stock_name  = stock_dict.get(ticker_code, item["name"])
        st.subheader(f"{ticker_code}{stock_name}")
        st.caption(f"<span class='status-label'>{get_market_status(item['rsi'])}</span>", unsafe_allow_html=True)
        st.caption(f"{item['ticker']}  |  {item['latest_close']:,.1f} JPY |  {item['change_pct']:+.2f}%")
    with c3:
        st.altair_chart(create_daily_line_chart(item["df_daily"]), width='stretch')

    date_col = item["df"]["_date"] if "_date" in item["df"].columns else item["df"]["Datetime"].dt.date
    s_df = item["df"].groupby(date_col).agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    ).sort_index()

    # 日足データで OHLC を一括上書き
    df_d = item["df_daily"]
    common_idx = s_df.index.intersection(df_d.index)
    if len(common_idx) > 0:
        s_df.loc[common_idx, ['Open', 'High', 'Low', 'Close']] = \
            df_d.loc[common_idx, ['Open', 'High', 'Low', 'Close']].values

    # shift() で前日終値を一括取得
    prev_close_series = df_d['Close'].shift(1)
    gaps, chgs = [], []
    for d in s_df.index:
        if d in prev_close_series.index and pd.notna(prev_close_series.loc[d]):
            prev_close = prev_close_series.loc[d]
            gap_val = s_df.loc[d, 'Open'] - prev_close
            gaps.append(f"{gap_val:+,.1f}")
            val = ((s_df.loc[d, 'Close'] - prev_close) / prev_close) * 100
            chgs.append(f"{'🔴' if val > 0 else '🟢' if val < 0 else ''}{val:+.2f}%")
        else:
            gaps.append("-")
            chgs.append("0.00%")

    s_df['値幅'] = s_df['High'] - s_df['Low']
    s_df['GAP']  = gaps
    s_df['騰落'] = chgs
    s_df = s_df.rename(columns={'Open': '始値', 'High': '高値', 'Low': '安値'})

    st.altair_chart(create_candle_chart(item["df"], show_volume=True), width="stretch")

    if selected_cols:
        valid_cols = [c for c in selected_cols if c in s_df.columns]
        if valid_cols:
            display_df = s_df[valid_cols].T
            display_df.columns = [d.strftime('%m/%d') for d in display_df.columns]
            st.dataframe(
                display_df.map(lambda x: f"{x:,.1f}" if isinstance(x, (int, float)) else x),
                width='stretch'
            )


# ================================================
# --- Streamlit UI ---
# ================================================
st.set_page_config(page_title="銘柄別チャート", page_icon="📈", layout="wide")
st.markdown("<style>.stDataFrame div{border-radius:0px;} .status-label{font-weight:bold;margin-bottom:10px;display:block;}</style>", unsafe_allow_html=True)

# ★ UIレンダリング前にプリロードを起動（初回はスピナー表示、以降は即時）
_preload_data()
stock_dict = _preload_dicts()

ALL_DISPLAY_COLS = ['騰落', 'GAP', '始値', '高値', '安値', '値幅']

col_input, col_stock, col_date, col_days, col_sort, col_select = st.columns([2, 0.4, 0.4, 0.4, 0.4, 0.4])
with col_input:
    st.markdown("## 📈 銘柄別チャート")
    data_source = st.radio("データソース", options=["yfinance", "ローカルCSV"], index=1,
                           key="data_source_radio", horizontal=True, label_visibility="collapsed")
    use_csv = (data_source == "ローカルCSV")
with col_stock:
    watch_a_label = f"ウォッチA（{len(WATCH_A)}）"
    watch_b_label = f"ウォッチB（{len(WATCH_B)}）"
    watch_c_label = f"ウォッチC（{len(WATCH_C)}）"
    stock_mode = st.radio(
        "銘柄選択",
        options=["銘柄コードを指定", watch_a_label, watch_b_label, watch_c_label],
        index=0,
        key="stock_mode_radio"
    )
    is_manual = (stock_mode == "銘柄コードを指定")
with col_date:
    raw_input = st.text_area("1. 銘柄コード", value="5020, 5019, 5021, 1662", height=160, key="ticker_input",
                             disabled=not is_manual)
    if is_manual:
        ticker_list = [t.strip() for t in re.split(r'[,\s\n]+', raw_input) if t.strip()]
    elif stock_mode == watch_a_label:
        ticker_list = [t.replace(".T", "") for t in WATCH_A]
    elif stock_mode == watch_b_label:
        ticker_list = [t.replace(".T", "") for t in WATCH_B]
    elif stock_mode == watch_c_label:
        ticker_list = [t.replace(".T", "") for t in WATCH_C]
    else:
        ticker_list = []
with col_days:
    end_date    = st.date_input("2. 基準日", value=datetime.now())
    period_days = st.number_input("3. 遡る日数", min_value=1, max_value=20, value=3, step=1)  # ★ 10→3に変更
with col_sort:
    sort_order = st.radio("並べ替え", ["コードの入力順", "騰落率降順", "騰落率昇順"])
    st.caption("※基準日の騰落率")
with col_select:
    selected_cols = []
    rows = [ALL_DISPLAY_COLS[i:i+2] for i in range(0, len(ALL_DISPLAY_COLS), 2)]
    for row in rows:
        cols = st.columns(2)
        for col, label in zip(cols, row):
            with col:
                if st.checkbox(label, value=True, key=f"col_chk_{label}"):
                    selected_cols.append(label)

# --- 処理ループ ---
scored_tickers = []
for ticker in ticker_list:
    if use_csv:
        df_5m, df_daily = get_single_stock_data_csv(ticker, end_date, period_days)
    else:
        py_time.sleep(0.2)
        df_5m, df_daily = get_single_stock_data(ticker, end_date, period_days)

    if not df_5m.empty:
        chg, rsi, last_c = get_ticker_metrics(df_daily)
        name = stock_dict.get(str(ticker), f"{ticker}.T")
        scored_tickers.append({"ticker": ticker, "df": df_5m, "name": name,
                                "df_daily": df_daily, "change_pct": chg, "rsi": rsi, "latest_close": last_c})
    else:
        scored_tickers.append({"ticker": ticker, "df": pd.DataFrame(), "change_pct": -999})

if "降順" in sort_order: scored_tickers.sort(key=lambda x: x['change_pct'], reverse=True)
elif "昇順" in sort_order: scored_tickers.sort(key=lambda x: x['change_pct'])

# --- ★ 2列描画ループ ---
for i in range(0, len(scored_tickers), 2):
    left_item  = scored_tickers[i]
    right_item = scored_tickers[i + 1] if i + 1 < len(scored_tickers) else None

    col_left, col_spacer, col_right = st.columns([10, 1, 10])

    with col_left:
        st.markdown("---")
        st.html('<div style="height: 4px;"></div>')
        render_stock_card(left_item, selected_cols, stock_dict)

    with col_right:
        st.markdown("---")
        st.html('<div style="height: 4px;"></div>')
        if right_item is not None:
            render_stock_card(right_item, selected_cols, stock_dict)