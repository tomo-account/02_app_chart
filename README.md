# 銘柄別チャートアプリ

こちらのリポジトリで紹介したアプリのバリエーションです。
https://github.com/tomo-account/00_sample_app/tree/main

<br><br>

## 必要なライブラリのインストール

```bash
pip install streamlit==1.52.2 pandas==2.3.3 altair==6.0.0 yfinance==1.0 openpyxl==3.1.5 pyarrow==22.0.0
```

<br><br>

## パフォーマンスについて

- 初回起動時にCSVをParquet形式に変換します（2回目以降は高速化されます）
- 「コードの入力順」モードでは1銘柄取得するたびに即時描画するため、体感速度が向上します
- 「騰落率順」モードでは全銘柄取得後にソートして一括描画します


<br><br>

## 画面イメージ

<br>

### 設定UI

![](https://raw.githubusercontent.com/tomo-account/01_app_chart/refs/heads/main/image_settings.png)

<br>

### 📈 app_chart_1col.py のチャート

- 5分足ローソク足チャートを20日分まで表示
- 日別サマリーテーブル（始値・高値・安値・終値・値幅・GAP・騰落率）
- データソースをyfinanceとローカルCSVで切り替え可能

![](https://raw.githubusercontent.com/tomo-account/01_app_chart/refs/heads/main/img_chart_1col.png)

<br>

### 📈 app_chart_6col.py のチャート

![](https://raw.githubusercontent.com/tomo-account/01_app_chart/refs/heads/main/img_chart_6col.png)

<br>

### 📈 app_chart_5min_6col.py のチャート

![](https://raw.githubusercontent.com/tomo-account/01_app_chart/refs/heads/main/img_chart_5min_6col.png)

<br>

### 📈 app_chart_5min_3col.py のチャート

![](https://raw.githubusercontent.com/tomo-account/01_app_chart/refs/heads/main/img_chart_5min_3col.png)


<br><br>

## 必要なデータファイル（別リポジトリで取得）

本アプリはローカルCSVをメインのデータソースとして使用します。
以下のファイルをアプリと同じフォルダに配置してください。

| ファイル | 内容 | 取得方法 |
|:---|:---|:---|
| `_5min.csv` | 5分足データ | [yfinance_data_loader](https://github.com/tomo-account/00_data_loader/tree/main) を参考 |
| `_daily.csv` | 日足データ | 同上 |
| `_topix_list.xlsx` | 東証上場銘柄一覧 | [JPXサイト](https://www.jpx.co.jp/markets/statistics-equities/misc/01.html)からダウンロード |

> yfinanceモードを使う場合、上記CSVファイルがなくても動作します。

<br><br>

## データの取り扱いについて

- 本アプリは個人利用および学習を目的としたツールであり、投資勧誘を目的としたものではありません。
- `yfinance` ライブラリを使用しています。利用にあたっては、Yahoo! の規約を遵守してください。
- 短時間での大量取得はサーバーに負担がかかります。APIのレート制限を守り、過度なリクエストは避けてください。

### Yahoo! 規約類

- [Yahoo! Finance Terms of Service](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html)
- [Yahoo! Developer API Terms of Use](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm)
- [Yahoo! 権利関係ページ](https://legal.yahoo.com/us/en/yahoo/permissions/requests/index.html)

<br><br>

## ⚠️ 免責事項

- **データの正確性**：取得データは正確性や即時性を保証しません。
- **損害への責任**：本ツールの利用により生じたいかなる損害についても、制作者は一切の責任を負いません。

<br><br>

## 関連リポジトリ・記事

- [Qiita：yfinanceとStreamlitで株価分析アプリを作ってみよう（準備編）](https://qiita.com/tomo-account/items/946761b0855ed75c381a)

<br><br>

## License

MIT License
