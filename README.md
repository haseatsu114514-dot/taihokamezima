# taihokamezima

みんレポの `タイホウ亀島店` データを自動取得して、SQLite に保存し、店全体の強弱・機種別傾向・末尾傾向・監視機種・仮説検証まで回すためのローカル分析ツールです。

公開用の想定 URL:

- GitHub リポジトリ: [haseatsu114514-dot/taihokamezima](https://github.com/haseatsu114514-dot/taihokamezima)
- GitHub Pages: [https://haseatsu114514-dot.github.io/taihokamezima/](https://haseatsu114514-dot.github.io/taihokamezima/)

## できること

- 店舗一覧ページを巡回して、未取得日だけ日別ページを取得
- 日別ページから `daily_summary` / `machine_summary` / `sueo_summary` を保存
- `?kishu=all&sort=num` の全台ページから `machine_unit_results` を保存
- 監視機種を `config.toml` で追加・変更
- イベントタグを自動付与
  - 公式ベース: `3のつく日`, `第1土曜日`
  - ユーザー仮説: `4のつく日`
  - `30日`, `31日` は `3のつく日` 判定から除外
- HTML 構造変更時に `parse_errors` へエラーログ保存
- レポート出力
  - 今日の強弱サマリー
  - 監視機種の優先順位
  - 狙い候補の末尾
  - 仮説検証レポート

## テーブル

必須テーブルはすべて `sql/schema.sql` に入っています。

- `daily_summary`
- `machine_summary`
- `sueo_summary`
- `machine_unit_results`
- `calendar_tags`
- `watchlist_machines`
- `hypotheses`

補助テーブル:

- `raw_pages`
- `parse_errors`

## セットアップ

```bash
cd /Users/hasegawaatsuki/Documents/New\ project/taihokamezima
python3 -m pip install -r requirements.txt
```

## 実行

いちばん簡単なのは [run_report.command](/Users/hasegawaatsuki/Documents/New project/taihokamezima/run_report.command) をダブルクリックです。

これで:

- 必要ライブラリの確認
- データ取得
- 分析
- レポート表示

まで一気に進みます。

手動で動かす場合:

収集:

```bash
python3 scripts/collect_minrepo.py --config config.toml
```

分析とレポート生成:

```bash
python3 scripts/analyze_minrepo.py --config config.toml
```

まとめて実行:

```bash
python3 scripts/run_pipeline.py --config config.toml
```

## 出力

- SQLite: `data/taihokamezima.sqlite3`
- Markdown: `reports/latest_report.md`
- JSON: `reports/latest_analysis.json`
- HTML: `reports/index.html`

GitHub Actions を使うと、`data/` と `reports/` もリポジトリに保存されます。

## GitHub 公開

`.github/workflows/update-report.yml` で次を自動化しています。

- 定期取得
- 分析レポート生成
- `data/` と `reports/` の更新コミット
- GitHub Pages へのデプロイ

初回 push 後は GitHub の Actions タブから `update-report` を手動実行すれば、Pages 反映を早められます。

## 設定

`config.toml` で次を編集できます。

- `max_listing_pages`
  最新何ページ分まで一覧をなめるか
- `watchlist_machines`
  監視機種と優先度
- `hypotheses`
  仮説タグ定義

`watchlist_machines` は次のマッチ方法に対応しています。

- `exact`
  完全一致
- `contains`
  部分一致
- `regex`
  表記ゆれ込みで監視したいとき用

仮説は今の実装では `rule_type = "day_digit"` をサポートしています。
例えば `rule_value = "4"` で `4のつく日` を表せます。

## 実装メモ

- 一覧ページは `日付 / 総差枚 / 平均差枚 / 平均G` のテーブルを解析します
- 日別ページは `機種別データ`, `バラエティ`, `末尾別データ` セクションを解析します
- 全台データは `all_units_query = "?kishu=all&sort=num"` を使って取りにいきます
- 全台ページが失敗した場合は日別ページの `バラエティ` を最低限のフォールバックとして保存します

## テスト

```bash
pytest -q
```

fixtures ベースで以下を検証しています。

- 一覧ページの未取得日抽出
- 日別ページのサマリー / 機種別 / 末尾別解析
- 全台ページの解析
- イベントタグ生成
- 簡易分析レポート生成
