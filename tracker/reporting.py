from __future__ import annotations

import html
import json
from typing import Any


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


def render_markdown(analysis: dict[str, Any]) -> str:
    if analysis.get("sample_days", 0) == 0:
        return "# タイホウ亀島レポート\n\n- データ未取得\n"

    lines: list[str] = []
    lines.append(f"# {analysis['store_key']} レポート")
    lines.append("")
    lines.append(f"- 生成時刻: {analysis['generated_at']}")
    lines.append(f"- サンプル日数: {analysis['sample_days']}")
    lines.append("")

    today = analysis["today_summary"]
    intensity = today["intensity"]
    lines.append("## 今日の強弱サマリー")
    lines.append(
        f"- 最新日: {today['stat_date']} / 判定: {intensity['label']} / スコア: {_fmt(intensity['score'])}"
    )
    lines.append(
        "- 指標: "
        f"平均G数={_fmt(analysis['latest_day'].get('average_games'))}, "
        f"平均差枚={_fmt(analysis['latest_day'].get('average_diff'))}, "
        f"勝率={_fmt(analysis['latest_day'].get('win_rate'))}"
    )
    tag_labels = [item["tag_label"] for item in analysis["latest_day"].get("tags", [])]
    lines.append(f"- タグ: {', '.join(tag_labels) if tag_labels else 'なし'}")
    lines.append("")

    lines.append("## 監視機種の優先順位")
    for item in analysis["watchlist_priorities"][:8]:
        matched_names = ", ".join(item.get("matched_machine_names", [])[:3])
        if matched_names:
            matched_names = f", 対象={matched_names}"
        lines.append(
            "- "
            f"{item['machine_name']}: score={_fmt(item['score'])}, "
            f"event_avg_G={_fmt(item['event_average_games'])}, "
            f"overall_avg_G={_fmt(item['overall_average_games'])}, "
            f"理由={item['reason']}{matched_names}"
        )
    if not analysis["watchlist_priorities"]:
        lines.append("- 監視機種なし")
    lines.append("")

    lines.append("## 狙い候補の末尾")
    for item in analysis["suffix_candidates"][:5]:
        lines.append(
            "- "
            f"末尾{item['suffix_label']}: score={_fmt(item['score'])}, "
            f"event_avg_G={_fmt(item['event_average_games'])}, "
            f"top3_hits={_fmt(item['top3_hits'])}"
        )
    if not analysis["suffix_candidates"]:
        lines.append("- 末尾データなし")
    lines.append("")

    lines.append("## イベント日比較")
    for item in analysis["event_comparisons"]:
        lines.append(
            "- "
            f"{item['label']}: avg_G={_fmt(item['tagged']['average_games'])} "
            f"(通常差 {_fmt(item['delta']['average_games'])}), "
            f"勝率={_fmt(item['tagged']['win_rate'])} "
            f"(通常差 {_fmt(item['delta']['win_rate'])})"
        )
    lines.append("")

    lines.append("## 前回イベント日との比較")
    previous = analysis.get("previous_event_comparison")
    if previous:
        lines.append(
            "- "
            f"{previous['tag_label']} / 現在 {previous['current_date']} と 前回 {previous['previous_date']} を比較"
        )
        lines.append(
            "- "
            f"平均G差={_fmt(previous['delta']['average_games'])}, "
            f"平均差枚差={_fmt(previous['delta']['average_diff'])}, "
            f"勝率差={_fmt(previous['delta']['win_rate'])}"
        )
    else:
        lines.append("- 比較対象なし")
    lines.append("")

    lines.append("## 仮説検証レポート")
    for item in analysis["hypothesis_results"]:
        lines.append(
            "- "
            f"{item['label']}: verdict={item['verdict']}, "
            f"avg_G差={_fmt(item['delta']['average_games'])}, "
            f"勝率差={_fmt(item['delta']['win_rate'])}"
        )
    if not analysis["hypothesis_results"]:
        lines.append("- 仮説なし")
    lines.append("")

    if analysis.get("warnings"):
        lines.append("## 取得/解析警告")
        for item in analysis["warnings"][:10]:
            lines.append(
                "- "
                f"{item['created_at']} {item['page_kind']} {item['error_kind']}: {item['error_message']}"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def render_json(analysis: dict[str, Any]) -> str:
    return json.dumps(analysis, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def render_html(analysis: dict[str, Any]) -> str:
    md = render_markdown(analysis)
    escaped = "<br>".join(html.escape(line) for line in md.splitlines())
    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(analysis.get('store_key', 'taihokamezima'))} report</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f1e8;
      --panel: #fffaf3;
      --ink: #1f1b16;
      --accent: #af3b2f;
      --line: #d8c9b1;
    }}
    body {{
      margin: 0;
      font-family: "Hiragino Sans", "Yu Gothic", sans-serif;
      background: radial-gradient(circle at top, #fff3db 0%, var(--bg) 62%);
      color: var(--ink);
    }}
    main {{
      max-width: 900px;
      margin: 40px auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 28px;
      box-shadow: 0 12px 40px rgba(58, 41, 22, 0.08);
    }}
    h1 {{
      margin-top: 0;
      color: var(--accent);
      font-size: 1.9rem;
    }}
    .content {{
      line-height: 1.8;
      white-space: normal;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(analysis.get("store_key", "taihokamezima"))} report</h1>
    <div class="content">{escaped}</div>
  </main>
</body>
</html>
"""
