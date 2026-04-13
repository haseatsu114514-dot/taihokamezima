#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tracker.analysis import build_analysis
from tracker.config import load_config
from tracker.db import apply_schema, connect_database
from tracker.reporting import render_html, render_json, render_markdown


def run_analysis(config_path: str) -> int:
    config = load_config(config_path)
    config.report_dir.mkdir(parents=True, exist_ok=True)
    conn = connect_database(config.database_path)
    apply_schema(conn, PROJECT_ROOT / "sql" / "schema.sql")

    for store in config.stores:
        analysis = build_analysis(
            conn,
            store.key,
            config.analysis.lookback_days,
            config.analysis.watchlist_event_weight,
            config.analysis.recent_weight,
        )
        json_text = render_json(analysis)
        md_text = render_markdown(analysis)
        html_text = render_html(analysis)

        (config.report_dir / f"{store.key}_analysis.json").write_text(json_text, encoding="utf-8")
        (config.report_dir / f"{store.key}_report.md").write_text(md_text, encoding="utf-8")
        (config.report_dir / f"{store.key}_report.html").write_text(html_text, encoding="utf-8")
        (config.report_dir / "latest_analysis.json").write_text(json_text, encoding="utf-8")
        (config.report_dir / "latest_report.md").write_text(md_text, encoding="utf-8")
        (config.report_dir / "index.html").write_text(html_text, encoding="utf-8")
        print(f"[ok] rendered reports for {store.key}")

    conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Taiho Kamejima min-repo data.")
    parser.add_argument("--config", default="config.toml", help="Path to TOML config")
    args = parser.parse_args()
    return run_analysis(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
