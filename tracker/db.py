from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from tracker.models import (
    CalendarTagRecord,
    DailySummaryRecord,
    HypothesisRecord,
    MachineSummaryRecord,
    MachineUnitResultRecord,
    ParseErrorRecord,
    RawPageRecord,
    SueoSummaryRecord,
    WatchlistMachineRecord,
)


def connect_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def apply_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    conn.executescript(schema_path.read_text(encoding="utf-8"))
    _ensure_column(conn, "watchlist_machines", "display_name", "TEXT")
    _ensure_column(conn, "watchlist_machines", "match_type", "TEXT NOT NULL DEFAULT 'exact'")
    _ensure_column(conn, "watchlist_machines", "match_value", "TEXT")
    conn.commit()


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    columns = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in columns:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def insert_raw_page(conn: sqlite3.Connection, record: RawPageRecord) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO raw_pages (
            store_key, page_kind, page_url, page_date, fetched_at,
            status_code, body_sha256, file_path, content_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.store_key,
            record.page_kind,
            record.page_url,
            record.page_date,
            record.fetched_at,
            record.status_code,
            record.body_sha256,
            record.file_path,
            record.content_type,
        ),
    )


def log_parse_error(conn: sqlite3.Connection, record: ParseErrorRecord) -> None:
    conn.execute(
        """
        INSERT INTO parse_errors (
            store_key, page_kind, page_url, page_date, error_kind,
            error_message, body_sha256, file_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.store_key,
            record.page_kind,
            record.page_url,
            record.page_date,
            record.error_kind,
            record.error_message,
            record.body_sha256,
            record.file_path,
            record.created_at,
        ),
    )


def daily_exists(conn: sqlite3.Connection, store_key: str, stat_date: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM daily_summary
        WHERE store_key = ? AND stat_date = ?
        LIMIT 1
        """,
        (store_key, stat_date),
    ).fetchone()
    return row is not None


def replace_day_dataset(
    conn: sqlite3.Connection,
    summary: DailySummaryRecord,
    machine_rows: list[MachineSummaryRecord],
    sueo_rows: list[SueoSummaryRecord],
    unit_rows: list[MachineUnitResultRecord],
) -> None:
    now = summary.collected_at
    conn.execute(
        """
        INSERT INTO daily_summary (
            store_key, stat_date, report_url, all_units_url,
            list_position, list_total_diff, list_average_diff, list_average_games,
            published_at, total_diff, average_diff, average_games, win_rate, winners,
            total_machines, pay_rate, official_event_text, note_json, collected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(store_key, stat_date) DO UPDATE SET
            report_url = excluded.report_url,
            all_units_url = excluded.all_units_url,
            list_position = excluded.list_position,
            list_total_diff = excluded.list_total_diff,
            list_average_diff = excluded.list_average_diff,
            list_average_games = excluded.list_average_games,
            published_at = excluded.published_at,
            total_diff = excluded.total_diff,
            average_diff = excluded.average_diff,
            average_games = excluded.average_games,
            win_rate = excluded.win_rate,
            winners = excluded.winners,
            total_machines = excluded.total_machines,
            pay_rate = excluded.pay_rate,
            official_event_text = excluded.official_event_text,
            note_json = excluded.note_json,
            updated_at = excluded.updated_at
        """,
        (
            summary.store_key,
            summary.stat_date,
            summary.report_url,
            summary.all_units_url,
            summary.list_position,
            summary.list_total_diff,
            summary.list_average_diff,
            summary.list_average_games,
            summary.published_at,
            summary.total_diff,
            summary.average_diff,
            summary.average_games,
            summary.win_rate,
            summary.winners,
            summary.total_machines,
            summary.pay_rate,
            summary.official_event_text,
            json.dumps(summary.note, ensure_ascii=False, sort_keys=True),
            summary.collected_at,
            now,
        ),
    )

    conn.execute(
        "DELETE FROM machine_summary WHERE store_key = ? AND stat_date = ?",
        (summary.store_key, summary.stat_date),
    )
    conn.execute(
        "DELETE FROM sueo_summary WHERE store_key = ? AND stat_date = ?",
        (summary.store_key, summary.stat_date),
    )
    conn.execute(
        "DELETE FROM machine_unit_results WHERE store_key = ? AND stat_date = ?",
        (summary.store_key, summary.stat_date),
    )

    conn.executemany(
        """
        INSERT INTO machine_summary (
            store_key, stat_date, machine_name, machine_count, average_diff,
            average_games, win_rate, winners, pay_rate, source_section,
            raw_row_json, collected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.store_key,
                row.stat_date,
                row.machine_name,
                row.machine_count,
                row.average_diff,
                row.average_games,
                row.win_rate,
                row.winners,
                row.pay_rate,
                row.source_section,
                json.dumps(row.raw_row, ensure_ascii=False, sort_keys=True),
                row.collected_at,
                now,
            )
            for row in machine_rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO sueo_summary (
            store_key, stat_date, suffix_key, suffix_label, machine_count,
            average_diff, average_games, win_rate, winners, pay_rate,
            raw_row_json, collected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.store_key,
                row.stat_date,
                row.suffix_key,
                row.suffix_label,
                row.machine_count,
                row.average_diff,
                row.average_games,
                row.win_rate,
                row.winners,
                row.pay_rate,
                json.dumps(row.raw_row, ensure_ascii=False, sort_keys=True),
                row.collected_at,
                now,
            )
            for row in sueo_rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO machine_unit_results (
            store_key, stat_date, machine_name, unit_number, diff, games,
            pay_rate, is_variety, raw_row_json, collected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.store_key,
                row.stat_date,
                row.machine_name,
                row.unit_number,
                row.diff,
                row.games,
                row.pay_rate,
                int(row.is_variety),
                json.dumps(row.raw_row, ensure_ascii=False, sort_keys=True),
                row.collected_at,
                now,
            )
            for row in unit_rows
        ],
    )


def replace_calendar_tags(
    conn: sqlite3.Connection,
    store_key: str,
    tag_date: str,
    tags: list[CalendarTagRecord],
) -> None:
    conn.execute(
        """
        DELETE FROM calendar_tags
        WHERE store_key = ? AND tag_date = ? AND rule_source != 'manual'
        """,
        (store_key, tag_date),
    )
    conn.executemany(
        """
        INSERT INTO calendar_tags (
            store_key, tag_date, tag_group, tag_key, tag_label, is_active,
            rule_source, detail_json, collected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.store_key,
                row.tag_date,
                row.tag_group,
                row.tag_key,
                row.tag_label,
                int(row.is_active),
                row.rule_source,
                json.dumps(row.detail, ensure_ascii=False, sort_keys=True),
                row.collected_at,
                row.collected_at,
            )
            for row in tags
        ],
    )


def sync_watchlist(conn: sqlite3.Connection, rows: list[WatchlistMachineRecord], now_iso: str) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO watchlist_machines (
                store_key, machine_name, display_name, match_type, match_value,
                priority, enabled, note, source_config, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'config', ?, ?)
            ON CONFLICT(store_key, machine_name) DO UPDATE SET
                display_name = excluded.display_name,
                match_type = excluded.match_type,
                match_value = excluded.match_value,
                priority = excluded.priority,
                enabled = excluded.enabled,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (
                row.store_key,
                row.machine_name,
                row.display_name or row.machine_name,
                row.match_type,
                row.match_value or row.machine_name,
                row.priority,
                int(row.enabled),
                row.note,
                now_iso,
                now_iso,
            ),
        )


def sync_hypotheses(conn: sqlite3.Connection, rows: list[HypothesisRecord], now_iso: str) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO hypotheses (
                store_key, hypothesis_key, label, tag_key, tag_label, tag_group,
                rule_type, rule_value, expected_direction, description, enabled,
                source_config, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'config', ?, ?)
            ON CONFLICT(store_key, hypothesis_key) DO UPDATE SET
                label = excluded.label,
                tag_key = excluded.tag_key,
                tag_label = excluded.tag_label,
                tag_group = excluded.tag_group,
                rule_type = excluded.rule_type,
                rule_value = excluded.rule_value,
                expected_direction = excluded.expected_direction,
                description = excluded.description,
                enabled = excluded.enabled,
                updated_at = excluded.updated_at
            """,
            (
                row.store_key,
                row.hypothesis_key,
                row.label,
                row.tag_key,
                row.tag_label,
                row.tag_group,
                row.rule_type,
                row.rule_value,
                row.expected_direction,
                row.description,
                int(row.enabled),
                now_iso,
                now_iso,
            ),
        )
