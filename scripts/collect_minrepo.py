#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tracker.config import AppConfig, StoreConfig, load_config
from tracker.db import (
    apply_schema,
    connect_database,
    daily_exists,
    insert_raw_page,
    log_parse_error,
    replace_calendar_tags,
    replace_day_dataset,
    sync_hypotheses,
    sync_watchlist,
)
from tracker.events import generate_calendar_tags
from tracker.minrepo import (
    ParseStructureError,
    build_all_units_url,
    fetch_url,
    maybe_wait,
    parse_all_units_page,
    parse_daily_page,
    parse_listing_page,
    sha256_hex,
)
from tracker.models import ParseErrorRecord, RawPageRecord


def _snapshot_path(
    config: AppConfig,
    store_key: str,
    page_kind: str,
    fetched_at: str,
    page_date: str | None = None,
) -> Path:
    config.snapshot_dir.mkdir(parents=True, exist_ok=True)
    safe_timestamp = fetched_at.replace(":", "").replace("-", "")
    date_part = f"_{page_date}" if page_date else ""
    return config.snapshot_dir / f"{store_key}_{page_kind}{date_part}_{safe_timestamp}.html"


def _fetch_and_store(
    conn,
    config: AppConfig,
    store_key: str,
    page_kind: str,
    page_url: str,
    page_date: str | None,
) -> tuple[str, bytes]:
    fetched_at = datetime.now().astimezone().isoformat(timespec="seconds")
    status_code, content_type, body = fetch_url(
        page_url,
        config.user_agent,
        config.request_timeout_seconds,
    )
    path = _snapshot_path(config, store_key, page_kind, fetched_at, page_date)
    path.write_bytes(body)
    insert_raw_page(
        conn,
        RawPageRecord(
            store_key=store_key,
            page_kind=page_kind,
            page_url=page_url,
            page_date=page_date,
            fetched_at=fetched_at,
            status_code=status_code,
            body_sha256=sha256_hex(body),
            file_path=str(path),
            content_type=content_type,
        ),
    )
    return str(path), body


def _record_error(
    conn,
    store_key: str,
    page_kind: str,
    page_url: str,
    page_date: str | None,
    error_kind: str,
    error_message: str,
    file_path: str | None,
    body: bytes | None,
) -> None:
    log_parse_error(
        conn,
        ParseErrorRecord(
            store_key=store_key,
            page_kind=page_kind,
            page_url=page_url,
            page_date=page_date,
            error_kind=error_kind,
            error_message=error_message,
            body_sha256=sha256_hex(body) if body is not None else None,
            file_path=file_path,
            created_at=datetime.now().astimezone().isoformat(timespec="seconds"),
        ),
    )


def _collect_store(conn, config: AppConfig, store: StoreConfig) -> tuple[int, int]:
    page_url = store.listing_url
    listing_pages = 0
    collected_days = 0
    discovered_days = 0
    visited_urls: set[str] = set()

    while page_url and listing_pages < config.max_listing_pages and page_url not in visited_urls:
        visited_urls.add(page_url)
        snapshot_path, body = _fetch_and_store(
            conn,
            config,
            store.key,
            "listing",
            page_url,
            None,
        )
        maybe_wait(config.request_delay_seconds)
        try:
            parsed_listing = parse_listing_page(
                store.key,
                body.decode("utf-8", errors="ignore"),
                page_url,
                datetime.now().astimezone().isoformat(timespec="seconds"),
            )
        except ParseStructureError as exc:
            _record_error(
                conn,
                store.key,
                "listing",
                page_url,
                None,
                "structure_changed",
                str(exc),
                snapshot_path,
                body,
            )
            break

        discovered_days += len(parsed_listing.entries)
        for entry in parsed_listing.entries:
            if daily_exists(conn, store.key, entry.stat_date):
                continue

            all_units_url = build_all_units_url(entry.report_url, store.all_units_query)
            daily_snapshot_path, daily_body = _fetch_and_store(
                conn,
                config,
                store.key,
                "daily",
                entry.report_url,
                entry.stat_date,
            )
            maybe_wait(config.request_delay_seconds)
            try:
                daily_parsed = parse_daily_page(
                    store.key,
                    daily_body.decode("utf-8", errors="ignore"),
                    entry.report_url,
                    entry.stat_date,
                    all_units_url,
                    datetime.now().astimezone().isoformat(timespec="seconds"),
                    listing_entry=entry,
                )
            except ParseStructureError as exc:
                _record_error(
                    conn,
                    store.key,
                    "daily",
                    entry.report_url,
                    entry.stat_date,
                    "structure_changed",
                    str(exc),
                    daily_snapshot_path,
                    daily_body,
                )
                conn.commit()
                continue

            unit_rows = list(daily_parsed.variety_unit_results)
            all_units_snapshot_path = None
            try:
                all_units_snapshot_path, all_units_body = _fetch_and_store(
                    conn,
                    config,
                    store.key,
                    "all_units",
                    all_units_url,
                    entry.stat_date,
                )
                maybe_wait(config.request_delay_seconds)
                parsed_units = parse_all_units_page(
                    store.key,
                    all_units_body.decode("utf-8", errors="ignore"),
                    entry.stat_date,
                    datetime.now().astimezone().isoformat(timespec="seconds"),
                )
                unit_rows = parsed_units.unit_results
            except Exception as exc:  # noqa: BLE001
                _record_error(
                    conn,
                    store.key,
                    "all_units",
                    all_units_url,
                    entry.stat_date,
                    "all_units_fallback",
                    str(exc),
                    all_units_snapshot_path,
                    all_units_body if "all_units_body" in locals() else None,
                )

            tags = generate_calendar_tags(
                store.key,
                entry.stat_date,
                [item for item in config.hypotheses if item.store_key == store.key],
                daily_parsed.summary.collected_at,
                daily_parsed.summary.note.get("page_status"),
            )

            replace_day_dataset(
                conn,
                daily_parsed.summary,
                daily_parsed.machine_summaries,
                daily_parsed.sueo_summaries,
                unit_rows,
            )
            replace_calendar_tags(conn, store.key, entry.stat_date, tags)
            conn.commit()
            collected_days += 1
            print(
                f"[ok] {store.key} {entry.stat_date} "
                f"machines={len(daily_parsed.machine_summaries)} "
                f"suffixes={len(daily_parsed.sueo_summaries)} "
                f"units={len(unit_rows)}"
            )

        listing_pages += 1
        page_url = parsed_listing.next_page_url

    return discovered_days, collected_days


def run_collection(config_path: str) -> int:
    config = load_config(config_path)
    conn = connect_database(config.database_path)
    apply_schema(conn, PROJECT_ROOT / "sql" / "schema.sql")
    now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
    sync_watchlist(conn, config.watchlist_machines, now_iso)
    sync_hypotheses(conn, config.hypotheses, now_iso)
    conn.commit()

    for store in config.stores:
        discovered, collected = _collect_store(conn, config, store)
        print(f"[done] store={store.key} discovered={discovered} newly_collected={collected}")

    conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect min-repo data into SQLite.")
    parser.add_argument("--config", default="config.toml", help="Path to TOML config")
    args = parser.parse_args()
    return run_collection(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
